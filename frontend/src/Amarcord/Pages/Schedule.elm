module Amarcord.Pages.Schedule exposing (..)

import Amarcord.API.Requests exposing (ChemicalsResponse, ExperimentTypesResponse, RequestError, ScheduleEntry, ScheduleResponse, httpGetChemicals, httpGetSchedule, httpUpdateSchedule)
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon)
import Amarcord.Chemical exposing (Chemical, ChemicalId)
import Amarcord.File exposing (File)
import Amarcord.Html exposing (input_)
import Date
import Dict exposing (Dict)
import Html exposing (Html, button, div, em, h3, option, select, span, table, tbody, td, text, th, thead, tr)
import Html.Attributes exposing (class, disabled, id, placeholder, selected, style, type_, value)
import Html.Events exposing (onClick, onInput)
import List.Extra
import Maybe.Extra
import Regex
import RemoteData exposing (RemoteData(..), fromResult)


type ScheduleMsg
    = ScheduleUpdated (Result RequestError ())
    | ScheduleReceived (Result RequestError ScheduleResponse)
    | SubmitShift
    | ModifyShift ShiftId
    | SubmitModifiedShift ShiftId
    | RemoveShift ShiftId
    | SubmitDeleteShift ScheduleEntry
    | UpdateNewShiftByColumn TableColumn String
    | UpdateNewShiftChemical String
    | UpdateToModifyShiftByColumn TableColumn String
    | UpdateToModifyShiftChemical String
    | ResetToModifyShift
    | ResetToDeleteShift
    | ChemicalsReceived ChemicalsResponse


type alias ChemicalsAndAttributi =
    { chemicals : List (Chemical ChemicalId (AttributoMap AttributoValue) File)
    , attributi : List (Attributo AttributoType)
    }


type alias ShiftId =
    Int


type alias ScheduleEntryToModify =
    { scheduleEntry : ScheduleEntry, id : Maybe ShiftId }


type alias ScheduleModel =
    { schedule : Dict ShiftId ScheduleEntry
    , newScheduleEntry : ScheduleEntry
    , editingScheduleEntry : ScheduleEntryToModify
    , deletingScheduleEntry : ScheduleEntryToModify
    , chemicals : RemoteData RequestError ChemicalsAndAttributi
    }


emptyScheduleEntry : ScheduleEntry
emptyScheduleEntry =
    { users = "", date = "", shift = "", chemicalId = Nothing, comment = "", tdSupport = "" }


emptyScheduleEntryToModify : ScheduleEntryToModify
emptyScheduleEntryToModify =
    { scheduleEntry = emptyScheduleEntry, id = Nothing }


type TableColumn
    = Date
    | Shift
    | Users
    | TdSupport
    | Chemical
    | Comment
    | Actions


styleColumn : TableColumn -> Html.Attribute msg
styleColumn column =
    case column of
        Date ->
            style "width" "11%"

        Shift ->
            style "width" "11%"

        Users ->
            style "width" "15%"

        TdSupport ->
            style "width" "8%"

        Chemical ->
            style "width" "12%"

        Comment ->
            style "width" "20%"

        Actions ->
            style "width" "8%"


initSchedule : ( ScheduleModel, Cmd ScheduleMsg )
initSchedule =
    ( { chemicals = Loading
      , schedule = Dict.empty
      , newScheduleEntry = emptyScheduleEntry
      , editingScheduleEntry = emptyScheduleEntryToModify
      , deletingScheduleEntry = emptyScheduleEntryToModify
      }
    , Cmd.batch [ httpGetSchedule ScheduleReceived, httpGetChemicals ChemicalsReceived ]
    )


view : ScheduleModel -> Html ScheduleMsg
view model =
    div [ class "container" ]
        [ h3 [] [ text "Beamtime Schedule" ]
        , div []
            [ table [ class "table table-striped" ]
                [ thead [ class "thead-light" ]
                    [ tr []
                        [ th [ styleColumn Date ] [ text "Date" ]
                        , th [ styleColumn Shift ] [ text "Shift" ]
                        , th [ styleColumn Users ] [ text "Users" ]
                        , th [ styleColumn TdSupport ] [ text "TD-Support" ]
                        , th [ styleColumn Chemical ] [ text "Chemical" ]
                        , th [ styleColumn Comment ] [ text "Comment" ]
                        , th [ styleColumn Actions ] [ text "Actions" ]
                        ]
                    ]
                , tbody []
                    (Dict.toList model.schedule
                        |> List.map (scheduleEntryView model)
                        |> List.reverse
                        |> (::) (newScheduleEntryView model)
                        |> List.reverse
                    )
                ]
            ]
        , case unscheduledChemicalsNames model of
            Nothing ->
                div [] []

            Just us ->
                div []
                    [ em [] [ text <| "Following chemicals have not yet been scheduled: " ]
                    , span [] [ text <| us ]
                    ]
        ]


unscheduledChemicalsNames : ScheduleModel -> Maybe String
unscheduledChemicalsNames model =
    let
        chemicals =
            case model.chemicals of
                Success s ->
                    s.chemicals

                _ ->
                    []

        chemicalIsNotScheduled s =
            List.member s.id (Maybe.Extra.values (List.map .chemicalId (Dict.values model.schedule))) == False

        unscheduledChemicals =
            List.filter chemicalIsNotScheduled chemicals
    in
    if List.isEmpty unscheduledChemicals then
        Nothing

    else
        Just (String.join ", " (List.map .name unscheduledChemicals))


scheduleEntryView : ScheduleModel -> ( ShiftId, ScheduleEntry ) -> Html ScheduleMsg
scheduleEntryView model shiftIdValue =
    let
        modifiedScheduleId =
            model.editingScheduleEntry.id
    in
    case modifiedScheduleId of
        Nothing ->
            let
                deletingScheduleId =
                    model.deletingScheduleEntry.id
            in
            case deletingScheduleId of
                Nothing ->
                    let
                        entry =
                            Tuple.second shiftIdValue

                        shiftId =
                            Tuple.first shiftIdValue
                    in
                    readOnlyScheduleEntryView model entry shiftId

                Just idShiftToDelete ->
                    let
                        shiftId =
                            Tuple.first shiftIdValue
                    in
                    if idShiftToDelete == shiftId then
                        let
                            entryToDelete =
                                model.deletingScheduleEntry.scheduleEntry
                        in
                        deleteScheduleEntryView model entryToDelete

                    else
                        let
                            entry =
                                Tuple.second shiftIdValue
                        in
                        readOnlyScheduleEntryView model entry shiftId

        Just idShiftToModify ->
            let
                shiftId =
                    Tuple.first shiftIdValue
            in
            if idShiftToModify == shiftId then
                let
                    entryToModify =
                        model.editingScheduleEntry.scheduleEntry
                in
                editingScheduleEntryView model entryToModify idShiftToModify

            else
                let
                    entry =
                        Tuple.second shiftIdValue
                in
                readOnlyScheduleEntryView model entry shiftId


editingScheduleEntryView : ScheduleModel -> ScheduleEntry -> ShiftId -> Html ScheduleMsg
editingScheduleEntryView model entryToModify idShiftToModify =
    tr []
        [ td [ styleColumn Date ]
            [ input_
                [ type_ "string"
                , class "form-control"
                , id "input-schedule-date"
                , placeholder "YYYY-MM-DD"
                , value entryToModify.date
                , onInput (UpdateToModifyShiftByColumn Date)
                ]
            ]
        , td [ styleColumn Shift ]
            [ input_
                [ type_ "string"
                , class "form-control"
                , id "input-schedule-shift"
                , placeholder "HH:mm - HH:mm"
                , value entryToModify.shift
                , onInput (UpdateToModifyShiftByColumn Shift)
                ]
            ]
        , td [ styleColumn Users ]
            [ input_
                [ type_ "string"
                , class "form-control"
                , id "input-schedule-users"
                , value entryToModify.users
                , onInput (UpdateToModifyShiftByColumn Users)
                ]
            ]
        , td [ styleColumn TdSupport ]
            [ input_
                [ type_ "string"
                , class "form-control"
                , id "input-schedule-td-support"
                , value entryToModify.tdSupport
                , onInput (UpdateToModifyShiftByColumn TdSupport)
                ]
            ]
        , td [ styleColumn Chemical ]
            [ chemicalDropdownEdit entryToModify.chemicalId model
            ]
        , td [ styleColumn Comment ]
            [ input_
                [ type_ "string"
                , class "form-control"
                , id "input-schedule-comment"
                , value entryToModify.comment
                , onInput (UpdateToModifyShiftByColumn Comment)
                ]
            ]
        , td [ styleColumn Actions ]
            [ div [ class "form-control-sm" ]
                [ button
                    [ class "btn btn-sm btn-success me-1"
                    , type_ "button"
                    , onClick <| SubmitModifiedShift idShiftToModify
                    , disabled <| cannotAddSchedule entryToModify
                    ]
                    [ icon { name = "check" } ]
                , button
                    [ class "btn btn-sm btn-warning"
                    , type_ "button"
                    , onClick ResetToModifyShift
                    ]
                    [ icon { name = "x" } ]
                ]
            ]
        ]


deleteScheduleEntryView : ScheduleModel -> ScheduleEntry -> Html ScheduleMsg
deleteScheduleEntryView model entry =
    tr [] <|
        shiftSubview model entry
            ++ [ td [ styleColumn Actions ]
                    [ div [ class "form-control-sm" ]
                        [ button
                            [ class "btn btn-sm btn-success me-1"
                            , type_ "button"
                            , onClick (SubmitDeleteShift entry)
                            ]
                            [ icon { name = "check" } ]
                        , button
                            [ class "btn btn-sm btn-warning"
                            , type_ "button"
                            , onClick ResetToDeleteShift
                            ]
                            [ icon { name = "x" }
                            ]
                        ]
                    ]
               ]


readOnlyScheduleEntryView : ScheduleModel -> ScheduleEntry -> ShiftId -> Html ScheduleMsg
readOnlyScheduleEntryView model entry shiftId =
    tr [] <|
        shiftSubview model entry
            ++ [ td [ styleColumn Actions ]
                    [ div [ class "form-control-sm" ]
                        [ button [ class "btn btn-sm btn-info me-1", type_ "button", onClick (ModifyShift shiftId) ] [ icon { name = "pencil-square" } ]
                        , button [ class "btn btn-sm btn-danger", type_ "button", onClick (RemoveShift shiftId) ] [ icon { name = "trash" } ]
                        ]
                    ]
               ]


shiftSubview : ScheduleModel -> ScheduleEntry -> List (Html ScheduleMsg)
shiftSubview model entry =
    [ td [ styleColumn Date ] [ text <| dateEntry entry.date ]
    , td [ styleColumn Shift ] [ text entry.shift ]
    , td [ styleColumn Users ] [ text entry.users ]
    , td [ styleColumn TdSupport ] [ text entry.tdSupport ]
    , td [ styleColumn Chemical ]
        [ case entry.chemicalId of
            Nothing ->
                text ""

            Just chemicalId ->
                case model.chemicals of
                    Success chemicals ->
                        case List.Extra.find (\chemical -> chemicalId == chemical.id) chemicals.chemicals of
                            Nothing ->
                                text ("Chemical ID " ++ String.fromInt chemicalId ++ " is unknown")

                            Just s ->
                                text s.name

                    Loading ->
                        text ""

                    _ ->
                        text ("Chemical ID " ++ String.fromInt chemicalId ++ " is unknown")
        ]
    , td [ styleColumn Comment ]
        [ text entry.comment ]
    ]


newScheduleEntryView : ScheduleModel -> Html ScheduleMsg
newScheduleEntryView model =
    tr []
        [ td [ styleColumn Date ]
            [ input_
                [ type_ "string"
                , class "form-control"
                , id "input-schedule-date"
                , placeholder "YYYY-MM-DD"
                , onInput (UpdateNewShiftByColumn Date)
                ]
            ]
        , td [ styleColumn Shift ]
            [ input_
                [ type_ "string"
                , class "form-control"
                , id "input-schedule-shift"
                , placeholder "HH:mm - HH:mm"
                , onInput (UpdateNewShiftByColumn Shift)
                ]
            ]
        , td [ styleColumn Users ]
            [ input_
                [ type_ "string"
                , class "form-control"
                , id "input-schedule-users"
                , onInput (UpdateNewShiftByColumn Users)
                ]
            ]
        , td [ styleColumn TdSupport ]
            [ input_
                [ type_ "string"
                , class "form-control"
                , id "input-schedule-td-support"
                , onInput (UpdateNewShiftByColumn TdSupport)
                ]
            ]
        , td [ styleColumn Chemical ]
            [ chemicalDropdown model
            ]
        , td [ styleColumn Comment ]
            [ input_
                [ type_ "string"
                , class "form-control"
                , id "input-schedule-comment"
                , onInput (UpdateNewShiftByColumn Comment)
                ]
            ]
        , td [ styleColumn Actions ]
            [ div [ class "form-control-sm" ]
                [ button [ class "btn btn-sm btn-primary", type_ "button", onClick SubmitShift, disabled <| cannotAddSchedule model.newScheduleEntry ] [ icon { name = "calendar-plus" } ]
                ]
            ]
        ]


chemicalDropdown : ScheduleModel -> Html ScheduleMsg
chemicalDropdown model =
    let
        optionEntry chemical =
            option [ value <| String.fromInt chemical.id ] [ text chemical.name ]

        listChemicalsOptions =
            List.map optionEntry <|
                case model.chemicals of
                    Success chemicals ->
                        chemicals.chemicals

                    _ ->
                        []
    in
    select [ class "form-select", onInput UpdateNewShiftChemical ] <|
        (option [ selected True ] [ text "«none selected»" ]
            :: listChemicalsOptions
        )


chemicalDropdownEdit : Maybe Int -> ScheduleModel -> Html ScheduleMsg
chemicalDropdownEdit modifiableChemicalId model =
    let
        isNothingSelected =
            case modifiableChemicalId of
                Nothing ->
                    False

                Just _ ->
                    True

        isChemicalSelected chemicalId =
            case modifiableChemicalId of
                Nothing ->
                    False

                Just i ->
                    chemicalId == i

        optionEntry chemical =
            option [ selected (isChemicalSelected chemical.id), value <| String.fromInt chemical.id ] [ text chemical.name ]

        listChemicalsOptions =
            List.map optionEntry <|
                case model.chemicals of
                    Success chemicals ->
                        chemicals.chemicals

                    _ ->
                        []
    in
    select [ class "form-select", onInput UpdateToModifyShiftChemical ] <|
        (option [ selected isNothingSelected ] [ text "«none selected»" ]
            :: listChemicalsOptions
        )



-- To check input format YYYY-MM-DD


dateFormatRegex : Regex.Regex
dateFormatRegex =
    Maybe.withDefault Regex.never <|
        Regex.fromString "^\\d{4}\\-(0[1-9]|1[012])\\-(0[1-9]|[12][0-9]|3[01])$"



-- To check input format HH24:mm


shiftFormatRegex : Regex.Regex
shiftFormatRegex =
    Maybe.withDefault Regex.never <|
        Regex.fromString "^(0\\d|1\\d|2[0-3]):[0-5]\\d\\s*-\\s*(0\\d|1\\d|2[0-3]):[0-5]\\d$"


cannotAddSchedule : ScheduleEntry -> Bool
cannotAddSchedule se =
    String.isEmpty (String.trim se.users)
        || String.isEmpty (String.trim se.shift)
        || String.isEmpty (String.trim se.date)
        || not (Regex.contains dateFormatRegex (String.trim se.date))
        || not (Regex.contains shiftFormatRegex (String.trim se.shift))


sortScheduleEntry : ScheduleEntry -> ScheduleEntry -> Order
sortScheduleEntry a b =
    case compare a.date b.date of
        LT ->
            LT

        EQ ->
            case compare a.shift b.shift of
                LT ->
                    LT

                EQ ->
                    case a.chemicalId of
                        Nothing ->
                            EQ

                        Just sa ->
                            case b.chemicalId of
                                Nothing ->
                                    EQ

                                Just sb ->
                                    case compare sa sb of
                                        LT ->
                                            LT

                                        EQ ->
                                            EQ

                                        GT ->
                                            GT

                GT ->
                    GT

        GT ->
            GT


scheduleDictFromScheduleList : List ScheduleEntry -> Dict ShiftId ScheduleEntry
scheduleDictFromScheduleList shifts =
    Dict.fromList <|
        List.indexedMap (\x y -> ( x, y )) <|
            List.sortWith sortScheduleEntry shifts


updateSchedule : ScheduleMsg -> ScheduleModel -> ( ScheduleModel, Cmd ScheduleMsg )
updateSchedule msg model =
    case msg of
        ScheduleUpdated _ ->
            ( model, httpGetSchedule ScheduleReceived )

        ScheduleReceived getScheduleResponse ->
            case getScheduleResponse of
                Ok response ->
                    ( { model
                        | schedule = scheduleDictFromScheduleList response.schedule
                        , newScheduleEntry = emptyScheduleEntry
                        , editingScheduleEntry = emptyScheduleEntryToModify
                      }
                    , Cmd.none
                    )

                Err _ ->
                    ( model, Cmd.none )

        SubmitShift ->
            ( { model | newScheduleEntry = emptyScheduleEntry }
            , httpUpdateSchedule ScheduleUpdated (model.newScheduleEntry :: Dict.values model.schedule)
            )

        UpdateNewShiftByColumn tableColumn value ->
            ( { model
                | newScheduleEntry = updateScheduleEntryByColumn model.newScheduleEntry tableColumn value
              }
            , Cmd.none
            )

        UpdateNewShiftChemical chemicalId ->
            let
                nse =
                    model.newScheduleEntry

                ns =
                    { nse | chemicalId = String.toInt chemicalId }
            in
            ( { model | newScheduleEntry = ns }, Cmd.none )

        SubmitDeleteShift scheduleEntry ->
            let
                newScheduleValues =
                    List.Extra.remove scheduleEntry (Dict.values model.schedule)

                newScheduleDict =
                    scheduleDictFromScheduleList newScheduleValues
            in
            ( { model
                | schedule = newScheduleDict
                , newScheduleEntry = emptyScheduleEntry
                , editingScheduleEntry = emptyScheduleEntryToModify
                , deletingScheduleEntry = emptyScheduleEntryToModify
              }
            , httpUpdateSchedule ScheduleUpdated newScheduleValues
            )

        RemoveShift shiftId ->
            let
                newEmptyScheduleEntry =
                    { emptyScheduleEntryToModify
                        | id = Just shiftId
                        , scheduleEntry = Maybe.withDefault emptyScheduleEntry (Dict.get shiftId model.schedule)
                    }
            in
            ( { model
                | deletingScheduleEntry = newEmptyScheduleEntry
              }
            , Cmd.none
            )

        ChemicalsReceived chemicalsResponse ->
            case chemicalsResponse of
                Ok _ ->
                    ( { model
                        | chemicals =
                            fromResult <|
                                Result.map (\( chemicals, attributi ) -> { chemicals = chemicals, attributi = attributi }) <|
                                    chemicalsResponse
                      }
                    , Cmd.none
                    )

                Err _ ->
                    ( model, Cmd.none )

        ModifyShift shiftId ->
            ( let
                newEmptyScheduleEntry =
                    { emptyScheduleEntryToModify
                        | id = Just shiftId
                        , scheduleEntry = Maybe.withDefault emptyScheduleEntry (Dict.get shiftId model.schedule)
                    }
              in
              { model
                | editingScheduleEntry = newEmptyScheduleEntry
                , deletingScheduleEntry = emptyScheduleEntryToModify
                , newScheduleEntry = emptyScheduleEntry
              }
            , Cmd.none
            )

        SubmitModifiedShift modifiedShiftId ->
            let
                mse =
                    model.editingScheduleEntry

                newDict =
                    Dict.remove modifiedShiftId model.schedule

                shifts =
                    Dict.values newDict

                allShifts =
                    mse.scheduleEntry :: shifts
            in
            ( model
            , httpUpdateSchedule ScheduleUpdated allShifts
            )

        UpdateToModifyShiftByColumn tableColumn value ->
            ( let
                mse =
                    model.editingScheduleEntry

                mid =
                    { id = mse.id
                    , scheduleEntry = updateScheduleEntryByColumn mse.scheduleEntry tableColumn value
                    }
              in
              { model | editingScheduleEntry = mid }
            , Cmd.none
            )

        UpdateToModifyShiftChemical chemicalId ->
            let
                modScheduleEntry =
                    model.editingScheduleEntry

                mid =
                    modScheduleEntry.scheduleEntry

                fig =
                    { mid | chemicalId = String.toInt chemicalId }

                nse2 =
                    { modScheduleEntry | scheduleEntry = fig }
            in
            ( { model | editingScheduleEntry = nse2 }, Cmd.none )

        ResetToModifyShift ->
            ( { model | editingScheduleEntry = emptyScheduleEntryToModify }, httpGetSchedule ScheduleReceived )

        ResetToDeleteShift ->
            ( { model | deletingScheduleEntry = emptyScheduleEntryToModify }, httpGetSchedule ScheduleReceived )


updateScheduleEntryByColumn : ScheduleEntry -> TableColumn -> String -> ScheduleEntry
updateScheduleEntryByColumn se column data =
    { date =
        case column of
            Date ->
                data

            _ ->
                se.date
    , shift =
        case column of
            Shift ->
                data

            _ ->
                se.shift
    , users =
        case column of
            Users ->
                data

            _ ->
                se.users
    , tdSupport =
        case column of
            TdSupport ->
                data

            _ ->
                se.tdSupport
    , comment =
        case column of
            Comment ->
                data

            _ ->
                se.comment
    , chemicalId = se.chemicalId
    }


dateEntry : String -> String
dateEntry inputDate =
    case Date.fromIsoString inputDate of
        Ok date ->
            Date.format "EEE, dd MMM y" date

        Err _ ->
            "Date " ++ inputDate ++ " is not parseable"
