module Amarcord.Pages.Schedule exposing (..)

import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue)
import Amarcord.Bootstrap exposing (icon)
import Amarcord.Chemical exposing (Chemical, ChemicalId)
import Amarcord.Html exposing (input_, li_)
import Amarcord.Pages.Chemicals exposing (convertChemicalsResponse)
import Api exposing (send)
import Api.Data exposing (JsonBeamtimeSchedule, JsonBeamtimeScheduleOutput, JsonBeamtimeScheduleRow, JsonFileOutput, JsonReadChemicals)
import Api.Request.Chemicals exposing (readChemicalsApiChemicalsBeamtimeIdGet)
import Api.Request.Schedule exposing (getBeamtimeScheduleApiScheduleBeamtimeIdGet, updateBeamtimeScheduleApiSchedulePost)
import Date
import Dict exposing (Dict)
import Html exposing (Html, button, div, em, h3, input, label, span, table, tbody, td, text, th, thead, tr, ul)
import Html.Attributes exposing (attribute, checked, class, disabled, for, id, placeholder, style, type_, value)
import Html.Events exposing (onClick, onInput)
import Http
import List.Extra
import Regex
import RemoteData exposing (RemoteData(..), fromResult)


type ScheduleMsg
    = ScheduleUpdated (Result Http.Error JsonBeamtimeScheduleOutput)
    | ScheduleReceived (Result Http.Error JsonBeamtimeSchedule)
    | SubmitShift
    | ModifyShift ShiftId
    | SubmitModifiedShift ShiftId
    | RemoveShift ShiftId
    | SubmitDeleteShift JsonBeamtimeScheduleRow
    | UpdateNewShiftByColumn TableColumn String
    | UpdateNewShiftChemical String
    | UpdateToModifyShiftByColumn TableColumn String
    | UpdateToModifyShiftChemical String
    | ResetToModifyShift
    | ResetToDeleteShift
    | ChemicalsReceived (Result Http.Error JsonReadChemicals)


type ChemicalDropdownMode
    = Edit
    | New


type alias ChemicalsAndAttributi =
    { chemicals : List (Chemical ChemicalId (AttributoMap AttributoValue) JsonFileOutput)
    , attributi : List (Attributo AttributoType)
    }


type alias ShiftId =
    Int


type alias JsonBeamtimeScheduleRowToModify =
    { scheduleEntry : JsonBeamtimeScheduleRow, id : Maybe ShiftId }


type alias ScheduleModel =
    { schedule : Dict ShiftId JsonBeamtimeScheduleRow
    , newJsonBeamtimeScheduleRow : JsonBeamtimeScheduleRow
    , editingJsonBeamtimeScheduleRow : JsonBeamtimeScheduleRowToModify
    , deletingJsonBeamtimeScheduleRow : JsonBeamtimeScheduleRowToModify
    , chemicals : RemoteData Http.Error ChemicalsAndAttributi
    , beamtimeId : BeamtimeId
    }


emptyJsonBeamtimeScheduleRow : JsonBeamtimeScheduleRow
emptyJsonBeamtimeScheduleRow =
    { users = "", date = "", shift = "", chemicals = [], comment = "", tdSupport = "" }


emptyJsonBeamtimeScheduleRowToModify : JsonBeamtimeScheduleRowToModify
emptyJsonBeamtimeScheduleRowToModify =
    { scheduleEntry = emptyJsonBeamtimeScheduleRow, id = Nothing }


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


initSchedule : BeamtimeId -> ( ScheduleModel, Cmd ScheduleMsg )
initSchedule beamtimeId =
    ( { chemicals = Loading
      , schedule = Dict.empty
      , newJsonBeamtimeScheduleRow = emptyJsonBeamtimeScheduleRow
      , editingJsonBeamtimeScheduleRow = emptyJsonBeamtimeScheduleRowToModify
      , deletingJsonBeamtimeScheduleRow = emptyJsonBeamtimeScheduleRowToModify
      , beamtimeId = beamtimeId
      }
    , Cmd.batch
        [ send ScheduleReceived (getBeamtimeScheduleApiScheduleBeamtimeIdGet beamtimeId)
        , send ChemicalsReceived (readChemicalsApiChemicalsBeamtimeIdGet beamtimeId)
        ]
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
                        |> (::) (newJsonBeamtimeScheduleRowView model)
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

        scheduled_chemical_ids =
            List.concatMap .chemicals (Dict.values model.schedule)

        not_scheduled_chemical =
            List.filter (\cid -> False == List.member cid.id scheduled_chemical_ids) chemicals
    in
    if List.isEmpty not_scheduled_chemical then
        Nothing

    else
        Just <| String.join ", " <| List.map .name not_scheduled_chemical


scheduleEntryView : ScheduleModel -> ( ShiftId, JsonBeamtimeScheduleRow ) -> Html ScheduleMsg
scheduleEntryView model shiftIdValue =
    let
        modifiedScheduleId =
            model.editingJsonBeamtimeScheduleRow.id
    in
    case modifiedScheduleId of
        Nothing ->
            let
                deletingScheduleId =
                    model.deletingJsonBeamtimeScheduleRow.id
            in
            case deletingScheduleId of
                Nothing ->
                    let
                        entry =
                            Tuple.second shiftIdValue

                        shiftId =
                            Tuple.first shiftIdValue
                    in
                    readOnlyJsonBeamtimeScheduleRowView model entry shiftId

                Just idShiftToDelete ->
                    let
                        shiftId =
                            Tuple.first shiftIdValue
                    in
                    if idShiftToDelete == shiftId then
                        let
                            entryToDelete =
                                model.deletingJsonBeamtimeScheduleRow.scheduleEntry
                        in
                        deleteJsonBeamtimeScheduleRowView model entryToDelete

                    else
                        let
                            entry =
                                Tuple.second shiftIdValue
                        in
                        readOnlyJsonBeamtimeScheduleRowView model entry shiftId

        Just idShiftToModify ->
            let
                shiftId =
                    Tuple.first shiftIdValue
            in
            if idShiftToModify == shiftId then
                let
                    entryToModify =
                        model.editingJsonBeamtimeScheduleRow.scheduleEntry
                in
                editingJsonBeamtimeScheduleRowView model entryToModify idShiftToModify

            else
                let
                    entry =
                        Tuple.second shiftIdValue
                in
                readOnlyJsonBeamtimeScheduleRowView model entry shiftId


editingJsonBeamtimeScheduleRowView : ScheduleModel -> JsonBeamtimeScheduleRow -> ShiftId -> Html ScheduleMsg
editingJsonBeamtimeScheduleRowView model entryToModify idShiftToModify =
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
            [ chemicalDropdown model Edit
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


deleteJsonBeamtimeScheduleRowView : ScheduleModel -> JsonBeamtimeScheduleRow -> Html ScheduleMsg
deleteJsonBeamtimeScheduleRowView model entry =
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


readOnlyJsonBeamtimeScheduleRowView : ScheduleModel -> JsonBeamtimeScheduleRow -> ShiftId -> Html ScheduleMsg
readOnlyJsonBeamtimeScheduleRowView model entry shiftId =
    tr [] <|
        shiftSubview model entry
            ++ [ td [ styleColumn Actions ]
                    [ div [ class "form-control-sm" ]
                        [ button [ class "btn btn-sm btn-info me-1", type_ "button", onClick (ModifyShift shiftId) ] [ icon { name = "pencil-square" } ]
                        , button [ class "btn btn-sm btn-danger", type_ "button", onClick (RemoveShift shiftId) ] [ icon { name = "trash" } ]
                        ]
                    ]
               ]


shiftSubview : ScheduleModel -> JsonBeamtimeScheduleRow -> List (Html ScheduleMsg)
shiftSubview model entry =
    [ td [ styleColumn Date ] [ text <| dateEntry entry.date ]
    , td [ styleColumn Shift ] [ text entry.shift ]
    , td [ styleColumn Users ] [ text entry.users ]
    , td [ styleColumn TdSupport ] [ text entry.tdSupport ]
    , td [ styleColumn Chemical ]
        [ let
            nameOfChemical chemicalId =
                case model.chemicals of
                    Success chemicals ->
                        case List.Extra.find (\chemical -> chemical.id == chemicalId) chemicals.chemicals of
                            Nothing ->
                                "Chemical ID " ++ String.fromInt chemicalId ++ " is unknown"

                            Just chem ->
                                chem.name

                    NotAsked ->
                        "Not retrieved"

                    Loading ->
                        "Loading the chemicals..."

                    Failure _ ->
                        "Failed to load the chemicals"
          in
          text <| String.join ", " <| List.map nameOfChemical entry.chemicals
        ]
    , td [ styleColumn Comment ]
        [ text entry.comment ]
    ]


newJsonBeamtimeScheduleRowView : ScheduleModel -> Html ScheduleMsg
newJsonBeamtimeScheduleRowView model =
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
            [ chemicalDropdown model New
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
                [ button [ class "btn btn-sm btn-primary", type_ "button", onClick SubmitShift, disabled <| cannotAddSchedule model.newJsonBeamtimeScheduleRow ] [ icon { name = "calendar-plus" } ]
                ]
            ]
        ]


chemicalDropdown : ScheduleModel -> ChemicalDropdownMode -> Html ScheduleMsg
chemicalDropdown model mode =
    let
        chemicals =
            case model.chemicals of
                Success cc ->
                    cc.chemicals

                _ ->
                    []

        chemicalsAlreadySelected =
            model.editingJsonBeamtimeScheduleRow.scheduleEntry.chemicals

        editInput chemical =
            input
                [ class "form-check-input"
                , type_ "checkbox"
                , value <| String.fromInt chemical.id
                , checked <| List.member chemical.id chemicalsAlreadySelected
                , for chemical.name
                , onInput UpdateToModifyShiftChemical
                ]
                []

        newInput chemical =
            input
                [ class "form-check-input"
                , type_ "checkbox"
                , value <| String.fromInt chemical.id
                , for chemical.name
                , onInput UpdateNewShiftChemical
                ]
                []

        checkboxForOneChemical chemical =
            li_
                [ div [ class "dropdown-item " ]
                    [ div [ class "form-check" ]
                        [ case mode of
                            Edit ->
                                editInput chemical

                            New ->
                                newInput chemical
                        , label [ class "form-check-label", for chemical.name ] [ text chemical.name ]
                        ]
                    ]
                ]
    in
    div [ class "dropdown" ]
        [ button
            [ class "btn dropdown-toggle"
            , attribute "data-bs-toggle" "dropdown"
            , attribute "data-bs-auto-close" "outside"
            ]
            [ text "Choose chemical(s)" ]
        , ul [ class "dropdown-menu" ] <|
            List.map checkboxForOneChemical chemicals
        ]


dateFormatRegex : Regex.Regex
dateFormatRegex =
    Maybe.withDefault Regex.never <|
        Regex.fromString "^\\d{4}\\-(0[1-9]|1[012])\\-(0[1-9]|[12][0-9]|3[01])$"


shiftFormatRegex : Regex.Regex
shiftFormatRegex =
    Maybe.withDefault Regex.never <|
        Regex.fromString "^(0\\d|1\\d|2[0-3]):[0-5]\\d\\s*-\\s*(0\\d|1\\d|2[0-3]):[0-5]\\d$"


cannotAddSchedule : JsonBeamtimeScheduleRow -> Bool
cannotAddSchedule se =
    String.isEmpty (String.trim se.users)
        || String.isEmpty (String.trim se.shift)
        || String.isEmpty (String.trim se.date)
        || not (Regex.contains dateFormatRegex (String.trim se.date))
        || not (Regex.contains shiftFormatRegex (String.trim se.shift))


sortJsonBeamtimeScheduleRow : JsonBeamtimeScheduleRow -> JsonBeamtimeScheduleRow -> Order
sortJsonBeamtimeScheduleRow a b =
    case compare a.date b.date of
        LT ->
            LT

        EQ ->
            case compare a.shift b.shift of
                LT ->
                    LT

                EQ ->
                    EQ

                GT ->
                    GT

        GT ->
            GT


scheduleDictFromScheduleList : List JsonBeamtimeScheduleRow -> Dict ShiftId JsonBeamtimeScheduleRow
scheduleDictFromScheduleList shifts =
    Dict.fromList <|
        List.indexedMap (\x y -> ( x, y )) <|
            List.sortWith sortJsonBeamtimeScheduleRow shifts


updateSchedule : ScheduleMsg -> ScheduleModel -> ( ScheduleModel, Cmd ScheduleMsg )
updateSchedule msg model =
    case msg of
        ScheduleUpdated _ ->
            ( model, send ScheduleReceived (getBeamtimeScheduleApiScheduleBeamtimeIdGet model.beamtimeId) )

        ScheduleReceived getScheduleResponse ->
            case getScheduleResponse of
                Ok response ->
                    ( { model
                        | schedule = scheduleDictFromScheduleList response.schedule
                        , newJsonBeamtimeScheduleRow = emptyJsonBeamtimeScheduleRow
                        , editingJsonBeamtimeScheduleRow = emptyJsonBeamtimeScheduleRowToModify
                      }
                    , Cmd.none
                    )

                Err _ ->
                    ( model, Cmd.none )

        SubmitShift ->
            ( { model | newJsonBeamtimeScheduleRow = emptyJsonBeamtimeScheduleRow }
            , send ScheduleUpdated
                (updateBeamtimeScheduleApiSchedulePost
                    { beamtimeId = model.beamtimeId
                    , schedule = model.newJsonBeamtimeScheduleRow :: Dict.values model.schedule
                    }
                )
            )

        UpdateNewShiftByColumn tableColumn value ->
            ( { model
                | newJsonBeamtimeScheduleRow = updateJsonBeamtimeScheduleRowByColumn model.newJsonBeamtimeScheduleRow tableColumn value
              }
            , Cmd.none
            )

        UpdateToModifyShiftByColumn tableColumn value ->
            ( let
                mse =
                    model.editingJsonBeamtimeScheduleRow

                mid =
                    { id = mse.id
                    , scheduleEntry = updateJsonBeamtimeScheduleRowByColumn mse.scheduleEntry tableColumn value
                    }
              in
              { model | editingJsonBeamtimeScheduleRow = mid }
            , Cmd.none
            )

        UpdateNewShiftChemical chemicalIdAsString ->
            case String.toInt chemicalIdAsString of
                Nothing ->
                    ( model, Cmd.none )

                Just chemicalId ->
                    let
                        nse =
                            model.newJsonBeamtimeScheduleRow

                        selectedChemicals =
                            nse.chemicals

                        newSelectedChemicals =
                            if List.member chemicalId selectedChemicals then
                                { nse | chemicals = List.Extra.remove chemicalId selectedChemicals }

                            else
                                { nse | chemicals = chemicalId :: selectedChemicals }
                    in
                    ( { model | newJsonBeamtimeScheduleRow = newSelectedChemicals }, Cmd.none )

        UpdateToModifyShiftChemical chemicalIds ->
            case String.toInt chemicalIds of
                Nothing ->
                    ( model, Cmd.none )

                Just chemicalId ->
                    let
                        ese =
                            model.editingJsonBeamtimeScheduleRow

                        currentEditJsonBeamtimeScheduleRow =
                            ese.scheduleEntry

                        alreadySelectedChemicals =
                            currentEditJsonBeamtimeScheduleRow.chemicals

                        updatedSelectedChemicals =
                            if List.member chemicalId alreadySelectedChemicals then
                                { currentEditJsonBeamtimeScheduleRow | chemicals = List.Extra.remove chemicalId alreadySelectedChemicals }

                            else
                                { currentEditJsonBeamtimeScheduleRow | chemicals = chemicalId :: alreadySelectedChemicals }

                        newEditEntry =
                            { ese
                                | scheduleEntry = updatedSelectedChemicals
                            }
                    in
                    ( { model | editingJsonBeamtimeScheduleRow = newEditEntry }, Cmd.none )

        SubmitDeleteShift scheduleEntry ->
            let
                newScheduleValues =
                    List.Extra.remove scheduleEntry (Dict.values model.schedule)

                newScheduleDict =
                    scheduleDictFromScheduleList newScheduleValues
            in
            ( { model
                | schedule = newScheduleDict
                , newJsonBeamtimeScheduleRow = emptyJsonBeamtimeScheduleRow
                , editingJsonBeamtimeScheduleRow = emptyJsonBeamtimeScheduleRowToModify
                , deletingJsonBeamtimeScheduleRow = emptyJsonBeamtimeScheduleRowToModify
              }
            , send ScheduleUpdated
                (updateBeamtimeScheduleApiSchedulePost
                    { beamtimeId = model.beamtimeId
                    , schedule = newScheduleValues
                    }
                )
            )

        RemoveShift shiftId ->
            let
                newEmptyJsonBeamtimeScheduleRow =
                    { emptyJsonBeamtimeScheduleRowToModify
                        | id = Just shiftId
                        , scheduleEntry = Maybe.withDefault emptyJsonBeamtimeScheduleRow (Dict.get shiftId model.schedule)
                    }
            in
            ( { model
                | deletingJsonBeamtimeScheduleRow = newEmptyJsonBeamtimeScheduleRow
              }
            , Cmd.none
            )

        ChemicalsReceived chemicalsResponse ->
            case chemicalsResponse of
                Ok _ ->
                    ( { model
                        | chemicals = fromResult (Result.map convertChemicalsResponse chemicalsResponse)
                      }
                    , Cmd.none
                    )

                Err _ ->
                    ( model, Cmd.none )

        ModifyShift shiftId ->
            ( let
                newEmptyJsonBeamtimeScheduleRow =
                    { emptyJsonBeamtimeScheduleRowToModify
                        | id = Just shiftId
                        , scheduleEntry = Maybe.withDefault emptyJsonBeamtimeScheduleRow (Dict.get shiftId model.schedule)
                    }
              in
              { model
                | editingJsonBeamtimeScheduleRow = newEmptyJsonBeamtimeScheduleRow
                , deletingJsonBeamtimeScheduleRow = emptyJsonBeamtimeScheduleRowToModify
                , newJsonBeamtimeScheduleRow = emptyJsonBeamtimeScheduleRow
              }
            , Cmd.none
            )

        SubmitModifiedShift modifiedShiftId ->
            let
                mse =
                    model.editingJsonBeamtimeScheduleRow

                newDict =
                    Dict.remove modifiedShiftId model.schedule

                shifts =
                    Dict.values newDict

                allShifts =
                    mse.scheduleEntry :: shifts
            in
            ( model
            , send ScheduleUpdated
                (updateBeamtimeScheduleApiSchedulePost
                    { beamtimeId = model.beamtimeId
                    , schedule = allShifts
                    }
                )
            )

        ResetToModifyShift ->
            ( { model | editingJsonBeamtimeScheduleRow = emptyJsonBeamtimeScheduleRowToModify }
            , send ScheduleReceived (getBeamtimeScheduleApiScheduleBeamtimeIdGet model.beamtimeId)
            )

        ResetToDeleteShift ->
            ( { model | deletingJsonBeamtimeScheduleRow = emptyJsonBeamtimeScheduleRowToModify }
            , send ScheduleReceived (getBeamtimeScheduleApiScheduleBeamtimeIdGet model.beamtimeId)
            )


updateJsonBeamtimeScheduleRowByColumn : JsonBeamtimeScheduleRow -> TableColumn -> String -> JsonBeamtimeScheduleRow
updateJsonBeamtimeScheduleRowByColumn se column data =
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
    , chemicals = se.chemicals
    }


dateEntry : String -> String
dateEntry inputDate =
    case Date.fromIsoString inputDate of
        Ok date ->
            Date.format "EEE, dd MMM y" date

        Err _ ->
            "Date " ++ inputDate ++ " is not parseable"
