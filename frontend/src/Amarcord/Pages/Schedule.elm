module Amarcord.Pages.Schedule exposing (..)

import Amarcord.API.Requests exposing (ExperimentType, ExperimentTypesResponse, RequestError, SamplesResponse, ScheduleEntry, ScheduleResponse, httpGetSamples, httpGetSchedule, httpUpdateSchedule)
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon)
import Amarcord.File exposing (File)
import Amarcord.Html exposing (input_)
import Amarcord.Sample exposing (Sample, SampleId)
import Date
import Dict exposing (Dict)
import Html exposing (Html, button, div, em, h3, hr, option, select, span, table, tbody, td, text, th, thead, tr)
import Html.Attributes exposing (class, disabled, id, placeholder, selected, style, type_, value)
import Html.Events exposing (onClick, onInput)
import List.Extra
import Maybe.Extra
import Regex
import RemoteData exposing (RemoteData(..), fromResult)
import Set


type ScheduleMsg
    = ScheduleUpdated (Result RequestError ())
    | ScheduleReceived (Result RequestError ScheduleResponse)
    | SubmitShift
    | ModifyShift ShiftId
    | SubmitModifiedShift ShiftId
    | RemoveShift ScheduleEntry
    | UpdateNewShiftByColumn TableColumn String
    | UpdateNewShiftSample String
    | UpdateToModifyShiftByColumn TableColumn String
    | UpdateToModifyShiftSample String
    | ResetToModifyShift
    | SamplesReceived SamplesResponse


type alias SamplesAndAttributi =
    { samples : List (Sample SampleId (AttributoMap AttributoValue) File)
    , attributi : List (Attributo AttributoType)
    }


type alias ShiftId =
    Int


type alias EditSchedule =
    { scheduleEntry : ScheduleEntry, id : Maybe ShiftId }


type alias ScheduleModel =
    { schedule : Dict ShiftId ScheduleEntry
    , newScheduleEntry : ScheduleEntry
    , editingScheduleEntry : EditSchedule
    , samples : RemoteData RequestError SamplesAndAttributi
    }


emptyScheduleEntry : ScheduleEntry
emptyScheduleEntry =
    { users = "", date = "", shift = "", sampleId = Nothing, comment = "", tdSupport = "" }


emptyScheduleEntryToEdit : EditSchedule
emptyScheduleEntryToEdit =
    { scheduleEntry = emptyScheduleEntry, id = Nothing }


type TableColumn
    = Date
    | Shift
    | Users
    | TdSupport
    | Sample
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

        Sample ->
            style "width" "12%"

        Comment ->
            style "width" "20%"

        Actions ->
            style "width" "8%"


initSchedule : ( ScheduleModel, Cmd ScheduleMsg )
initSchedule =
    ( { samples = Loading
      , schedule = Dict.empty
      , newScheduleEntry = emptyScheduleEntry
      , editingScheduleEntry = emptyScheduleEntryToEdit
      }
    , Cmd.batch [ httpGetSchedule ScheduleReceived, httpGetSamples SamplesReceived ]
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
                        , th [ styleColumn Sample ] [ text "Sample" ]
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
        , case unscheduledSamplesNames model of
            Nothing ->
                div [] []

            Just us ->
                div []
                    [ em [] [ text <| "Following samples have not yet been scheduled: " ]
                    , span [] [ text <| us ]
                    ]
        ]


unscheduledSamplesNames : ScheduleModel -> Maybe String
unscheduledSamplesNames model =
    let
        samples =
            case model.samples of
                Success s ->
                    s.samples

                _ ->
                    []

        sampleIsNotScheduled s =
            List.member s.id (Maybe.Extra.values (List.map .sampleId (Dict.values model.schedule))) == False

        unscheduledSamples =
            List.filter sampleIsNotScheduled samples
    in
    if List.isEmpty unscheduledSamples then
        Nothing

    else
        Just (String.join ", " (List.map .name unscheduledSamples))


scheduleEntryView : ScheduleModel -> ( ShiftId, ScheduleEntry ) -> Html ScheduleMsg
scheduleEntryView model shiftIdValue =
    let
        modifiedScheduleId =
            model.editingScheduleEntry.id
    in
    case modifiedScheduleId of
        Nothing ->
            let
                entry =
                    Tuple.second shiftIdValue

                shiftId =
                    Tuple.first shiftIdValue
            in
            readOnlyScheduleEntryView model shiftId entry

        Just idShiftToModify ->
            let
                entry =
                    Tuple.second shiftIdValue

                shiftId =
                    Tuple.first shiftIdValue

                entryToModify =
                    model.editingScheduleEntry.scheduleEntry
            in
            if idShiftToModify == shiftId then
                editingScheduleEntryView model entryToModify idShiftToModify

            else
                readOnlyScheduleEntryView model shiftId entry


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
        , td [ styleColumn Sample ]
            [ sampleDropdownEdit entryToModify.sampleId model
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
            [ button [ class "btn btn-success", type_ "button", onClick <| SubmitModifiedShift idShiftToModify, disabled <| cannotAddSchedule entryToModify ] [ icon { name = "check" } ]
            , button [ class "btn btn-info", type_ "button", onClick ResetToModifyShift ] [ icon { name = "x" } ]
            ]
        ]


readOnlyScheduleEntryView : ScheduleModel -> ShiftId -> ScheduleEntry -> Html ScheduleMsg
readOnlyScheduleEntryView model shiftid entry =
    tr []
        [ td [ styleColumn Date ] [ text <| dateEntry entry.date ]
        , td [ styleColumn Shift ] [ text entry.shift ]
        , td [ styleColumn Users ] [ text entry.users ]
        , td [ styleColumn TdSupport ] [ text entry.tdSupport ]
        , td [ styleColumn Sample ]
            [ case entry.sampleId of
                Nothing ->
                    text ""

                Just sampleId ->
                    case model.samples of
                        Success samples ->
                            case List.Extra.find (\sample -> sampleId == sample.id) samples.samples of
                                Nothing ->
                                    text ("Sample ID " ++ String.fromInt sampleId ++ " is unknown")

                                Just s ->
                                    text s.name

                        Loading ->
                            text ""

                        _ ->
                            text ("Sample ID " ++ String.fromInt sampleId ++ " is unknown")
            ]
        , td [ styleColumn Comment ]
            [ text entry.comment ]
        , td [ styleColumn Actions ]
            [ button [ class "btn btn-primary", type_ "button", onClick (ModifyShift shiftid) ] [ icon { name = "pencil-square" } ]
            , button [ class "btn btn-warning", type_ "button", onClick (RemoveShift entry) ] [ icon { name = "trash" } ]
            ]
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
        , td [ styleColumn Sample ]
            [ sampleDropdown model
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
            [ button [ class "btn btn-success", type_ "button", onClick SubmitShift, disabled <| cannotAddSchedule model.newScheduleEntry ] [ icon { name = "calendar-plus" } ]
            ]
        ]


sampleDropdown : ScheduleModel -> Html ScheduleMsg
sampleDropdown model =
    let
        optionEntry sample =
            option [ value <| String.fromInt sample.id ] [ text sample.name ]

        listSamplesOptions =
            List.map optionEntry <|
                case model.samples of
                    Success samples ->
                        samples.samples

                    _ ->
                        []
    in
    select [ class "form-select", onInput UpdateNewShiftSample ] <|
        (option [ selected True ] [ text "«none selected»" ]
            :: listSamplesOptions
        )


sampleDropdownEdit : Maybe Int -> ScheduleModel -> Html ScheduleMsg
sampleDropdownEdit modifiableSampleId model =
    let
        isNothingSelected =
            case modifiableSampleId of
                Nothing ->
                    False

                Just _ ->
                    True

        isSampleSelected sampleId =
            case modifiableSampleId of
                Nothing ->
                    False

                Just i ->
                    sampleId == i

        optionEntry sample =
            option [ selected (isSampleSelected sample.id), value <| String.fromInt sample.id ] [ text sample.name ]

        listSamplesOptions =
            List.map optionEntry <|
                case model.samples of
                    Success samples ->
                        samples.samples

                    _ ->
                        []
    in
    select [ class "form-select", onInput UpdateToModifyShiftSample ] <|
        (option [ selected isNothingSelected ] [ text "«none selected»" ]
            :: listSamplesOptions
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
                    case a.sampleId of
                        Nothing ->
                            EQ

                        Just sa ->
                            case b.sampleId of
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
                        , editingScheduleEntry = emptyScheduleEntryToEdit
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

        UpdateNewShiftSample sampleId ->
            let
                nse =
                    model.newScheduleEntry

                ns =
                    { nse | sampleId = String.toInt sampleId }
            in
            ( { model | newScheduleEntry = ns }, Cmd.none )

        RemoveShift scheduleEntry ->
            let
                newScheduleValues =
                    List.Extra.remove scheduleEntry (Dict.values model.schedule)

                newScheduleDict =
                    scheduleDictFromScheduleList newScheduleValues
            in
            ( { model
                | schedule = newScheduleDict
                , newScheduleEntry = emptyScheduleEntry
                , editingScheduleEntry = emptyScheduleEntryToEdit
              }
            , httpUpdateSchedule ScheduleUpdated newScheduleValues
            )

        SamplesReceived samplesResponse ->
            case samplesResponse of
                Ok _ ->
                    ( { model
                        | samples =
                            fromResult <|
                                Result.map (\( samples, attributi ) -> { samples = samples, attributi = attributi }) <|
                                    samplesResponse
                      }
                    , Cmd.none
                    )

                Err _ ->
                    ( model, Cmd.none )

        ModifyShift shiftId ->
            ( let
                ff =
                    emptyScheduleEntryToEdit

                f1 =
                    { ff
                        | id = Just shiftId
                        , scheduleEntry = Maybe.withDefault emptyScheduleEntry (Dict.get shiftId model.schedule)
                    }
              in
              { model
                | editingScheduleEntry = f1
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

        UpdateToModifyShiftSample sampleId ->
            let
                modScheduleEntry =
                    model.editingScheduleEntry

                mid =
                    modScheduleEntry.scheduleEntry

                fig =
                    { mid | sampleId = String.toInt sampleId }

                nse2 =
                    { modScheduleEntry | scheduleEntry = fig }
            in
            ( { model | editingScheduleEntry = nse2 }, Cmd.none )

        ResetToModifyShift ->
            ( { model | editingScheduleEntry = emptyScheduleEntryToEdit }, httpGetSchedule ScheduleReceived )


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
    , sampleId = se.sampleId
    }


dateEntry : String -> String
dateEntry inputDate =
    case Date.fromIsoString inputDate of
        Ok date ->
            Date.format "EEE, dd MMM y" date

        Err _ ->
            "Date " ++ inputDate ++ " is not parseable"
