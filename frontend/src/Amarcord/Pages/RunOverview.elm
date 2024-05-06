module Amarcord.Pages.RunOverview exposing (Model, Msg(..), init, update, view)

import Amarcord.API.ExperimentType exposing (ExperimentTypeId, experimentTypeIdDict)
import Amarcord.API.Requests exposing (BeamtimeId, RunEventDate(..), RunEventDateFilter, RunExternalId(..), RunFilter(..), RunInternalId(..), emptyRunEventDateFilter, emptyRunFilter, runEventDateFilter, runEventDateToString, runFilterToString, runInternalIdToInt, runInternalIdToString, specificRunEventDateFilter)
import Amarcord.API.RequestsHtml exposing (showHttpError)
import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo exposing (Attributo, AttributoType(..), attributoExposureTime, attributoMapToListOfAttributi, convertAttributoFromApi, convertAttributoMapFromApi, retrieveFloatAttributoValue)
import Amarcord.AttributoHtml exposing (AttributoFormMsg(..), AttributoNameWithValueUpdate, EditableAttributiAndOriginal, EditableAttributo, convertEditValues, createEditableAttributi, editEditableAttributi, formatFloatHumanFriendly, formatIntHumanFriendly, isEditValueChemicalId, makeAttributoHeader, resetEditableAttributo, unsavedAttributoChanges, viewAttributoCell, viewAttributoForm, viewRunExperimentTypeCell)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert, mimeTypeToIcon, spinner, viewRemoteDataHttp)
import Amarcord.Chemical exposing (Chemical, chemicalIdDict, convertChemicalFromApi)
import Amarcord.ColumnChooser as ColumnChooser
import Amarcord.Constants exposing (manualAttributiGroup, manualGlobalAttributiGroup)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.EventForm as EventForm
import Amarcord.Gauge exposing (gauge, thisFillColor, totalFillColor)
import Amarcord.Html exposing (div_, form_, h1_, h2_, h3_, h5_, hr_, img_, input_, li_, onIntInput, p_, strongText, tbody_, td_, th_, thead_)
import Amarcord.LocalStorage exposing (LocalStorage)
import Amarcord.MarkdownUtil exposing (markupWithoutErrors)
import Amarcord.Route exposing (makeFilesLink)
import Amarcord.Util exposing (HereAndNow, formatPosixHumanFriendly, formatPosixTimeOfDayHumanFriendly, listContainsBy, posixBefore, posixDiffHumanFriendly, scrollToTop, secondsDiffHumanFriendly)
import Api exposing (send)
import Api.Data exposing (ChemicalType(..), JsonAttributiIdAndRole, JsonChangeRunExperimentTypeOutput, JsonCreateDataSetFromRunOutput, JsonDeleteEventOutput, JsonEvent, JsonExperimentType, JsonFileOutput, JsonReadRuns, JsonRun, JsonUpdateRunOutput, JsonUserConfigurationSingleOutput)
import Api.Request.Config exposing (updateUserConfigurationSingleApiUserConfigBeamtimeIdKeyValuePatch)
import Api.Request.Datasets exposing (createDataSetFromRunApiDataSetsFromRunPost)
import Api.Request.Events exposing (deleteEventApiEventsDelete)
import Api.Request.Experimenttypes exposing (changeCurrentRunExperimentTypeApiExperimentTypesChangeForRunPost)
import Api.Request.Runs exposing (readRunsApiRunsBeamtimeIdGet, updateRunApiRunsPatch)
import Char exposing (fromCode)
import Date
import Dict exposing (Dict)
import Html exposing (Html, a, button, div, em, figcaption, figure, form, h4, label, option, p, select, span, table, td, text, tr, ul)
import Html.Attributes exposing (checked, class, colspan, disabled, for, href, id, selected, src, style, type_, value)
import Html.Events exposing (onClick, onInput)
import Html.Events.Extra exposing (onEnter)
import Http
import List exposing (head)
import List.Extra as ListExtra exposing (find)
import Maybe
import Maybe.Extra as MaybeExtra exposing (isNothing)
import RemoteData exposing (RemoteData(..), fromResult, isLoading, isSuccess)
import String
import Time exposing (Posix, Zone, millisToPosix, posixToMillis)
import Tuple exposing (first, second)


type Msg
    = RunsReceived (Result Http.Error JsonReadRuns)
    | Refresh Posix
    | EventDelete Int
    | EventDeleteFinished (Result Http.Error JsonDeleteEventOutput)
    | EventFormMsg EventForm.Msg
    | RunEditInfoValueUpdate AttributoNameWithValueUpdate
    | RunEditInfoExperimentTypeIdChanged Int
    | RunEditSubmit
    | RunEditFinished (Result Http.Error JsonUpdateRunOutput)
    | RunInitiateEdit JsonRun
    | RunEditCancel
    | RunFilterSubMsg RunFilterMsg
    | Nop
    | SelectedExperimentTypeChanged String
    | ChangeCurrentExperimentType
    | ColumnChooserMessage ColumnChooser.Msg
    | ChangeAutoPilot Bool
    | ChangeOnlineCrystFEL Bool
    | ChangeShowAllAttributi Bool
    | AutoPilotToggled (Result Http.Error JsonUserConfigurationSingleOutput)
    | OnlineCrystFELToggled (Result Http.Error JsonUserConfigurationSingleOutput)
    | CreateDataSetFromRun RunInternalId
    | CreateDataSetFromRunFinished (Result Http.Error JsonCreateDataSetFromRunOutput)
    | ChangeCurrentExperimentTypeFinished (Maybe ExperimentTypeId) (Result Http.Error JsonChangeRunExperimentTypeOutput)
    | ResetDate
    | SetRunDateFilter RunEventDate


type alias RunEditInfo =
    { runId : RunInternalId
    , runExternalId : RunExternalId
    , started : Posix
    , stopped : Maybe Posix
    , experimentTypeId : Int
    , editableAttributi : EditableAttributiAndOriginal

    -- This is to handle a tricky case: usually we want to stay with the latest run so we can quickly change settings.
    -- If we manually click on an older run to edit it, we don't want to then jump to the latest one.
    , initiatedManually : Bool
    , showAllAttributi : Bool
    }


type alias RunFilterInfo =
    { runFilter : RunFilter
    , nextRunFilter : String
    , runFilterRequest : RemoteData Http.Error JsonReadRuns
    }


type alias RunDateFilterInfo =
    { runDateFilter : RunEventDateFilter }


type RunFilterMsg
    = RunFilterEdit String
    | RunFilterReset
    | RunFilterSubmit
    | RunFilterSubmitFinished (Result Http.Error JsonReadRuns)


initRunFilter : RunFilterInfo
initRunFilter =
    { runFilter = emptyRunFilter
    , nextRunFilter = ""
    , runFilterRequest = NotAsked
    }


initRunDateFilter : RunDateFilterInfo
initRunDateFilter =
    { runDateFilter = emptyRunEventDateFilter
    }


viewRunFilter : RunFilterInfo -> Html RunFilterMsg
viewRunFilter model =
    form_
        [ h5_ [ text "Run filter" ]
        , div [ class "input-group mb-3" ]
            [ input_
                [ type_ "text"
                , class "form-control"
                , value model.nextRunFilter
                , onInput RunFilterEdit
                , disabled (isLoading model.runFilterRequest)
                , onEnter RunFilterSubmit
                ]
            , button
                [ class "btn btn-outline-secondary"
                , onClick RunFilterReset
                , type_ "button"
                ]
                [ text "Reset" ]
            , button
                [ class "btn btn-secondary"
                , disabled (isLoading model.runFilterRequest)
                , type_ "button"
                , onClick RunFilterSubmit
                ]
                [ icon { name = "save" }, text " Update" ]
            ]
        , case model.runFilterRequest of
            Failure e ->
                div [ class "mb-3" ] [ div_ [ makeAlert [ AlertDanger ] [ showHttpError e ] ] ]

            _ ->
                text ""
        ]


updateRunFilter : BeamtimeId -> RunFilterInfo -> RunFilterMsg -> ( RunFilterInfo, Cmd RunFilterMsg )
updateRunFilter beamtimeId model msg =
    case msg of
        RunFilterEdit newRunFilter ->
            ( { model | nextRunFilter = newRunFilter }, Cmd.none )

        RunFilterReset ->
            ( { model | nextRunFilter = runFilterToString model.runFilter, runFilterRequest = NotAsked }, Cmd.none )

        RunFilterSubmit ->
            ( { model | runFilterRequest = Loading }
            , send RunFilterSubmitFinished
                (readRunsApiRunsBeamtimeIdGet
                    beamtimeId
                    (Maybe.map runEventDateToString <| runEventDateFilter <| emptyRunEventDateFilter)
                    (Just model.nextRunFilter)
                )
            )

        RunFilterSubmitFinished response ->
            case response of
                Err e ->
                    ( { model | runFilterRequest = Failure e }, Cmd.none )

                Ok v ->
                    ( { model | runFilter = RunFilter model.nextRunFilter, runFilterRequest = Success v }, Cmd.none )


type alias Model =
    { runs : RemoteData Http.Error JsonReadRuns
    , myTimeZone : Zone
    , refreshRequest : RemoteData Http.Error ()
    , eventForm : EventForm.Model
    , now : Posix
    , runDateFilter : RunDateFilterInfo
    , runDates : List RunEventDate
    , runEditInfo : Maybe RunEditInfo
    , runEditRequest : RemoteData Http.Error JsonUpdateRunOutput
    , runFilter : RunFilterInfo
    , submitErrors : List String
    , currentExperimentType : Maybe JsonExperimentType
    , selectedExperimentType : Maybe JsonExperimentType
    , columnChooser : ColumnChooser.Model
    , localStorage : Maybe LocalStorage
    , dataSetFromRunRequest : RemoteData Http.Error JsonCreateDataSetFromRunOutput
    , changeExperimentTypeRequest : RemoteData Http.Error JsonChangeRunExperimentTypeOutput
    , beamtimeId : BeamtimeId
    }


init : HereAndNow -> Maybe LocalStorage -> BeamtimeId -> ( Model, Cmd Msg )
init { zone, now } localStorage beamtimeId =
    ( { runs = Loading
      , myTimeZone = zone
      , refreshRequest = NotAsked
      , eventForm = EventForm.init beamtimeId
      , now = now
      , runDates = []
      , runDateFilter = initRunDateFilter
      , runEditInfo = Nothing
      , runEditRequest = NotAsked
      , runFilter = initRunFilter
      , submitErrors = []
      , currentExperimentType = Nothing
      , selectedExperimentType = Nothing
      , columnChooser = ColumnChooser.init localStorage []
      , localStorage = localStorage
      , dataSetFromRunRequest = NotAsked
      , changeExperimentTypeRequest = NotAsked
      , beamtimeId = beamtimeId
      }
    , send RunsReceived
        (readRunsApiRunsBeamtimeIdGet
            beamtimeId
            (Maybe.map runEventDateToString <| runEventDateFilter emptyRunEventDateFilter)
            (Just <| runFilterToString emptyRunFilter)
        )
    )


attributiColumnHeaders : List (Attributo AttributoType) -> List (Html msg)
attributiColumnHeaders =
    List.map (th_ << makeAttributoHeader)


attributiColumns : Zone -> Dict Int String -> Dict Int String -> List (Attributo AttributoType) -> JsonRun -> List (Html Msg)
attributiColumns zone chemicalIds experimentTypeIds attributi run =
    let
        viewCell : Attributo AttributoType -> Maybe (Html Msg)
        viewCell a =
            if a.associatedTable == AssociatedTable.Run then
                Just <|
                    if a.name == virtualExperimentTypeAttributoName then
                        viewRunExperimentTypeCell (Maybe.withDefault "unknown experiment type" <| Dict.get run.experimentTypeId experimentTypeIds)

                    else
                        viewAttributoCell
                            { shortDateTime = True
                            , colorize = True
                            , withUnit = False
                            }
                            zone
                            chemicalIds
                            (convertAttributoMapFromApi run.attributi)
                            a

            else
                Nothing
    in
    List.filterMap viewCell attributi


viewRunRow : Zone -> Dict Int String -> Dict Int String -> List (Attributo AttributoType) -> JsonRun -> Html Msg
viewRunRow zone chemicalIds experimentTypeIds attributi r =
    tr [ style "white-space" "nowrap" ] <|
        td_ [ strongText (String.fromInt r.externalId) ]
            :: td_ [ text (String.fromInt r.id) ]
            :: td_ [ text <| formatPosixTimeOfDayHumanFriendly zone (millisToPosix r.started) ]
            :: td_ [ text <| Maybe.withDefault "" <| Maybe.map (formatPosixTimeOfDayHumanFriendly zone) (Maybe.map millisToPosix r.stopped) ]
            :: attributiColumns zone chemicalIds experimentTypeIds attributi r
            ++ [ td_
                    [ button
                        [ class "btn btn-link amarcord-small-link-button"
                        , onClick (RunInitiateEdit r)
                        ]
                        [ icon { name = "pencil-square" } ]
                    ]
               ]


viewEventRow : Zone -> Int -> JsonEvent -> Html Msg
viewEventRow zone attributoColumnCount e =
    let
        viewFile : JsonFileOutput -> Html Msg
        viewFile { type__, fileName, id } =
            li_
                [ mimeTypeToIcon type__
                , text " "
                , a [ href (makeFilesLink id) ] [ text fileName ]
                ]

        maybeFiles =
            if List.isEmpty e.files then
                [ text "" ]

            else
                [ ul [ class "me-0" ] (List.map viewFile e.files) ]

        mainContent =
            [ button [ class "btn btn-sm btn-link amarcord-small-link-button", type_ "button", onClick (EventDelete e.id) ] [ icon { name = "trash" } ]
            , strongText <| " " ++ e.source ++ " "
            , markupWithoutErrors e.text
            ]
                ++ maybeFiles
    in
    tr [ class "bg-light" ]
        [ td_ []
        , td_ [ text <| formatPosixTimeOfDayHumanFriendly zone (millisToPosix e.created) ]
        , td [ colspan attributoColumnCount ] mainContent
        ]


viewRunAndEventRows : Zone -> Dict Int String -> Dict Int String -> List (Attributo AttributoType) -> List JsonRun -> List JsonEvent -> List (Html Msg)
viewRunAndEventRows zone chemicalIds experimentTypeIds attributi runs events =
    case ( head runs, head events ) of
        -- No elements, neither runs nor events, left anymore
        ( Nothing, Nothing ) ->
            []

        -- Only runs left
        ( Just _, Nothing ) ->
            List.map (viewRunRow zone chemicalIds experimentTypeIds attributi) runs

        -- Only events left
        ( Nothing, Just _ ) ->
            List.map (viewEventRow zone (List.length attributi)) events

        -- We have events and runs and have to compare the dates
        ( Just run, Just event ) ->
            if posixBefore (millisToPosix event.created) (millisToPosix run.started) then
                viewRunRow zone chemicalIds experimentTypeIds attributi run :: viewRunAndEventRows zone chemicalIds experimentTypeIds attributi (List.drop 1 runs) events

            else
                viewEventRow zone (List.length attributi) event :: viewRunAndEventRows zone chemicalIds experimentTypeIds attributi runs (List.drop 1 events)


viewRunsTable : Zone -> List (Attributo AttributoType) -> JsonReadRuns -> Html Msg
viewRunsTable zone chosenColumns { runs, events, chemicals, experimentTypes } =
    let
        runRows : List (Html Msg)
        runRows =
            viewRunAndEventRows zone (chemicalIdDict <| List.map convertChemicalFromApi chemicals) (experimentTypeIdDict experimentTypes) chosenColumns runs events
    in
    table [ class "table amarcord-table-fix-head table-bordered table-hover" ]
        [ thead_
            [ tr [ class "align-top" ] <|
                th_ [ text "ID" ]
                    :: th_ [ text "Internal ID" ]
                    :: th_ [ text "Started" ]
                    :: th_ [ text "Stopped" ]
                    :: attributiColumnHeaders chosenColumns
                    ++ [ th_ [ text "Actions" ] ]
            ]
        , tbody_ runRows
        ]


dataSetInformation :
    Zone
    -> JsonRun
    -> RemoteData Http.Error JsonCreateDataSetFromRunOutput
    -> Maybe JsonExperimentType
    -> JsonReadRuns
    -> List (Html Msg)
dataSetInformation zone run dataSetFromRunRequest currentExperimentTypeMaybe rrc =
    case currentExperimentTypeMaybe of
        Nothing ->
            [ p [ class "text-muted" ] [ text "No experiment type selected, cannot display data set information." ] ]

        Just currentExperimentType ->
            case find (\ds -> ds.experimentTypeId == currentExperimentType.id && List.member ds.id run.dataSets) rrc.dataSets of
                Nothing ->
                    [ p [ class "text-muted" ]
                        [ text <| "Run is not part of any data set in \"" ++ currentExperimentType.name ++ "\". You can automatically create a data set that matches the current run's attributi."
                        ]
                    , button
                        [ type_ "button"
                        , class "btn btn-secondary"
                        , onClick (CreateDataSetFromRun (RunInternalId run.id))
                        , disabled (isLoading dataSetFromRunRequest)
                        ]
                        [ icon { name = "plus-lg" }, text " Create data set from run" ]
                    , viewRemoteDataHttp "Data set created" dataSetFromRunRequest
                    ]

                Just ds ->
                    let
                        indexingProgress =
                            let
                                progressSummary =
                                    case ds.summary of
                                        Nothing ->
                                            run.summary

                                        Just dsSummary ->
                                            dsSummary

                                etaFor10kFrames =
                                    let
                                        ir =
                                            run.summary.indexingRate

                                        hr =
                                            run.summary.hitRate
                                    in
                                    if ir > 0.01 && hr > 0.01 then
                                        let
                                            runExposureTime : Maybe Float
                                            runExposureTime =
                                                ListExtra.find
                                                    (\a -> a.name == attributoExposureTime)
                                                    rrc.attributi
                                                    |> Maybe.andThen (\a -> retrieveFloatAttributoValue a.id (convertAttributoMapFromApi run.attributi))
                                        in
                                        runExposureTime
                                            |> Maybe.andThen
                                                (\realExposureTime ->
                                                    let
                                                        remainingFrames =
                                                            10000 - progressSummary.indexedFrames
                                                    in
                                                    if remainingFrames > 0 then
                                                        let
                                                            remainingFramesToCapture =
                                                                round <| toFloat remainingFrames / (ir / 100.0 * hr / 100.0)

                                                            framesPerSecond =
                                                                1000 / (2 * realExposureTime)

                                                            indexedFramesPerSecond =
                                                                framesPerSecond * ir / 100.0 * hr / 100.0

                                                            remainingTimeStr =
                                                                secondsDiffHumanFriendly <| round (toFloat remainingFrames / indexedFramesPerSecond)
                                                        in
                                                        Just <| text <| "Remaining time: " ++ remainingTimeStr ++ ", remaining frames " ++ formatIntHumanFriendly remainingFramesToCapture

                                                    else
                                                        Nothing
                                                )

                                    else
                                        Nothing

                                -- case run.summary.indexingRate of
                                --     Nothing ->
                                --         Nothing
                                --     Just ir ->
                                --         case run.summary.hitRate of
                                --             Nothing ->
                                --                 Nothing
                                --             Just hr ->
                                --                 if ir > 0.01 && hr > 0.01 then
                                --                     let
                                --                         runExposureTime : Maybe Float
                                --                         runExposureTime =
                                --                             ListExtra.find
                                --                                 (\a -> a.name == attributoExposureTime)
                                --                                 rrc.attributi
                                --                                 |> Maybe.andThen (\a -> retrieveFloatAttributoValue a.id run.attributi)
                                --                     in
                                --                     case runExposureTime of
                                --                         Nothing ->
                                --                             Nothing
                                --                         Just realExposureTime ->
                                --                             let
                                --                                 remainingFrames =
                                --                                     10000 - progressSummary.indexedFrames
                                --                             in
                                --                             if remainingFrames > 0 then
                                --                                 let
                                --                                     remainingFramesToCapture =
                                --                                         round <| toFloat remainingFrames / (ir / 100.0 * hr / 100.0)
                                --                                     framesPerSecond =
                                --                                         1000 / (2 * realExposureTime)
                                --                                     indexedFramesPerSecond =
                                --                                         framesPerSecond * ir / 100.0 * hr / 100.0
                                --                                     remainingTimeStr =
                                --                                         secondsDiffHumanFriendly <| round (toFloat remainingFrames / indexedFramesPerSecond)
                                --                                 in
                                --                                 Just <| text <| "Remaining time: " ++ remainingTimeStr ++ ", remaining frames " ++ formatIntHumanFriendly remainingFramesToCapture
                                --                             else
                                --                                 Nothing
                                --                 else
                                --                     Nothing
                            in
                            [ div [ class "d-flex justify-content-center" ]
                                [ div [ class "progress", style "width" "80%" ]
                                    [ div
                                        [ class
                                            ("progress-bar"
                                                ++ (if MaybeExtra.isJust run.stopped then
                                                        ""

                                                    else
                                                        " progress-bar-striped progress-bar-animated"
                                                   )
                                            )
                                        , style "width" ((String.fromInt <| min 100 <| round <| toFloat progressSummary.indexedFrames / 10000 * 100.0) ++ "%")
                                        ]
                                        []
                                    ]
                                ]
                            , div [ class "d-flex justify-content-center" ] [ em [ class "amarcord-small-text" ] [ text <| "Indexed frames: " ++ formatIntHumanFriendly progressSummary.indexedFrames ++ "/" ++ formatIntHumanFriendly 10000 ] ]
                            , case etaFor10kFrames of
                                Nothing ->
                                    text ""

                                Just eta ->
                                    div [ class "d-flex justify-content-center" ] [ em [ class "amarcord-small-text" ] [ eta ] ]
                            ]

                        smallBox color =
                            span [ style "color" color ] [ text <| String.fromChar <| fromCode 9632 ]

                        twoValueGauge : String -> Float -> Maybe Float -> List (Html msg)
                        twoValueGauge title thisValue total =
                            case total of
                                Nothing ->
                                    [ gauge (Just thisValue) total, div_ [ em [ class "amarcord-small-text" ] [ smallBox thisFillColor, text <| " " ++ title ++ " " ++ formatFloatHumanFriendly thisValue ++ "%" ] ] ]

                                Just totalValue ->
                                    [ gauge (Just thisValue) total
                                    , div_
                                        [ em [ class "amarcord-small-text" ]
                                            [ smallBox thisFillColor
                                            , text (" run: " ++ formatFloatHumanFriendly thisValue ++ "% | ")
                                            , smallBox totalFillColor
                                            , text <| " data set: " ++ formatFloatHumanFriendly totalValue ++ "%"
                                            ]
                                        ]
                                    ]
                    in
                    [ div [ class "d-flex flex-row justify-content-evenly" ]
                        [ div [ class "text-center" ] (twoValueGauge "Indexing rate" run.summary.indexingRate Nothing)
                        , div [ class "text-center" ] (twoValueGauge "Hit rate" run.summary.hitRate Nothing)
                        ]
                    , div [ class "mb-3" ] indexingProgress
                    , h3_ [ text "Data set" ]
                    , viewDataSetTable (List.map convertAttributoFromApi rrc.attributi)
                        zone
                        (chemicalIdDict (List.map convertChemicalFromApi rrc.chemicals))
                        (convertAttributoMapFromApi ds.attributi)
                        True
                        Nothing
                    ]


posixDiffHumanFriendlyLongDurationsExact : Zone -> Posix -> Posix -> String
posixDiffHumanFriendlyLongDurationsExact zone relative now =
    if posixToMillis now - posixToMillis relative > 48 * 60 * 60 * 1000 then
        formatPosixHumanFriendly zone relative

    else
        posixDiffHumanFriendly now relative ++ " ago "


viewCurrentRun :
    Zone
    -> Posix
    -> Maybe JsonExperimentType
    -> Maybe JsonExperimentType
    -> RemoteData Http.Error JsonChangeRunExperimentTypeOutput
    -> RemoteData Http.Error JsonCreateDataSetFromRunOutput
    -> JsonReadRuns
    -> List (Html Msg)
viewCurrentRun zone now selectedExperimentType currentExperimentType changeExperimentTypeRequest dataSetFromRunRequest rrc =
    -- Here, we assume runs are ordered so the first one is the latest one.
    case head rrc.runs of
        Nothing ->
            List.singleton <| text ""

        Just ({ id, externalId, started, stopped } as run) ->
            let
                autoPilot =
                    [ div [ class "form-check form-switch mb-3" ]
                        [ input_ [ type_ "checkbox", Html.Attributes.id "auto-pilot", class "form-check-input", checked rrc.userConfig.autoPilot, onInput (always (ChangeAutoPilot (not rrc.userConfig.autoPilot))) ]
                        , label [ class "form-check-label", for "auto-pilot" ] [ text "Auto pilot" ]
                        , div [ class "form-text" ] [ text "Manual attributi will be copied over from the previous run. Be careful not to change experimental conditions if this is active." ]
                        ]
                    ]

                onlineCrystFEL =
                    [ div [ class "form-check form-switch mb-3" ]
                        [ input_ [ type_ "checkbox", Html.Attributes.id "crystfel-online", class "form-check-input", checked rrc.userConfig.onlineCrystfel, onInput (always (ChangeOnlineCrystFEL (not rrc.userConfig.onlineCrystfel))) ]
                        , label [ class "form-check-label", for "crystfel-online" ] [ text "Use CrystFEL Online" ]
                        ]
                    , runningIndexingJobsIndicator
                    ]

                runIdsWithRunningIndexingJobs =
                    List.filterMap
                        (\r ->
                            if List.isEmpty r.runningIndexingJobs then
                                Nothing

                            else if r.id == id then
                                Maybe.map (\_ -> r.externalId) stopped

                            else if r.id < id then
                                Just r.externalId

                            else
                                Nothing
                        )
                        rrc.runs

                runningIndexingJobsIndicator =
                    if List.isEmpty runIdsWithRunningIndexingJobs then
                        div [] []

                    else
                        div [ class "pb-3 pt-1", style "display" "flex" ]
                            [ div [ class "text-secondary ", style "flex" "0 0 5%" ]
                                [ spinner True ]
                            , div [ class "text-secondary align-bottom", style "flex" "1" ]
                                [ text
                                    (String.join ", " (List.map String.fromInt runIdsWithRunningIndexingJobs)
                                        |> (++) " CrystFEL online indexing jobs still running for following runs: "
                                    )
                                ]
                            ]

                header =
                    case stopped of
                        Nothing ->
                            [ h1_
                                [ span [ class "text-success" ] [ spinner False ]
                                , text <| " Run " ++ String.fromInt externalId
                                ]
                            , p [ class "lead text-success" ] [ strongText "Running", text <| " for " ++ posixDiffHumanFriendly now (millisToPosix started) ]
                            ]

                        Just realStoppedTime ->
                            [ h1_ [ icon { name = "stop-circle" }, text <| " Run " ++ String.fromInt externalId ]
                            , p [ class "lead" ] [ strongText "Stopped", text <| " " ++ posixDiffHumanFriendlyLongDurationsExact zone (millisToPosix realStoppedTime) now ]
                            , p_ [ text <| "Duration " ++ posixDiffHumanFriendly (millisToPosix started) (millisToPosix realStoppedTime) ]
                            ]

                viewExperimentTypeOption : JsonExperimentType -> Html msg
                viewExperimentTypeOption experimentType =
                    option [ selected (Just experimentType.id == Maybe.map .id currentExperimentType), value (String.fromInt experimentType.id) ] [ text experimentType.name ]

                dataSetSelection =
                    [ form_
                        [ div [ class "input-group mb-3" ]
                            [ div [ class "form-floating flex-grow-1" ]
                                [ select
                                    [ class "form-select"
                                    , Html.Attributes.id "current-experiment-type"
                                    , onInput SelectedExperimentTypeChanged
                                    ]
                                    (option [ selected (isNothing selectedExperimentType), value "" ] [ text "«no value»" ] :: List.map viewExperimentTypeOption rrc.experimentTypes)
                                , label [ for "current-experiment-type" ] [ text "Experiment Type" ]
                                ]
                            , button
                                [ class "btn btn-primary"
                                , type_ "button"
                                , disabled (currentExperimentType == selectedExperimentType || isLoading changeExperimentTypeRequest)
                                , onClick ChangeCurrentExperimentType
                                ]
                                [ icon { name = "save" }, text " Change" ]
                            ]
                        ]
                    ]
            in
            header ++ autoPilot ++ onlineCrystFEL ++ dataSetSelection ++ dataSetInformation zone run dataSetFromRunRequest currentExperimentType rrc


viewRunAttributiForm :
    Maybe (List JsonAttributiIdAndRole)
    -> Maybe JsonRun
    -> List String
    -> RemoteData Http.Error JsonUpdateRunOutput
    -> List (Chemical Int a b)
    -> Maybe RunEditInfo
    -> List JsonExperimentType
    -> List (Html Msg)
viewRunAttributiForm currentExperimentTypeAttributi latestRun submitErrorsList runEditRequest chemicals rei experimentTypes =
    case rei of
        Nothing ->
            []

        Just { runId, experimentTypeId, editableAttributi, showAllAttributi } ->
            let
                matchesCurrentExperiment a x =
                    case x of
                        Nothing ->
                            True

                        Just attributi ->
                            listContainsBy (\otherAttributo -> otherAttributo.id == a.id) attributi

                -- For ergonomic reasons, we want chemical attributi to be on top - everything else should be
                -- sorted alphabetically
                attributoSortKey a =
                    ( if isEditValueChemicalId a.type_.editValue then
                        0

                      else
                        1
                    , a.name
                    )

                attributoFilterFunction a =
                    a.associatedTable
                        == AssociatedTable.Run
                        && (showAllAttributi || a.group == manualGlobalAttributiGroup || a.group == manualAttributiGroup && matchesCurrentExperiment a currentExperimentTypeAttributi)
                        && not (List.member a.name [ "started", "stopped" ])

                filteredAttributi : List EditableAttributo
                filteredAttributi =
                    List.sortBy attributoSortKey <| List.filter attributoFilterFunction editableAttributi.editableAttributi

                viewAttributoFormWithRole : EditableAttributo -> Html AttributoFormMsg
                viewAttributoFormWithRole e =
                    viewAttributoForm chemicals
                        (Maybe.withDefault ChemicalTypeSolution <|
                            Maybe.map .role <|
                                ListExtra.find (\awr -> awr.id == e.id) <|
                                    Maybe.withDefault [] currentExperimentTypeAttributi
                        )
                        e

                submitErrors =
                    case submitErrorsList of
                        [] ->
                            [ text "" ]

                        errors ->
                            [ p_ [ strongText "There were submission errors:" ]
                            , ul [ class "text-danger" ] <| List.map (\e -> li_ [ text e ]) errors
                            ]

                submitSuccess =
                    if isSuccess runEditRequest then
                        [ p [ class "text-success" ] [ text "Saved!" ] ]

                    else
                        []

                buttons =
                    [ button
                        [ class "btn btn-secondary me-2"
                        , disabled (isLoading runEditRequest)
                        , type_ "button"
                        , onClick RunEditSubmit
                        ]
                        [ icon { name = "save" }, text " Save changes" ]
                    , button
                        [ class "btn btn-outline-secondary"
                        , type_ "button"
                        , onClick RunEditCancel
                        ]
                        [ icon { name = "x-lg" }, text " Cancel" ]
                    ]

                isLatestRun =
                    Just runId == Maybe.map (RunInternalId << .id) latestRun

                attributoFormMsgToMsg : AttributoFormMsg -> Msg
                attributoFormMsgToMsg x =
                    case x of
                        AttributoFormValueUpdate vu ->
                            RunEditInfoValueUpdate vu

                        AttributoFormSubmit ->
                            RunEditSubmit

                viewExperimentTypeOption : JsonExperimentType -> Html msg
                viewExperimentTypeOption experimentType =
                    option
                        [ selected (experimentType.id == experimentTypeId)
                        , value (String.fromInt experimentType.id)
                        ]
                        [ text experimentType.name ]
            in
            [ h2_
                [ text <|
                    if isLatestRun then
                        "Edit run"

                    else
                        "Edit run " ++ runInternalIdToString runId
                ]
            , if not isLatestRun then
                p [ class "text-warning" ] [ text "You are currently editing an older run!" ]

              else
                text ""
            , form [ class "mb-3" ] <|
                div [ class "form-check form-switch mb-3" ]
                    [ input_ [ type_ "checkbox", Html.Attributes.id "show-all-attributi", class "form-check-input", checked showAllAttributi, onInput (always (ChangeShowAllAttributi (not showAllAttributi))) ]
                    , label [ class "form-check-label", for "show-all-attributi" ] [ text "Show all attributi" ]
                    ]
                    :: div [ class "form-floating" ]
                        [ select
                            [ class "form-select"
                            , Html.Attributes.id "current-experiment-type-for-specific-run"
                            , onIntInput RunEditInfoExperimentTypeIdChanged
                            ]
                            (List.map viewExperimentTypeOption experimentTypes)
                        , label [ for "current-experiment-type" ] [ text "Experiment Type" ]
                        ]
                    :: (List.map (Html.map attributoFormMsgToMsg << viewAttributoFormWithRole) filteredAttributi ++ submitErrors ++ submitSuccess ++ buttons)
            ]


viewInner : Model -> JsonReadRuns -> List (Html Msg)
viewInner model rrc =
    List.concat
        [ [ div [ class "container" ]
                [ div
                    [ class "row" ]
                    [ div [ class "col-lg-6" ]
                        (viewCurrentRun
                            model.myTimeZone
                            model.now
                            model.selectedExperimentType
                            model.currentExperimentType
                            model.changeExperimentTypeRequest
                            model.dataSetFromRunRequest
                            rrc
                        )
                    , div [ class "col-lg-6" ]
                        (viewRunAttributiForm
                            (model.currentExperimentType
                                |> Maybe.andThen (\a -> ListExtra.find (\et -> et.id == a.id) rrc.experimentTypes)
                                |> Maybe.map .attributi
                            )
                            (head rrc.runs)
                            model.submitErrors
                            model.runEditRequest
                            (List.map convertChemicalFromApi rrc.chemicals)
                            model.runEditInfo
                            rrc.experimentTypes
                        )
                    ]
                ]
          , hr_
          , Html.map EventFormMsg (EventForm.view model.eventForm)
          , hr_
          , case rrc.liveStreamFileId of
                Nothing ->
                    Html.map ColumnChooserMessage (ColumnChooser.view model.columnChooser)

                Just jetStreamId ->
                    div [ class "row" ]
                        [ div [ class "col-lg-6" ] [ Html.map ColumnChooserMessage (ColumnChooser.view model.columnChooser) ]
                        , div [ class "col-lg-6 text-center" ]
                            [ figure [ class "figure" ]
                                [ a [ href (makeFilesLink jetStreamId) ] [ img_ [ src (makeFilesLink jetStreamId ++ "?timestamp=" ++ String.fromInt (posixToMillis model.now)), style "width" "35em" ] ]
                                , figcaption [ class "figure-caption" ]
                                    [ text "Live stream image"
                                    ]
                                ]
                            ]
                        ]
          , hr_
          , Html.map RunFilterSubMsg <| viewRunFilter model.runFilter
          ]
        , runDatesGroup model
        , [ hr_
          , div [ class "row" ]
                [ p_ [ span [ class "text-info" ] [ text "Colored columns" ], text " belong to manually entered attributi." ]
                , viewRunsTable model.myTimeZone (ColumnChooser.resolveChosen model.columnChooser) rrc
                ]
          ]
        ]


runDatesGroup : Model -> List (Html Msg)
runDatesGroup model =
    if List.isEmpty model.runDates then
        []

    else
        [ div [ class "btn-group" ] (dateFilterButtons model) ]


runDateFilterIsNothing : RunEventDateFilter -> Bool
runDateFilterIsNothing rdf =
    case runEventDateFilter rdf of
        Nothing ->
            True

        Maybe.Just _ ->
            False


dateEqualsDateInFilter : RunEventDate -> RunEventDateFilter -> Bool
dateEqualsDateInFilter rd rdf =
    case runEventDateFilter rdf of
        Nothing ->
            False

        Maybe.Just a ->
            a == rd


dateFilterButtons : Model -> List (Html Msg)
dateFilterButtons model =
    List.concat
        [ [ input_
                [ type_ "radio"
                , class "btn-check"
                , id "all_dates"
                , checked
                    (runDateFilterIsNothing model.runDateFilter.runDateFilter)
                , onClick ResetDate
                ]
          , label [ class "btn btn-outline-primary", for "all_dates" ] [ text "All dates" ]
          ]
        , List.concatMap (dateRadioOption model) model.runDates
        ]


dateRadioOption : Model -> RunEventDate -> List (Html Msg)
dateRadioOption model runEventDate =
    [ input_ [ type_ "radio", class "btn-check", id ("filter" ++ runEventDateToString runEventDate), checked (dateEqualsDateInFilter runEventDate model.runDateFilter.runDateFilter), onClick (SetRunDateFilter runEventDate) ]
    , label [ class "btn btn-outline-primary", for ("filter" ++ runEventDateToString runEventDate) ] [ text (formattedOrEmptyDate runEventDate) ]
    ]


formattedOrEmptyDate : RunEventDate -> String
formattedOrEmptyDate runEventDate =
    let
        date =
            runEventDateToString runEventDate
    in
    case Date.fromIsoString date of
        Ok v ->
            Date.format "EE, d MMM y" v

        Err _ ->
            "Unreadable date <" ++ date ++ ">"


view : Model -> Html Msg
view model =
    div [ class "container" ] <|
        case model.runs of
            NotAsked ->
                List.singleton <| text "Impossible state reached: time zone, but no runs in progress?"

            Loading ->
                List.singleton <| loadingBar "Loading runs..."

            Failure e ->
                List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve runs" ], showHttpError e ]

            Success a ->
                case model.refreshRequest of
                    Loading ->
                        viewInner model a ++ [ loadingBar "Refreshing..." ]

                    _ ->
                        viewInner model a


updateRunEditInfoFromContent : Zone -> Maybe RunEditInfo -> JsonReadRuns -> Maybe RunEditInfo
updateRunEditInfoFromContent zone runEditInfoRaw { runs, attributi } =
    case runEditInfoRaw of
        -- We have no previous edit info
        Nothing ->
            head runs
                |> Maybe.map
                    (\latestRun ->
                        { runId = RunInternalId latestRun.id
                        , runExternalId = RunExternalId latestRun.externalId
                        , started = millisToPosix latestRun.started
                        , stopped = Maybe.map millisToPosix latestRun.stopped
                        , experimentTypeId = latestRun.experimentTypeId
                        , editableAttributi = createEditableAttributi zone (List.map convertAttributoFromApi attributi) (convertAttributoMapFromApi latestRun.attributi)
                        , initiatedManually = False
                        , showAllAttributi = False
                        }
                    )

        -- We have previous edit info
        Just runEditInfo ->
            -- We have unsaved changes to the previous run
            -- OR we have a manually edited run
            if runEditInfo.initiatedManually || unsavedAttributoChanges runEditInfo.editableAttributi.editableAttributi then
                Just runEditInfo

            else
                -- We have no unsaved changes and a run
                head runs
                    |> Maybe.map
                        (\latestRun ->
                            { runId = RunInternalId latestRun.id
                            , runExternalId = RunExternalId latestRun.externalId
                            , started = millisToPosix latestRun.started
                            , stopped = Maybe.map millisToPosix latestRun.stopped
                            , experimentTypeId = latestRun.experimentTypeId
                            , editableAttributi = createEditableAttributi zone (List.map convertAttributoFromApi attributi) (convertAttributoMapFromApi latestRun.attributi)
                            , initiatedManually = False
                            , showAllAttributi = False
                            }
                        )


updateRunEditInfo : Zone -> Maybe RunEditInfo -> Result Http.Error JsonReadRuns -> Maybe RunEditInfo
updateRunEditInfo zone runEditInfoRaw responseRaw =
    case responseRaw of
        Ok content ->
            updateRunEditInfoFromContent zone runEditInfoRaw content

        Err _ ->
            runEditInfoRaw


virtualExperimentTypeAttributoName : String
virtualExperimentTypeAttributoName =
    "experiment_type"


updateColumnChooser : Maybe LocalStorage -> ColumnChooser.Model -> RemoteData Http.Error JsonReadRuns -> Result Http.Error JsonReadRuns -> ColumnChooser.Model
updateColumnChooser localStorage ccm currentRuns runsResponse =
    case ( currentRuns, runsResponse ) of
        ( _, Ok { attributi } ) ->
            -- Inject "virtual" attributo experiment type here
            let
                experimentTypeAttributo =
                    { id = -1
                    , name = virtualExperimentTypeAttributoName
                    , description = "Experiment Type"
                    , group = "manual"
                    , associatedTable = AssociatedTable.Run
                    , type_ = String
                    }
            in
            ColumnChooser.updateAttributi ccm (List.map convertAttributoFromApi attributi ++ [ experimentTypeAttributo ])

        ( _, Err _ ) ->
            ColumnChooser.init localStorage []


extractRunDates : Result Http.Error JsonReadRuns -> List RunEventDate
extractRunDates runDates =
    case runDates of
        Err _ ->
            []

        Ok { filterDates } ->
            List.map RunEventDate filterDates


updateRunDateFilter : RunDateFilterInfo -> RunEventDate -> RunDateFilterInfo
updateRunDateFilter runDateFilterInfo runDate =
    { runDateFilterInfo | runDateFilter = specificRunEventDateFilter runDate }


withRunsResponse : Model -> (JsonReadRuns -> ( Model, Cmd Msg )) -> ( Model, Cmd Msg )
withRunsResponse model f =
    case model.runs of
        Success rrc ->
            f rrc

        _ ->
            ( model, Cmd.none )


retrieveRuns : Model -> Cmd Msg
retrieveRuns model =
    send RunsReceived
        (readRunsApiRunsBeamtimeIdGet model.beamtimeId
            (Maybe.map runEventDateToString <| runEventDateFilter <| model.runDateFilter.runDateFilter)
            (Just <| runFilterToString model.runFilter.runFilter)
        )


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        ResetDate ->
            let
                newModel =
                    { model
                        | runDateFilter = initRunDateFilter
                    }
            in
            ( newModel
            , retrieveRuns newModel
            )

        SetRunDateFilter runDate ->
            let
                newRunDateFilter =
                    updateRunDateFilter model.runDateFilter runDate

                newModel =
                    { model | runDateFilter = newRunDateFilter }
            in
            ( newModel
            , retrieveRuns newModel
            )

        RunsReceived response ->
            let
                hasLiveStream =
                    case response of
                        Err _ ->
                            False

                        Ok { liveStreamFileId } ->
                            MaybeExtra.isJust liveStreamFileId

                newEventForm =
                    EventForm.updateLiveStream model.eventForm hasLiveStream

                newCurrentExperimentType : Maybe JsonExperimentType
                newCurrentExperimentType =
                    case response of
                        Ok { experimentTypes, userConfig } ->
                            userConfig.currentExperimentTypeId |> Maybe.andThen (\cet -> ListExtra.find (\et -> et.id == cet) experimentTypes)

                        _ ->
                            model.currentExperimentType
            in
            ( { model
                | runs = fromResult response
                , runDates = extractRunDates response
                , runEditInfo = updateRunEditInfo model.myTimeZone model.runEditInfo response
                , columnChooser = updateColumnChooser model.localStorage model.columnChooser model.runs response
                , eventForm = newEventForm
                , refreshRequest =
                    if isSuccess model.runs then
                        Success ()

                    else
                        model.refreshRequest

                -- These two are a bit tricky. First, an intro:
                --
                -- + "currentExperimentType" is the currently activated experiment type. Only attributi from that type
                --   are being displayed to be edited.
                -- + "selectedExperimentType" is the experiment type selected in the dropdown. This can be changed
                --   in your browser. As long as you don't press the "Change" button, nothing happens for other users.
                --
                -- We assume multiple people look at the same UI, changing things from time to time. This means the
                -- current experiment type (which is shared by all users inside the user configuration  SQL table) can
                -- change.
                --
                -- What we do with these two statements is:
                --
                -- 1. Make sure the shared current experiment type (from the SQL table) is the one being activated in
                --    the UI.
                -- 2. Keep the selected experiment type only while the current experiment type stays unchanged.
                --
                -- For example, let's say the user Alice opens the dropdown, changes the experiment type from
                -- "A" to "B", but doesn't press the "Change" button. User Bob does the same, changing "A" to "X", but
                -- Bob actually presses change. Alice will then have her selection overridden and will see "X" activated
                -- and in the dropdown.
                , currentExperimentType = newCurrentExperimentType
                , selectedExperimentType =
                    if newCurrentExperimentType == model.currentExperimentType then
                        model.selectedExperimentType

                    else
                        newCurrentExperimentType
              }
            , Cmd.none
            )

        Refresh now ->
            case ( model.refreshRequest, model.runs ) of
                ( Loading, _ ) ->
                    ( model, Cmd.none )

                ( _, Loading ) ->
                    ( model, Cmd.none )

                _ ->
                    ( { model | refreshRequest = Loading, now = now }
                    , retrieveRuns model
                    )

        EventFormMsg eventFormMsg ->
            let
                ( newEventForm, cmds ) =
                    EventForm.update eventFormMsg model.eventForm
            in
            ( { model | eventForm = newEventForm }
            , Cmd.batch
                [ case eventFormMsg of
                    EventForm.SubmitFinished _ ->
                        retrieveRuns model

                    _ ->
                        Cmd.none
                , Cmd.map EventFormMsg cmds
                ]
            )

        EventDelete eventId ->
            ( model, send EventDeleteFinished (deleteEventApiEventsDelete { id = eventId }) )

        EventDeleteFinished result ->
            case result of
                Ok _ ->
                    ( model, retrieveRuns model )

                _ ->
                    ( model, Cmd.none )

        RunEditInfoExperimentTypeIdChanged v ->
            case model.runEditInfo of
                -- This is the unlikely case that we have an "attributo was edited" message, but chemical is edited
                Nothing ->
                    ( model, Cmd.none )

                Just oldRunEditInfo ->
                    let
                        newRunEditInfo =
                            { oldRunEditInfo | experimentTypeId = v }
                    in
                    ( { model | runEditInfo = Just newRunEditInfo }, Cmd.none )

        RunEditInfoValueUpdate v ->
            case model.runEditInfo of
                -- This is the unlikely case that we have an "attributo was edited" message, but chemical is edited
                Nothing ->
                    ( model, Cmd.none )

                Just oldRunEditInfo ->
                    let
                        newEditable =
                            editEditableAttributi oldRunEditInfo.editableAttributi.editableAttributi v

                        newRunEditInfo =
                            { oldRunEditInfo | editableAttributi = { editableAttributi = newEditable, originalAttributi = oldRunEditInfo.editableAttributi.originalAttributi } }
                    in
                    ( { model | runEditInfo = Just newRunEditInfo }, Cmd.none )

        ChangeShowAllAttributi newValue ->
            case model.runEditInfo of
                Nothing ->
                    ( model, Cmd.none )

                Just oldRunEditInfo ->
                    let
                        newRunEditInfo =
                            { oldRunEditInfo | showAllAttributi = newValue }
                    in
                    ( { model | runEditInfo = Just newRunEditInfo }, Cmd.none )

        RunEditSubmit ->
            case model.runEditInfo of
                Nothing ->
                    ( model, Cmd.none )

                Just runEditInfo ->
                    case convertEditValues model.myTimeZone runEditInfo.editableAttributi of
                        Err errorList ->
                            ( { model | submitErrors = List.map (\( attributoId, errorMessage ) -> String.fromInt attributoId ++ ": " ++ errorMessage) errorList }, Cmd.none )

                        Ok editedAttributi ->
                            ( { model | runEditRequest = Loading }
                            , send RunEditFinished
                                (updateRunApiRunsPatch
                                    { id = runInternalIdToInt runEditInfo.runId
                                    , experimentTypeId = runEditInfo.experimentTypeId
                                    , attributi = attributoMapToListOfAttributi editedAttributi
                                    }
                                )
                            )

        RunEditFinished result ->
            case result of
                Err e ->
                    ( { model | runEditRequest = Failure e }, Cmd.none )

                Ok editRequestResult ->
                    case model.runs of
                        Success _ ->
                            let
                                resetEditedFlags : RunEditInfo -> RunEditInfo
                                resetEditedFlags rei =
                                    { runId = rei.runId
                                    , runExternalId = rei.runExternalId
                                    , started = rei.started
                                    , stopped = rei.stopped
                                    , experimentTypeId = rei.experimentTypeId
                                    , editableAttributi =
                                        { originalAttributi = rei.editableAttributi.originalAttributi
                                        , editableAttributi = List.map resetEditableAttributo rei.editableAttributi.editableAttributi
                                        }

                                    -- Reset manual edit flag, so we automatically jump to the latest run again
                                    , initiatedManually = False
                                    , showAllAttributi = False
                                    }
                            in
                            ( { model | runEditRequest = Success editRequestResult, submitErrors = [], runEditInfo = Maybe.map resetEditedFlags model.runEditInfo }
                            , retrieveRuns model
                            )

                        _ ->
                            -- Super unlikely case, we don't really have a successful runs request, after finishing editing a run?
                            ( { model | runEditRequest = Success editRequestResult, submitErrors = [], runEditInfo = Nothing }
                            , retrieveRuns model
                            )

        RunInitiateEdit run ->
            case model.runs of
                Success { attributi } ->
                    ( { model
                        | runEditInfo =
                            Just
                                { runId = RunInternalId run.id
                                , runExternalId = RunExternalId run.externalId
                                , started = millisToPosix run.started
                                , stopped = Maybe.map millisToPosix run.stopped
                                , experimentTypeId = run.experimentTypeId
                                , editableAttributi = createEditableAttributi model.myTimeZone (List.map convertAttributoFromApi attributi) (convertAttributoMapFromApi run.attributi)
                                , initiatedManually = True
                                , showAllAttributi = False
                                }
                        , runEditRequest = NotAsked
                      }
                    , scrollToTop (always Nop)
                    )

                _ ->
                    ( model, Cmd.none )

        RunEditCancel ->
            case model.runs of
                Success response ->
                    ( { model | runEditInfo = updateRunEditInfoFromContent model.myTimeZone Nothing response, submitErrors = [], runEditRequest = NotAsked }, Cmd.none )

                _ ->
                    ( { model | runEditInfo = Nothing }, Cmd.none )

        Nop ->
            ( model, Cmd.none )

        SelectedExperimentTypeChanged etIdStr ->
            withRunsResponse model <|
                \response ->
                    ( { model
                        | selectedExperimentType =
                            String.toInt etIdStr
                                |> Maybe.andThen (\etId -> ListExtra.find (\et -> et.id == etId) response.experimentTypes)
                      }
                    , Cmd.none
                    )

        ColumnChooserMessage columnChooserMessage ->
            let
                ( newColumnChooser, cmds ) =
                    ColumnChooser.update model.columnChooser columnChooserMessage
            in
            ( { model | columnChooser = newColumnChooser }, Cmd.map ColumnChooserMessage cmds )

        ChangeAutoPilot newValue ->
            ( model
            , send AutoPilotToggled
                (updateUserConfigurationSingleApiUserConfigBeamtimeIdKeyValuePatch model.beamtimeId
                    "auto-pilot"
                    (if newValue then
                        "True"

                     else
                        "False"
                    )
                )
            )

        ChangeOnlineCrystFEL newValue ->
            ( model
            , send OnlineCrystFELToggled
                (updateUserConfigurationSingleApiUserConfigBeamtimeIdKeyValuePatch model.beamtimeId
                    "online-crystfel"
                    (if newValue then
                        "True"

                     else
                        "False"
                    )
                )
            )

        AutoPilotToggled _ ->
            ( model, retrieveRuns model )

        OnlineCrystFELToggled _ ->
            ( model, retrieveRuns model )

        CreateDataSetFromRun runId ->
            ( { model | dataSetFromRunRequest = Loading }
            , send CreateDataSetFromRunFinished
                (createDataSetFromRunApiDataSetsFromRunPost
                    { runInternalId = runInternalIdToInt runId
                    }
                )
            )

        CreateDataSetFromRunFinished result ->
            ( { model | dataSetFromRunRequest = fromResult result }, retrieveRuns model )

        ChangeCurrentExperimentType ->
            case model.runEditInfo of
                Nothing ->
                    ( model, Cmd.none )

                Just ei ->
                    ( { model | changeExperimentTypeRequest = Loading }
                    , send
                        (ChangeCurrentExperimentTypeFinished (Maybe.map .id model.selectedExperimentType))
                        (changeCurrentRunExperimentTypeApiExperimentTypesChangeForRunPost
                            { experimentTypeId = Maybe.map .id model.selectedExperimentType
                            , runInternalId = runInternalIdToInt ei.runId
                            }
                        )
                    )

        ChangeCurrentExperimentTypeFinished selectedExperimentType result ->
            withRunsResponse model <|
                \response ->
                    ( { model
                        | changeExperimentTypeRequest = fromResult result
                        , currentExperimentType = ListExtra.find (\et -> Just et.id == selectedExperimentType) response.experimentTypes
                      }
                    , retrieveRuns model
                    )

        RunFilterSubMsg runFilterMsg ->
            let
                ( newRunFilter, cmds ) =
                    updateRunFilter model.beamtimeId model.runFilter runFilterMsg

                newModel =
                    { model | runFilter = newRunFilter }

                afterRunsResponse =
                    case runFilterMsg of
                        RunFilterSubmitFinished (Ok response) ->
                            update (RunsReceived (Ok response)) newModel

                        _ ->
                            ( newModel, Cmd.none )
            in
            ( first afterRunsResponse, Cmd.batch [ second afterRunsResponse, Cmd.map RunFilterSubMsg cmds ] )
