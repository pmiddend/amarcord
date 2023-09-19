module Amarcord.Pages.RunOverview exposing (Model, Msg(..), init, update, view)

import Amarcord.API.AttributoWithRole exposing (AttributoWithRole)
import Amarcord.API.DataSet exposing (DataSet, emptySummary)
import Amarcord.API.ExperimentType exposing (ExperimentType, ExperimentTypeId, experimentTypeIdDict)
import Amarcord.API.Requests exposing (Event, RequestError, Run, RunEventDate, RunEventDateFilter, RunFilter(..), RunsResponse, RunsResponseContent, emptyRunEventDateFilter, emptyRunFilter, httpChangeCurrentExperimentTypeForRun, httpCreateDataSetFromRun, httpDeleteEvent, httpGetRunsFilter, httpUpdateRun, httpUserConfigurationSetAutoPilot, httpUserConfigurationSetOnlineCrystFEL, runEventDateFilter, runEventDateToString, runFilterToString, specificRunEventDateFilter)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo exposing (Attributo, AttributoType(..), attributoExposureTime, attributoStarted, attributoStopped, extractDateTime, retrieveAttributoValue, retrieveDateTimeAttributoValue, retrieveFloatAttributoValue)
import Amarcord.AttributoHtml exposing (AttributoFormMsg(..), AttributoNameWithValueUpdate, EditableAttributiAndOriginal, EditableAttributo, convertEditValues, createEditableAttributi, editEditableAttributi, formatFloatHumanFriendly, formatIntHumanFriendly, isEditValueChemicalId, makeAttributoHeader, resetEditableAttributo, unsavedAttributoChanges, viewAttributoCell, viewAttributoForm, viewRunExperimentTypeCell)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert, mimeTypeToIcon, spinner, viewRemoteData)
import Amarcord.Chemical exposing (Chemical, ChemicalType(..), chemicalIdDict)
import Amarcord.ColumnChooser as ColumnChooser
import Amarcord.Constants exposing (manualAttributiGroup, manualGlobalAttributiGroup)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.EventForm as EventForm
import Amarcord.Gauge exposing (gauge, thisFillColor, totalFillColor)
import Amarcord.Html exposing (div_, form_, h1_, h2_, h3_, h5_, hr_, img_, input_, li_, onIntInput, p_, strongText, tbody_, td_, th_, thead_)
import Amarcord.LocalStorage exposing (LocalStorage)
import Amarcord.MarkdownUtil exposing (markupWithoutErrors)
import Amarcord.Route exposing (makeFilesLink)
import Amarcord.Util exposing (HereAndNow, formatPosixTimeOfDayHumanFriendly, listContainsBy, posixBefore, posixDiffHumanFriendly, scrollToTop, secondsDiffHumanFriendly)
import Char exposing (fromCode)
import Date
import Dict exposing (Dict)
import Html exposing (Html, a, button, div, em, figcaption, figure, form, h4, label, option, p, select, span, table, td, text, tr, ul)
import Html.Attributes exposing (checked, class, colspan, disabled, for, href, id, selected, src, style, type_, value)
import Html.Events exposing (onClick, onInput)
import Html.Events.Extra exposing (onEnter)
import List exposing (head)
import List.Extra as ListExtra exposing (find)
import Maybe
import Maybe.Extra as MaybeExtra exposing (isNothing)
import RemoteData exposing (RemoteData(..), fromResult, isLoading, isSuccess)
import String exposing (fromInt)
import Time exposing (Posix, Zone, posixToMillis)
import Tuple exposing (first, second)


type Msg
    = RunsReceived RunsResponse
    | Refresh Posix
    | EventDelete Int
    | EventDeleteFinished (Result RequestError ())
    | EventFormMsg EventForm.Msg
    | RunEditInfoValueUpdate AttributoNameWithValueUpdate
    | RunEditInfoExperimentTypeIdChanged Int
    | RunEditSubmit
    | RunEditFinished (Result RequestError ())
    | RunInitiateEdit Run
    | RunEditCancel
    | RunFilterSubMsg RunFilterMsg
    | Nop
    | SelectedExperimentTypeChanged String
    | ChangeCurrentExperimentType
    | ColumnChooserMessage ColumnChooser.Msg
    | ChangeAutoPilot Bool
    | ChangeOnlineCrystFEL Bool
    | AutoPilotToggled (Result RequestError Bool)
    | OnlineCrystFELToggled (Result RequestError Bool)
    | CreateDataSetFromRun ExperimentTypeId Int
    | CreateDataSetFromRunFinished (Result RequestError ())
    | ChangeCurrentExperimentTypeFinished (Maybe ExperimentTypeId) (Result RequestError ())
    | ResetDate
    | SetRunDateFilter RunEventDate


type alias RunEditInfo =
    { runId : Int
    , experimentTypeId : Int
    , editableAttributi : EditableAttributiAndOriginal

    -- This is to handle a tricky case: usually we want to stay with the latest run so we can quickly change settings.
    -- If we manually click on an older run to edit it, we don't want to then jump to the latest one.
    , initiatedManually : Bool
    }


type alias RunFilterInfo =
    { runFilter : RunFilter
    , nextRunFilter : String
    , runFilterRequest : RemoteData RequestError ()
    }


type alias RunDateFilterInfo =
    { runDateFilter : RunEventDateFilter }


type RunFilterMsg
    = RunFilterEdit String
    | RunFilterReset
    | RunFilterSubmit
    | RunFilterSubmitFinished RunsResponse


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
                div [ class "mb-3" ] [ div_ [ makeAlert [ AlertDanger ] [ showRequestError e ] ] ]

            _ ->
                text ""
        ]


updateRunFilter : RunFilterInfo -> RunFilterMsg -> ( RunFilterInfo, Cmd RunFilterMsg )
updateRunFilter model msg =
    case msg of
        RunFilterEdit newRunFilter ->
            ( { model | nextRunFilter = newRunFilter }, Cmd.none )

        RunFilterReset ->
            ( { model | nextRunFilter = runFilterToString model.runFilter, runFilterRequest = NotAsked }, Cmd.none )

        RunFilterSubmit ->
            ( { model | runFilterRequest = Loading }, httpGetRunsFilter (RunFilter model.nextRunFilter) emptyRunEventDateFilter RunFilterSubmitFinished )

        RunFilterSubmitFinished response ->
            case response of
                Err e ->
                    ( { model | runFilterRequest = Failure e }, Cmd.none )

                Ok _ ->
                    ( { model | runFilter = RunFilter model.nextRunFilter, runFilterRequest = Success () }, Cmd.none )


type alias Model =
    { runs : RemoteData RequestError RunsResponseContent
    , myTimeZone : Zone
    , refreshRequest : RemoteData RequestError ()
    , eventForm : EventForm.Model
    , now : Posix
    , runDateFilter : RunDateFilterInfo
    , runDates : List RunEventDate
    , runEditInfo : Maybe RunEditInfo
    , runEditRequest : RemoteData RequestError ()
    , runFilter : RunFilterInfo
    , submitErrors : List String
    , currentExperimentType : Maybe ExperimentType
    , selectedExperimentType : Maybe ExperimentType
    , columnChooser : ColumnChooser.Model
    , localStorage : Maybe LocalStorage
    , dataSetFromRunRequest : RemoteData RequestError ()
    , changeExperimentTypeRequest : RemoteData RequestError ()
    }


init : HereAndNow -> Maybe LocalStorage -> ( Model, Cmd Msg )
init { zone, now } localStorage =
    ( { runs = Loading
      , myTimeZone = zone
      , refreshRequest = NotAsked
      , eventForm = EventForm.init
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
      }
    , httpGetRunsFilter emptyRunFilter emptyRunEventDateFilter RunsReceived
    )


attributiColumnHeaders : List (Attributo AttributoType) -> List (Html msg)
attributiColumnHeaders =
    List.map (th_ << makeAttributoHeader)


attributiColumns : Zone -> Dict Int String -> Dict Int String -> List (Attributo AttributoType) -> Run -> List (Html Msg)
attributiColumns zone chemicalIds experimentTypeIds attributi run =
    let
        viewCell a =
            if a.associatedTable == AssociatedTable.Run then
                Just <|
                    if a.name == virtualExperimentTypeAttributoName then
                        viewRunExperimentTypeCell (Maybe.withDefault "unknown experiment type" <| Dict.get run.experimentTypeId experimentTypeIds)

                    else
                        viewAttributoCell { shortDateTime = True, colorize = True } zone chemicalIds run.attributi a

            else
                Nothing
    in
    List.filterMap viewCell attributi


viewRunRow : Zone -> Dict Int String -> Dict Int String -> List (Attributo AttributoType) -> Run -> Html Msg
viewRunRow zone chemicalIds experimentTypeIds attributi r =
    tr [ style "white-space" "nowrap" ] <|
        td_ [ text (fromInt r.id) ]
            :: attributiColumns zone chemicalIds experimentTypeIds attributi r
            ++ [ td_
                    [ button
                        [ class "btn btn-link amarcord-small-link-button"
                        , onClick (RunInitiateEdit r)
                        ]
                        [ icon { name = "pencil-square" } ]
                    ]
               ]


viewEventRow : Zone -> Int -> Event -> Html Msg
viewEventRow zone attributoColumnCount e =
    let
        viewFile { type_, fileName, id } =
            li_
                [ mimeTypeToIcon type_
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
        , td_ [ text <| formatPosixTimeOfDayHumanFriendly zone e.created ]
        , td [ colspan attributoColumnCount ] mainContent
        ]


viewRunAndEventRows : Zone -> Dict Int String -> Dict Int String -> List (Attributo AttributoType) -> List Run -> List Event -> List (Html Msg)
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
            case Maybe.andThen extractDateTime <| retrieveAttributoValue attributoStarted run.attributi of
                Just runStarted ->
                    if posixBefore event.created runStarted then
                        viewRunRow zone chemicalIds experimentTypeIds attributi run :: viewRunAndEventRows zone chemicalIds experimentTypeIds attributi (List.drop 1 runs) events

                    else
                        viewEventRow zone (List.length attributi) event :: viewRunAndEventRows zone chemicalIds experimentTypeIds attributi runs (List.drop 1 events)

                -- We don't have a start time...take the run
                Nothing ->
                    viewRunRow zone chemicalIds experimentTypeIds attributi run :: viewRunAndEventRows zone chemicalIds experimentTypeIds attributi (List.drop 1 runs) events


viewRunsTable : Zone -> List (Attributo AttributoType) -> RunsResponseContent -> Html Msg
viewRunsTable zone chosenColumns { runs, events, chemicals, experimentTypes } =
    let
        runRows : List (Html Msg)
        runRows =
            viewRunAndEventRows zone (chemicalIdDict chemicals) (experimentTypeIdDict experimentTypes) chosenColumns runs events
    in
    table [ class "table amarcord-table-fix-head table-bordered table-hover" ]
        [ thead_
            [ tr [ class "align-top" ] <|
                th_ [ text "ID" ]
                    :: attributiColumnHeaders chosenColumns
                    ++ [ th_ [ text "Actions" ] ]
            ]
        , tbody_ runRows
        ]


viewCurrentRun : Zone -> Posix -> Maybe ExperimentType -> Maybe ExperimentType -> RemoteData RequestError () -> RemoteData RequestError () -> RunsResponseContent -> List (Html Msg)
viewCurrentRun zone now selectedExperimentType currentExperimentType changeExperimentTypeRequest dataSetFromRunRequest rrc =
    -- Here, we assume runs are ordered so the first one is the latest one.
    case head rrc.runs of
        Nothing ->
            List.singleton <| text ""

        Just { id, attributi, summary, dataSets } ->
            let
                runStarted : Maybe Posix
                runStarted =
                    retrieveDateTimeAttributoValue attributoStarted attributi

                runStopped : Maybe Posix
                runStopped =
                    retrieveDateTimeAttributoValue attributoStopped attributi

                autoPilot =
                    [ div [ class "form-check form-switch mb-3" ]
                        [ input_ [ type_ "checkbox", Html.Attributes.id "auto-pilot", class "form-check-input", checked rrc.userConfig.autoPilot, onInput (always (ChangeAutoPilot (not rrc.userConfig.autoPilot))) ]
                        , label [ class "form-check-label", for "auto-pilot" ] [ text "Auto pilot" ]
                        , div [ class "form-text" ] [ text "Manual attributi will be copied over from the previous run. Be careful not to change experimental conditions if this is active." ]
                        ]
                    ]

                onlineCrystFEL =
                    [ div [ class "form-check form-switch mb-3" ]
                        [ input_ [ type_ "checkbox", Html.Attributes.id "crystfel-online", class "form-check-input", checked rrc.userConfig.onlineCrystFEL, onInput (always (ChangeOnlineCrystFEL (not rrc.userConfig.onlineCrystFEL))) ]
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
                                case runStopped of
                                    Just _ ->
                                        Just id

                                    Nothing ->
                                        Nothing

                            else if r.id < id then
                                Just r.id

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
                    case ( runStarted, runStopped ) of
                        ( Just started, Nothing ) ->
                            [ h1_
                                [ span [ class "text-success" ] [ spinner False ]
                                , text <| " Run " ++ fromInt id
                                ]
                            , p [ class "lead text-success" ] [ strongText "Running", text <| " for " ++ posixDiffHumanFriendly now started ]
                            ]

                        ( Just started, Just stopped ) ->
                            [ h1_ [ icon { name = "stop-circle" }, text <| " Run " ++ fromInt id ]
                            , p [ class "lead" ] [ strongText "Stopped", text <| " " ++ posixDiffHumanFriendly stopped now ++ " ago " ]
                            , p_ [ text <| "Duration " ++ posixDiffHumanFriendly started stopped ]
                            ]

                        _ ->
                            []

                viewExperimentTypeOption : ExperimentType -> Html msg
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

                currentRunDataSet : Maybe DataSet
                currentRunDataSet =
                    case currentExperimentType of
                        Nothing ->
                            Nothing

                        Just experimentType ->
                            find (\ds -> ds.experimentTypeId == experimentType.id && List.member ds.id dataSets) rrc.dataSets

                smallBox color =
                    span [ style "color" color ] [ text <| String.fromChar <| fromCode 9632 ]

                dataSetInformation =
                    case currentRunDataSet of
                        Nothing ->
                            case currentExperimentType of
                                Nothing ->
                                    [ p [ class "text-muted" ] [ text "No experiment type selected, cannot display data set information." ] ]

                                Just experimentType ->
                                    [ p [ class "text-muted" ] [ text <| "Run is not part of any data set in \"" ++ experimentType.name ++ "\". You can automatically create a data set that matches the current run's attributi." ]
                                    , button [ type_ "button", class "btn btn-secondary", onClick (CreateDataSetFromRun experimentType.id id), disabled (isLoading dataSetFromRunRequest) ]
                                        [ icon { name = "plus-lg" }, text " Create data set from run" ]
                                    , viewRemoteData "Data set created" dataSetFromRunRequest
                                    ]

                        Just ds ->
                            let
                                indexingProgress =
                                    let
                                        progressSummary =
                                            case Maybe.andThen .summary currentRunDataSet of
                                                Nothing ->
                                                    summary

                                                Just dsSummary ->
                                                    dsSummary

                                        etaFor10kFrames =
                                            case summary.indexingRate of
                                                Nothing ->
                                                    Nothing

                                                Just ir ->
                                                    case summary.hitRate of
                                                        Nothing ->
                                                            Nothing

                                                        Just hr ->
                                                            if ir > 0.01 && hr > 0.01 then
                                                                let
                                                                    runExposureTime : Maybe Float
                                                                    runExposureTime =
                                                                        retrieveFloatAttributoValue attributoExposureTime attributi
                                                                in
                                                                case runExposureTime of
                                                                    Nothing ->
                                                                        Nothing

                                                                    Just realExposureTime ->
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

                                                            else
                                                                Nothing
                                    in
                                    [ div [ class "d-flex justify-content-center" ]
                                        [ div [ class "progress", style "width" "80%" ]
                                            [ div
                                                [ class
                                                    ("progress-bar"
                                                        ++ (if MaybeExtra.isJust runStopped then
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

                                twoValueGauge title this total =
                                    case total of
                                        Nothing ->
                                            case this of
                                                Nothing ->
                                                    []

                                                Just thisValue ->
                                                    [ gauge this total, div_ [ em [ class "amarcord-small-text" ] [ smallBox thisFillColor, text <| " " ++ title ++ " " ++ formatFloatHumanFriendly thisValue ++ "%" ] ] ]

                                        Just totalValue ->
                                            case this of
                                                Nothing ->
                                                    [ gauge this total
                                                    , div_ [ em [ class "amarcord-small-text" ] [ smallBox totalFillColor, text <| " data set: " ++ formatFloatHumanFriendly totalValue ++ "%" ] ]
                                                    ]

                                                Just thisValue ->
                                                    [ gauge this total
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
                                [ div [ class "text-center" ] (twoValueGauge "Indexing rate" summary.indexingRate Nothing)
                                , div [ class "text-center" ] (twoValueGauge "Hit rate" summary.hitRate Nothing)
                                ]
                            , div [ class "mb-3" ] indexingProgress
                            , h3_ [ text "Data set" ]
                            , viewDataSetTable rrc.attributi
                                zone
                                (chemicalIdDict rrc.chemicals)
                                ds.attributi
                                True
                                Nothing
                            ]
            in
            header ++ autoPilot ++ onlineCrystFEL ++ dataSetSelection ++ dataSetInformation


viewRunAttributiForm :
    Maybe (List AttributoWithRole)
    -> Maybe Run
    -> List String
    -> RemoteData RequestError ()
    -> List (Chemical Int a b)
    -> Maybe RunEditInfo
    -> List ExperimentType
    -> List (Html Msg)
viewRunAttributiForm currentExperimentTypeAttributi latestRun submitErrorsList runEditRequest chemicals rei experimentTypes =
    case rei of
        Nothing ->
            []

        Just { runId, experimentTypeId, editableAttributi } ->
            let
                matchesCurrentExperiment a x =
                    case x of
                        Nothing ->
                            True

                        Just attributi ->
                            listContainsBy (\otherAttributo -> otherAttributo.name == a.name) attributi

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
                        && (a.group == manualGlobalAttributiGroup || a.group == manualAttributiGroup && matchesCurrentExperiment a currentExperimentTypeAttributi)
                        && not (List.member a.name [ "started", "stopped" ])

                filteredAttributi : List EditableAttributo
                filteredAttributi =
                    List.sortBy attributoSortKey <| List.filter attributoFilterFunction editableAttributi.editableAttributi

                viewAttributoFormWithRole : EditableAttributo -> Html AttributoFormMsg
                viewAttributoFormWithRole e =
                    viewAttributoForm chemicals
                        (Maybe.withDefault Solution <|
                            Maybe.map .role <|
                                ListExtra.find (\awr -> awr.name == e.name) <|
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
                    Just runId == Maybe.map .id latestRun

                attributoFormMsgToMsg : AttributoFormMsg -> Msg
                attributoFormMsgToMsg x =
                    case x of
                        AttributoFormValueUpdate vu ->
                            RunEditInfoValueUpdate vu

                        AttributoFormSubmit ->
                            RunEditSubmit

                viewExperimentTypeOption : ExperimentType -> Html msg
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
                        "Edit run " ++ fromInt runId
                ]
            , if not isLatestRun then
                p [ class "text-warning" ] [ text "You are currently editing an older run!" ]

              else
                text ""
            , form [ class "mb-3" ] <|
                div [ class "form-floating" ]
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


viewInner : Model -> RunsResponseContent -> List (Html Msg)
viewInner model rrc =
    List.concat
        [ [ div [ class "container" ]
                [ div
                    [ class "row" ]
                    [ div [ class "col-lg-6" ]
                        (viewCurrentRun model.myTimeZone model.now model.selectedExperimentType model.currentExperimentType model.changeExperimentTypeRequest model.dataSetFromRunRequest rrc)
                    , div [ class "col-lg-6" ]
                        (viewRunAttributiForm
                            (model.currentExperimentType
                                |> Maybe.andThen (\a -> ListExtra.find (\et -> et.id == a.id) rrc.experimentTypes)
                                |> Maybe.map .attributi
                            )
                            (head rrc.runs)
                            model.submitErrors
                            model.runEditRequest
                            rrc.chemicals
                            model.runEditInfo
                            rrc.experimentTypes
                        )
                    ]
                ]
          , hr_
          , Html.map EventFormMsg (EventForm.view model.eventForm)
          , hr_
          , case rrc.jetStreamFileId of
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
                List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve runs" ], showRequestError e ]

            Success a ->
                case model.refreshRequest of
                    Loading ->
                        viewInner model a ++ [ loadingBar "Refreshing..." ]

                    _ ->
                        viewInner model a


updateRunEditInfoFromContent : Zone -> Maybe RunEditInfo -> RunsResponseContent -> Maybe RunEditInfo
updateRunEditInfoFromContent zone runEditInfoRaw { runs, attributi } =
    case runEditInfoRaw of
        -- We have no previous edit info
        Nothing ->
            case head runs of
                -- We have no edit info, and no runs
                Nothing ->
                    Nothing

                -- We have no edit info, and now we have a run
                Just latestRun ->
                    Just
                        { runId = latestRun.id
                        , experimentTypeId = latestRun.experimentTypeId
                        , editableAttributi = createEditableAttributi zone attributi latestRun.attributi
                        , initiatedManually = False
                        }

        -- We have previous edit info
        Just runEditInfo ->
            -- We have unsaved changes to the previous run
            -- OR we have a manually edited run
            if runEditInfo.initiatedManually || unsavedAttributoChanges runEditInfo.editableAttributi.editableAttributi then
                Just runEditInfo

            else
                -- We have no unsaved changes and a run
                case head runs of
                    -- This case cannot really occur
                    Nothing ->
                        Nothing

                    Just latestRun ->
                        Just
                            { runId = latestRun.id
                            , experimentTypeId = latestRun.experimentTypeId
                            , editableAttributi = createEditableAttributi zone attributi latestRun.attributi
                            , initiatedManually = False
                            }


updateRunEditInfo : Zone -> Maybe RunEditInfo -> RunsResponse -> Maybe RunEditInfo
updateRunEditInfo zone runEditInfoRaw responseRaw =
    case responseRaw of
        Ok content ->
            updateRunEditInfoFromContent zone runEditInfoRaw content

        Err _ ->
            runEditInfoRaw


virtualExperimentTypeAttributoName : String
virtualExperimentTypeAttributoName =
    "experiment_type"


updateColumnChooser : Maybe LocalStorage -> ColumnChooser.Model -> RemoteData RequestError RunsResponseContent -> RunsResponse -> ColumnChooser.Model
updateColumnChooser localStorage ccm currentRuns runsResponse =
    case ( currentRuns, runsResponse ) of
        ( _, Ok currentResponseUnpacked ) ->
            -- Inject "virtual" attributo experiment type here
            let
                experimentTypeAttributo =
                    { name = virtualExperimentTypeAttributoName
                    , description = "Experiment Type"
                    , group = "manual"
                    , associatedTable = AssociatedTable.Run
                    , type_ = String
                    }
            in
            ColumnChooser.updateAttributi ccm (currentResponseUnpacked.attributi ++ [ experimentTypeAttributo ])

        ( _, Err _ ) ->
            ColumnChooser.init localStorage []


extractRunDates : RunsResponse -> List RunEventDate
extractRunDates runDates =
    case runDates of
        Err _ ->
            []

        Ok values ->
            values.runsDates


updateRunDateFilter : RunDateFilterInfo -> RunEventDate -> RunDateFilterInfo
updateRunDateFilter runDateFilterInfo runDate =
    { runDateFilterInfo | runDateFilter = specificRunEventDateFilter runDate }


withRunsResponse : Model -> (RunsResponseContent -> ( Model, Cmd Msg )) -> ( Model, Cmd Msg )
withRunsResponse model f =
    case model.runs of
        Success rrc ->
            f rrc

        _ ->
            ( model, Cmd.none )


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        ResetDate ->
            ( { model
                | runDateFilter = initRunDateFilter
              }
            , httpGetRunsFilter model.runFilter.runFilter initRunDateFilter.runDateFilter RunsReceived
            )

        SetRunDateFilter runDate ->
            let
                newRunDateFilter =
                    updateRunDateFilter model.runDateFilter runDate
            in
            ( { model
                | runDateFilter = newRunDateFilter
              }
            , httpGetRunsFilter model.runFilter.runFilter newRunDateFilter.runDateFilter RunsReceived
            )

        RunsReceived response ->
            let
                hasLiveStream =
                    case response of
                        Err _ ->
                            False

                        Ok { jetStreamFileId } ->
                            MaybeExtra.isJust jetStreamFileId

                newEventForm =
                    EventForm.updateLiveStream model.eventForm hasLiveStream

                newCurrentExperimentType : Maybe ExperimentType
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
                    ( { model | refreshRequest = Loading, now = now }, httpGetRunsFilter model.runFilter.runFilter model.runDateFilter.runDateFilter RunsReceived )

        EventFormMsg eventFormMsg ->
            let
                ( newEventForm, cmds ) =
                    EventForm.update eventFormMsg model.eventForm
            in
            ( { model | eventForm = newEventForm }
            , Cmd.batch
                [ case eventFormMsg of
                    EventForm.SubmitFinished _ ->
                        httpGetRunsFilter model.runFilter.runFilter model.runDateFilter.runDateFilter RunsReceived

                    _ ->
                        Cmd.none
                , Cmd.map EventFormMsg cmds
                ]
            )

        EventDelete eventId ->
            ( model, httpDeleteEvent EventDeleteFinished eventId )

        EventDeleteFinished result ->
            case result of
                Ok _ ->
                    ( model, httpGetRunsFilter model.runFilter.runFilter model.runDateFilter.runDateFilter RunsReceived )

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

        RunEditSubmit ->
            case model.runEditInfo of
                Nothing ->
                    ( model, Cmd.none )

                Just runEditInfo ->
                    case convertEditValues model.myTimeZone runEditInfo.editableAttributi of
                        Err errorList ->
                            ( { model | submitErrors = List.map (\( name, errorMessage ) -> name ++ ": " ++ errorMessage) errorList }, Cmd.none )

                        Ok editedAttributi ->
                            let
                                run =
                                    { id = runEditInfo.runId
                                    , experimentTypeId = runEditInfo.experimentTypeId
                                    , attributi = editedAttributi
                                    , summary = emptySummary
                                    , files = []
                                    , dataSets = []
                                    , runningIndexingJobs = []
                                    }
                            in
                            ( { model | runEditRequest = Loading }, httpUpdateRun RunEditFinished run )

        RunEditFinished result ->
            case result of
                Err e ->
                    ( { model | runEditRequest = Failure e }, Cmd.none )

                Ok _ ->
                    case model.runs of
                        Success _ ->
                            let
                                resetEditedFlags : RunEditInfo -> RunEditInfo
                                resetEditedFlags rei =
                                    { runId = rei.runId
                                    , experimentTypeId = rei.experimentTypeId
                                    , editableAttributi =
                                        { originalAttributi = rei.editableAttributi.originalAttributi
                                        , editableAttributi = List.map resetEditableAttributo rei.editableAttributi.editableAttributi
                                        }

                                    -- Reset manual edit flag, so we automatically jump to the latest run again
                                    , initiatedManually = False
                                    }
                            in
                            ( { model | runEditRequest = Success (), submitErrors = [], runEditInfo = Maybe.map resetEditedFlags model.runEditInfo }
                            , httpGetRunsFilter model.runFilter.runFilter model.runDateFilter.runDateFilter RunsReceived
                            )

                        _ ->
                            -- Super unlikely case, we don't really have a successful runs request, after finishing editing a run?
                            ( { model | runEditRequest = Success (), submitErrors = [], runEditInfo = Nothing }
                            , httpGetRunsFilter model.runFilter.runFilter model.runDateFilter.runDateFilter RunsReceived
                            )

        RunInitiateEdit run ->
            case model.runs of
                Success rrc ->
                    ( { model
                        | runEditInfo =
                            Just
                                { runId = run.id
                                , experimentTypeId = run.experimentTypeId
                                , editableAttributi = createEditableAttributi model.myTimeZone rrc.attributi run.attributi
                                , initiatedManually = True
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
            ( model, httpUserConfigurationSetAutoPilot AutoPilotToggled newValue )

        ChangeOnlineCrystFEL newValue ->
            ( model, httpUserConfigurationSetOnlineCrystFEL OnlineCrystFELToggled newValue )

        AutoPilotToggled _ ->
            ( model, httpGetRunsFilter model.runFilter.runFilter model.runDateFilter.runDateFilter RunsReceived )

        OnlineCrystFELToggled _ ->
            ( model, httpGetRunsFilter model.runFilter.runFilter model.runDateFilter.runDateFilter RunsReceived )

        CreateDataSetFromRun experimentTypeId runId ->
            ( { model | dataSetFromRunRequest = Loading }, httpCreateDataSetFromRun CreateDataSetFromRunFinished experimentTypeId runId )

        CreateDataSetFromRunFinished result ->
            ( { model | dataSetFromRunRequest = fromResult result }, httpGetRunsFilter model.runFilter.runFilter model.runDateFilter.runDateFilter RunsReceived )

        ChangeCurrentExperimentType ->
            case model.runEditInfo of
                Nothing ->
                    ( model, Cmd.none )

                Just ei ->
                    ( { model | changeExperimentTypeRequest = Loading }
                    , httpChangeCurrentExperimentTypeForRun (ChangeCurrentExperimentTypeFinished (Maybe.map .id model.selectedExperimentType))
                        (Maybe.map .id model.selectedExperimentType)
                        ei.runId
                    )

        ChangeCurrentExperimentTypeFinished selectedExperimentType result ->
            withRunsResponse model <|
                \response ->
                    ( { model
                        | changeExperimentTypeRequest = fromResult result
                        , currentExperimentType = ListExtra.find (\et -> Just et.id == selectedExperimentType) response.experimentTypes
                      }
                    , httpGetRunsFilter model.runFilter.runFilter model.runDateFilter.runDateFilter RunsReceived
                    )

        RunFilterSubMsg runFilterMsg ->
            let
                ( newRunFilter, cmds ) =
                    updateRunFilter model.runFilter runFilterMsg

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
