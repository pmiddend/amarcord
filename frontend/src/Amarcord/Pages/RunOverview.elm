module Amarcord.Pages.RunOverview exposing (Model, Msg(..), init, update, view)

import Amarcord.API.Requests exposing (Event, RequestError, Run, RunEventDate, RunEventDateFilter, RunFilter(..), RunsResponse, RunsResponseContent, emptyRunEventDateFilter, emptyRunFilter, httpChangeCurrentExperimentType, httpCreateDataSetFromRun, httpDeleteEvent, httpGetRunsFilter, httpUpdateRun, httpUserConfigurationSetAutoPilot, httpUserConfigurationSetOnlineCrystFEL, runEventDateFilter, runEventDateToString, runFilterToString, specificRunEventDateFilter)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType(..), AttributoValue, attributoExposureTime, attributoStarted, attributoStopped, extractDateTime, retrieveAttributoValue, retrieveDateTimeAttributoValue, retrieveFloatAttributoValue)
import Amarcord.AttributoHtml exposing (AttributoFormMsg(..), AttributoNameWithValueUpdate, EditableAttributiAndOriginal, convertEditValues, createEditableAttributi, editEditableAttributi, formatFloatHumanFriendly, formatIntHumanFriendly, isEditValueChemicalId, makeAttributoHeader, resetEditableAttributo, unsavedAttributoChanges, viewAttributoCell, viewAttributoForm)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert, mimeTypeToIcon, spinner, viewRemoteData)
import Amarcord.Chemical exposing (Chemical, chemicalIdDict)
import Amarcord.ColumnChooser as ColumnChooser
import Amarcord.Constants exposing (manualAttributiGroup, manualGlobalAttributiGroup)
import Amarcord.DataSet exposing (DataSet, DataSetSummary, emptySummary)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.EventForm as EventForm exposing (Msg(..))
import Amarcord.File exposing (File)
import Amarcord.Gauge exposing (gauge, thisFillColor, totalFillColor)
import Amarcord.Html exposing (div_, form_, h1_, h2_, h3_, h5_, hr_, img_, input_, li_, p_, strongText, tbody_, td_, th_, thead_)
import Amarcord.LocalStorage exposing (LocalStorage)
import Amarcord.MarkdownUtil exposing (markupWithoutErrors)
import Amarcord.Route exposing (makeFilesLink)
import Amarcord.Util exposing (HereAndNow, formatPosixTimeOfDayHumanFriendly, posixBefore, posixDiffHumanFriendly, scrollToTop, secondsDiffHumanFriendly)
import Char exposing (fromCode)
import Date exposing (Date)
import Dict exposing (Dict)
import Html exposing (Html, a, button, div, em, figcaption, figure, form, h4, label, option, p, select, span, table, td, text, tr, ul)
import Html.Attributes exposing (checked, class, colspan, disabled, for, href, id, name, selected, src, style, type_, value)
import Html.Events exposing (onClick, onInput)
import Html.Events.Extra exposing (onEnter)
import List exposing (head)
import List.Extra exposing (find)
import Maybe
import Maybe.Extra as MaybeExtra exposing (isNothing)
import RemoteData exposing (RemoteData(..), fromResult, isLoading, isSuccess)
import Set exposing (Set)
import String exposing (fromInt)
import Time exposing (Posix, Weekday(..), Zone, posixToMillis)
import Tuple exposing (first, second)


type Msg
    = RunsReceived RunsResponse
    | Refresh Posix
    | EventDelete Int
    | EventDeleteFinished (Result RequestError ())
    | EventFormMsg EventForm.Msg
    | RunEditInfoValueUpdate AttributoNameWithValueUpdate
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
    | CreateDataSetFromRun String Int
    | CreateDataSetFromRunFinished (Result RequestError ())
    | ChangeCurrentExperimentTypeFinished (Maybe String) (Result RequestError ())
    | ResetDate
    | SetRunDateFilter RunEventDate


type alias EventForm =
    { userName : String
    , message : String
    , fileIds : List Int
    , fileUploadRequest : RemoteData RequestError ()
    }


type alias RunEditInfo =
    { runId : Int
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


initRunFilter =
    { runFilter = emptyRunFilter
    , nextRunFilter = ""
    , runFilterRequest = NotAsked
    }


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
    , currentExperimentType : Maybe String
    , selectedExperimentType : Maybe String
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


attributiColumns : Zone -> Dict Int String -> List (Attributo AttributoType) -> Run -> List (Html Msg)
attributiColumns zone chemicalIds attributi run =
    List.map (viewAttributoCell { shortDateTime = True, colorize = True } zone chemicalIds run.attributi) <| List.filter (\a -> a.associatedTable == AssociatedTable.Run) attributi


viewRunRow : Zone -> Dict Int String -> List (Attributo AttributoType) -> Run -> Html Msg
viewRunRow zone chemicalIds attributi r =
    tr [ style "white-space" "nowrap" ] <|
        td_ [ text (fromInt r.id) ]
            :: attributiColumns zone chemicalIds attributi r
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


viewRunAndEventRows : Zone -> Dict Int String -> List (Attributo AttributoType) -> List Run -> List Event -> List (Html Msg)
viewRunAndEventRows zone chemicalIds attributi runs events =
    case ( head runs, head events ) of
        -- No elements, neither runs nor events, left anymore
        ( Nothing, Nothing ) ->
            []

        -- Only runs left
        ( Just _, Nothing ) ->
            List.map (viewRunRow zone chemicalIds attributi) runs

        -- Only events left
        ( Nothing, Just _ ) ->
            List.map (viewEventRow zone (List.length attributi)) events

        -- We have events and runs and have to compare the dates
        ( Just run, Just event ) ->
            case Maybe.andThen extractDateTime <| retrieveAttributoValue attributoStarted run.attributi of
                Just runStarted ->
                    if posixBefore event.created runStarted then
                        viewRunRow zone chemicalIds attributi run :: viewRunAndEventRows zone chemicalIds attributi (List.drop 1 runs) events

                    else
                        viewEventRow zone (List.length attributi) event :: viewRunAndEventRows zone chemicalIds attributi runs (List.drop 1 events)

                -- We don't have a start time...take the run
                Nothing ->
                    viewRunRow zone chemicalIds attributi run :: viewRunAndEventRows zone chemicalIds attributi (List.drop 1 runs) events


viewRunsTable : Zone -> List (Attributo AttributoType) -> RunsResponseContent -> Html Msg
viewRunsTable zone chosenColumns { runs, attributi, events, chemicals } =
    let
        runRows : List (Html Msg)
        runRows =
            viewRunAndEventRows zone (chemicalIdDict chemicals) chosenColumns runs events
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


viewCurrentRun : Zone -> Posix -> Maybe String -> Maybe String -> RemoteData RequestError () -> RemoteData RequestError () -> RunsResponseContent -> List (Html Msg)
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

                runExposureTime : Maybe Float
                runExposureTime =
                    retrieveFloatAttributoValue attributoExposureTime attributi

                autoPilot =
                    [ div [ class "form-check form-switch mb-3" ]
                        [ input_ [ type_ "checkbox", Html.Attributes.id "auto-pilot", class "form-check-input", checked rrc.autoPilot, onInput (always (ChangeAutoPilot (not rrc.autoPilot))) ]
                        , label [ class "form-check-label", for "auto-pilot" ] [ text "Auto pilot" ]
                        , div [ class "form-text" ] [ text "Manual attributi will be copied over from the previous run. Be careful not to change experimental conditions if this is active." ]
                        ]
                    ]

                onlineCrystFEL =
                    [ div [ class "form-check form-switch mb-3" ]
                        [ input_ [ type_ "checkbox", Html.Attributes.id "crystfel-online", class "form-check-input", checked rrc.onlineCrystFEL, onInput (always (ChangeOnlineCrystFEL (not rrc.onlineCrystFEL))) ]
                        , label [ class "form-check-label", for "crystfel-online" ] [ text "Use CrystFEL Online" ]

                        -- , div [ class "form-text" ] [ text "Manual attributi will be copied over from the previous run. Be careful not to change experimental conditions if this is active." ]
                        ]
                    ]

                header =
                    case ( runStarted, runStopped ) of
                        ( Just started, Nothing ) ->
                            [ h1_
                                [ span [ class "text-success" ] [ spinner ]
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

                viewExperimentTypeOption experimentType =
                    option [ selected (Just experimentType == currentExperimentType), value experimentType ] [ text experimentType ]

                dataSetSelection =
                    [ form_
                        [ div [ class "input-group mb-3" ]
                            [ div [ class "form-floating flex-grow-1" ]
                                [ select
                                    [ class "form-select"
                                    , Html.Attributes.id "current-experiment-type"
                                    , onInput SelectedExperimentTypeChanged
                                    ]
                                    (option [ selected (isNothing selectedExperimentType), value "" ] [ text "«no value»" ] :: List.map viewExperimentTypeOption (Dict.keys rrc.experimentTypes))
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
                            find (\ds -> ds.experimentType == experimentType && List.member ds.id dataSets) rrc.dataSets

                smallBox color =
                    span [ style "color" color ] [ text <| String.fromChar <| fromCode 9632 ]

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
                                                case runExposureTime of
                                                    Nothing ->
                                                        Nothing

                                                    Just realExposureTime ->
                                                        let
                                                            framesPerSecond =
                                                                1000 / (2 * realExposureTime)

                                                            indexedFramesPerSecond =
                                                                framesPerSecond * ir / 100.0 * hr / 100.0

                                                            remainingFrames =
                                                                10000 - progressSummary.indexedFrames

                                                            remainingTimeStr =
                                                                secondsDiffHumanFriendly <| round (toFloat remainingFrames / indexedFramesPerSecond)

                                                            remainingFramesToCapture =
                                                                round <| toFloat remainingFrames / (ir / 100.0 * hr / 100.0)
                                                        in
                                                        if remainingFrames > 0 then
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

                dataSetInformation =
                    case currentRunDataSet of
                        Nothing ->
                            case currentExperimentType of
                                Nothing ->
                                    [ p [ class "text-muted" ] [ text "No experiment type selected, cannot display data set information." ] ]

                                Just experimentType ->
                                    [ p [ class "text-muted" ] [ text <| "Run is not part of any data set in \"" ++ experimentType ++ "\". You can automatically create a data set that matches the current run's attributi." ]
                                    , button [ type_ "button", class "btn btn-secondary", onClick (CreateDataSetFromRun experimentType id), disabled (isLoading dataSetFromRunRequest) ]
                                        [ icon { name = "plus-lg" }, text " Create data set from run" ]
                                    , viewRemoteData "Data set created" dataSetFromRunRequest
                                    ]

                        Just ds ->
                            [ div [ class "d-flex flex-row justify-content-evenly" ]
                                [ div [ class "text-center" ] (twoValueGauge "Indexing rate" summary.indexingRate Nothing)
                                , div [ class "text-center" ] (twoValueGauge "Hit rate" summary.hitRate Nothing)
                                ]
                            , div [ class "mb-3" ] indexingProgress
                            , h3_ [ text "Data set" ]
                            , viewDataSetTable rrc.attributi
                                zone
                                (chemicalIdDict rrc.chemicals)
                                ds
                                True
                                Nothing
                            ]
            in
            header ++ autoPilot ++ onlineCrystFEL ++ dataSetSelection ++ dataSetInformation


viewRunAttributiForm : Maybe (Set String) -> Maybe Run -> List String -> RemoteData RequestError () -> List (Chemical Int a b) -> Maybe RunEditInfo -> List (Html Msg)
viewRunAttributiForm currentExperimentType latestRun submitErrorsList runEditRequest chemicals rei =
    case rei of
        Nothing ->
            []

        Just { runId, editableAttributi } ->
            let
                matchesCurrentExperiment a x =
                    case x of
                        Nothing ->
                            True

                        Just attributiNames ->
                            Set.member a.name attributiNames

                -- For ergonomic reasons, we want chemical attributi to be on top - everything else should be
                -- sorted alphabetically
                attributoSortKey a =
                    ( if isEditValueChemicalId (second a.type_) then
                        0

                      else
                        1
                    , a.name
                    )

                attributoFilterFunction a =
                    a.associatedTable
                        == AssociatedTable.Run
                        && (a.group == manualGlobalAttributiGroup || a.group == manualAttributiGroup && matchesCurrentExperiment a currentExperimentType)
                        && not (List.member a.name [ "started", "stopped" ])

                filteredAttributi =
                    List.sortBy attributoSortKey <| List.filter attributoFilterFunction editableAttributi.editableAttributi

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
                        , disabled (isLoading runEditRequest || not (unsavedAttributoChanges editableAttributi.editableAttributi))
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
            in
            h2_
                [ text <|
                    if isLatestRun then
                        "Edit run"

                    else
                        "Edit run " ++ fromInt runId
                ]
                :: [ if not isLatestRun then
                        p [ class "text-warning" ] [ text "You are currently editing an older run!" ]

                     else
                        text ""
                   , form [ class "mb-3" ]
                        (List.map (Html.map attributoFormMsgToMsg << viewAttributoForm chemicals) filteredAttributi ++ submitErrors ++ submitSuccess ++ buttons)
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
                            (Maybe.andThen (\a -> Dict.get a rrc.experimentTypes) model.currentExperimentType)
                            (head rrc.runs)
                            model.submitErrors
                            model.runEditRequest
                            rrc.chemicals
                            model.runEditInfo
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
                List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve runs" ] ] ++ [ showRequestError e ]

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
                    Just { runId = latestRun.id, editableAttributi = createEditableAttributi zone attributi latestRun.attributi, initiatedManually = False }

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
                        Just { runId = latestRun.id, editableAttributi = createEditableAttributi zone attributi latestRun.attributi, initiatedManually = False }


updateRunEditInfo : Zone -> Maybe RunEditInfo -> RunsResponse -> Maybe RunEditInfo
updateRunEditInfo zone runEditInfoRaw responseRaw =
    case responseRaw of
        Ok content ->
            updateRunEditInfoFromContent zone runEditInfoRaw content

        Err _ ->
            runEditInfoRaw


updateColumnChooser : Maybe LocalStorage -> ColumnChooser.Model -> RemoteData RequestError RunsResponseContent -> RunsResponse -> ColumnChooser.Model
updateColumnChooser localStorage ccm currentRuns runsResponse =
    case ( currentRuns, runsResponse ) of
        ( _, Ok currentResponseUnpacked ) ->
            ColumnChooser.updateAttributi ccm currentResponseUnpacked.attributi

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
                , currentExperimentType =
                    case ( model.currentExperimentType, response ) of
                        -- We have an experiment type and need to check it
                        ( Just currentExperimentType, Ok { experimentTypes } ) ->
                            -- Could be that the experiment type disappeared!
                            if Dict.member currentExperimentType experimentTypes then
                                Just currentExperimentType

                            else
                                List.head <| Dict.keys experimentTypes

                        -- We have an experiment type, but an error now. Just keep it for now.
                        ( currentExperimentType, _ ) ->
                            currentExperimentType
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
                ((case eventFormMsg of
                    EventForm.SubmitFinished _ ->
                        httpGetRunsFilter model.runFilter.runFilter model.runDateFilter.runDateFilter RunsReceived

                    _ ->
                        Cmd.none
                 )
                    :: [ Cmd.map EventFormMsg cmds ]
                )
            )

        EventDelete eventId ->
            ( model, httpDeleteEvent EventDeleteFinished eventId )

        EventDeleteFinished result ->
            case result of
                Ok _ ->
                    ( model, httpGetRunsFilter model.runFilter.runFilter model.runDateFilter.runDateFilter RunsReceived )

                _ ->
                    ( model, Cmd.none )

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
                                    , attributi = editedAttributi
                                    , summary = emptySummary
                                    , files = []
                                    , dataSets = []
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
                                , editableAttributi = createEditableAttributi model.myTimeZone rrc.attributi run.attributi
                                , initiatedManually = True
                                }
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

        SelectedExperimentTypeChanged string ->
            ( { model
                | selectedExperimentType =
                    if string == "" then
                        Nothing

                    else
                        Just string
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

        CreateDataSetFromRun experimentType runId ->
            ( { model | dataSetFromRunRequest = Loading }, httpCreateDataSetFromRun CreateDataSetFromRunFinished experimentType runId )

        CreateDataSetFromRunFinished result ->
            ( { model | dataSetFromRunRequest = fromResult result }, httpGetRunsFilter model.runFilter.runFilter model.runDateFilter.runDateFilter RunsReceived )

        ChangeCurrentExperimentType ->
            case model.runEditInfo of
                Nothing ->
                    ( model, Cmd.none )

                Just ei ->
                    ( { model | changeExperimentTypeRequest = Loading }
                    , httpChangeCurrentExperimentType (ChangeCurrentExperimentTypeFinished model.selectedExperimentType) model.selectedExperimentType ei.runId
                    )

        ChangeCurrentExperimentTypeFinished selectedExperimentType result ->
            ( { model | changeExperimentTypeRequest = fromResult result, currentExperimentType = selectedExperimentType }
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
