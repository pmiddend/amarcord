module Amarcord.Pages.RunOverview exposing (Model, Msg(..), init, pageTitle, subscriptions, update, view)

import Amarcord.API.ExperimentType exposing (ExperimentTypeId)
import Amarcord.API.Requests exposing (BeamtimeId, RunInternalId(..), runInternalIdToInt)
import Amarcord.Attributo exposing (attributoExposureTime, convertAttributoFromApi, convertAttributoMapFromApi, retrieveFloatAttributoValue)
import Amarcord.AttributoHtml exposing (formatIntHumanFriendly)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert, mimeTypeToIcon, spinner, viewCloseHelpButton, viewHelpButton, viewRemoteDataHttp)
import Amarcord.Chemical exposing (chemicalIdDict, convertChemicalFromApi)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.EventForm as EventForm
import Amarcord.Html exposing (form_, h1_, h3_, h4_, h5_, hr_, img_, input_, li_, p_, span_, strongText, tbody_, td_, th_, thead_, tr_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.LocalStorage exposing (LocalStorage)
import Amarcord.MarkdownUtil exposing (markupWithoutErrors)
import Amarcord.Route exposing (Route(..), makeFilesLink, makeLink)
import Amarcord.RunAttributiForm as RunAttributiForm
import Amarcord.RunStatistics exposing (viewHitRateAndIndexingGraphs)
import Amarcord.Util exposing (HereAndNow, formatPosixHumanFriendly, posixDiffHumanFriendly, secondsDiffHumanFriendly)
import Api.Data exposing (JsonChangeRunExperimentTypeOutput, JsonCreateDataSetFromRunOutput, JsonDeleteEventOutput, JsonEvent, JsonExperimentType, JsonFileOutput, JsonReadRunsOverview, JsonRun, JsonUserConfigurationSingleOutput)
import Api.Request.Config exposing (updateUserConfigurationSingleApiUserConfigBeamtimeIdKeyValuePatch)
import Api.Request.Datasets exposing (createDataSetFromRunApiDataSetsFromRunPost)
import Api.Request.Events exposing (deleteEventApiEventsDelete)
import Api.Request.Experimenttypes exposing (changeCurrentRunExperimentTypeApiExperimentTypesChangeForRunPost)
import Api.Request.Runs exposing (readRunsOverviewApiRunsOverviewBeamtimeIdGet)
import Basics.Extra exposing (safeDivide)
import Html exposing (Html, a, button, div, em, figcaption, figure, h4, label, option, p, select, span, table, text, ul)
import Html.Attributes exposing (checked, class, disabled, for, href, id, selected, src, style, type_, value)
import Html.Events exposing (onClick, onInput)
import List
import List.Extra as ListExtra
import Maybe
import Maybe.Extra as MaybeExtra exposing (isNothing)
import RemoteData exposing (RemoteData(..), fromResult, isLoading, isSuccess)
import String
import Time exposing (Posix, Zone, millisToPosix, posixToMillis)


type Msg
    = RunsReceived (Result HttpError JsonReadRunsOverview)
    | RunAttributiFormMsg RunAttributiForm.Msg
    | Refresh Posix
    | EventFormMsg EventForm.Msg
    | EventDelete Int
    | EventDeleteFinished (Result HttpError JsonDeleteEventOutput)
    | SelectedExperimentTypeChanged String
    | ChangeCurrentExperimentType
    | ChangeAutoPilot Bool
    | ChangeOnlineCrystFEL Bool
    | AutoPilotToggled (Result HttpError JsonUserConfigurationSingleOutput)
    | OnlineCrystFELToggled (Result HttpError JsonUserConfigurationSingleOutput)
    | CreateDataSetFromRun RunInternalId
    | CreateDataSetFromRunFinished (Result HttpError JsonCreateDataSetFromRunOutput)
    | ChangeCurrentExperimentTypeFinished (Maybe ExperimentTypeId) (Result HttpError JsonChangeRunExperimentTypeOutput)


type alias LatestRun =
    { run : JsonRun
    , runEditInfo : RunAttributiForm.Model
    }


type alias LoadedModel =
    { runsOverview : JsonReadRunsOverview
    , latestRun : Maybe LatestRun
    }


type alias Model =
    { loadedModel : RemoteData HttpError LoadedModel
    , zone : Zone
    , refreshRequest : RemoteData HttpError ()
    , eventForm : EventForm.Model
    , now : Posix
    , currentExperimentType : Maybe JsonExperimentType
    , selectedExperimentType : Maybe JsonExperimentType
    , localStorage : Maybe LocalStorage
    , dataSetFromRunRequest : RemoteData HttpError JsonCreateDataSetFromRunOutput
    , changeExperimentTypeRequest : RemoteData HttpError JsonChangeRunExperimentTypeOutput
    , beamtimeId : BeamtimeId
    }


subscriptions : Model -> List (Sub Msg)
subscriptions _ =
    [ Time.every 5000 Refresh ]


pageTitle : Model -> String
pageTitle { loadedModel } =
    case loadedModel of
        Success { latestRun } ->
            case latestRun of
                Nothing ->
                    "⏲💤 Waiting for runs"

                Just { run } ->
                    case run.stopped of
                        Nothing ->
                            "🏃 Run " ++ String.fromInt run.externalId

                        Just _ ->
                            "🧍 Run " ++ String.fromInt run.externalId

        _ ->
            "⏲💤 Waiting for runs"


init : HereAndNow -> Maybe LocalStorage -> BeamtimeId -> ( Model, Cmd Msg )
init { zone, now } localStorage beamtimeId =
    ( { loadedModel = Loading
      , zone = zone
      , refreshRequest = NotAsked
      , eventForm = EventForm.init beamtimeId "User"
      , now = now
      , currentExperimentType = Nothing
      , selectedExperimentType = Nothing
      , localStorage = localStorage
      , dataSetFromRunRequest = NotAsked
      , changeExperimentTypeRequest = NotAsked
      , beamtimeId = beamtimeId
      }
    , send RunsReceived (readRunsOverviewApiRunsOverviewBeamtimeIdGet beamtimeId)
    )


dataSetInformation :
    Zone
    -> BeamtimeId
    -> JsonRun
    -> RemoteData HttpError JsonCreateDataSetFromRunOutput
    -> Maybe JsonExperimentType
    -> JsonReadRunsOverview
    -> List (Html Msg)
dataSetInformation zone beamtimeId run dataSetFromRunRequest currentExperimentTypeMaybe rrc =
    case currentExperimentTypeMaybe of
        Nothing ->
            [ p [ class "text-muted" ] [ text "No experiment type selected, cannot display data set information." ] ]

        Just currentExperimentType ->
            case rrc.fomsForThisDataSet of
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

                Just dsWithFom ->
                    let
                        indexingProgress =
                            let
                                progressSummary =
                                    dsWithFom.fom

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
                                                                round <| Maybe.withDefault 0.0 <| safeDivide (toFloat remainingFrames) (ir / 100.0 * hr / 100.0)

                                                            framesPerSecond =
                                                                Maybe.withDefault 0.0 <| safeDivide 100 (2 * realExposureTime)

                                                            indexedFramesPerSecond =
                                                                framesPerSecond * ir / 100.0 * hr / 100.0

                                                            remainingTimeStr =
                                                                secondsDiffHumanFriendly <| round <| Maybe.withDefault 0.0 <| safeDivide (toFloat remainingFrames) indexedFramesPerSecond
                                                        in
                                                        Just <| text <| "Remaining time: " ++ remainingTimeStr ++ ", remaining frames " ++ formatIntHumanFriendly remainingFramesToCapture

                                                    else
                                                        Nothing
                                                )

                                    else
                                        Nothing
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
                            , div [ class "d-flex justify-content-center align-items-center" ]
                                [ p [] [ text "What’s this?" ]
                                , viewHelpButton "help-indexing-target"
                                ]
                            , div [ id "help-indexing-target", class "collapse text-bg-light p-2" ]
                                [ p_ [ text "In general, you cannot collect too many indexed frames to build the electron density. Moreover, it’s hard to say when you have “enough” of these frames to even start merging the data." ]
                                , p_ [ text "We’ve consulted with our crystallography experts on that, and came up with “shoot for 10k indexed frames”. This progress bar illustrates how far you are with that, and how long it will take to reach it, so you can decide if it’s feasible, or if you want to change parameters to increase indexing rate some more." ]
                                , viewCloseHelpButton "help-indexing-target"
                                ]
                            ]
                    in
                    [ Maybe.withDefault (text "") (Maybe.map .indexingStatistics rrc.latestIndexingResult |> Maybe.map (viewHitRateAndIndexingGraphs zone))
                    , div [ class "mb-3" ] indexingProgress
                    , h3_ [ text "Data Set", viewHelpButton "help-data-set" ]
                    , div [ id "help-data-set", class "collapse text-bg-light p-2" ]
                        [ p_
                            [ em [] [ text "Data sets" ]
                            , text " are a way to group runs according to common attributi. Each run has an associated "
                            , em [] [ text "Experiment Type" ]
                            , text ", which, in turn, consists of a set of attributi."
                            ]
                        , p_ [ text "For example, to group runs by the sample that was used and the detector distance, create an experiment type (say “SSD” for “Simple Structure Determination”) containing these two attributi, and acquire some runs:" ]
                        , h5_ [ text "Experiment Types" ]
                        , table [ class "table table-sm table-striped" ]
                            [ thead_
                                [ tr_
                                    [ th_ [ text "ID" ]
                                    , th_ [ text "Name" ]
                                    , th_ [ text "Attributi" ]
                                    ]
                                ]
                            , tbody_
                                [ tr_ [ td_ [ text "1" ], td_ [ text "SSD" ], td_ [ text "Sample, Detector Distance" ] ]
                                ]
                            ]
                        , h5_ [ text "Runs" ]
                        , table [ class "table table-sm table-striped" ]
                            [ thead_
                                [ tr_
                                    [ th_ [ text "Run ID" ]
                                    , th_ [ text "Sample" ]
                                    , th_ [ text "Detector Distance" ]
                                    , th_ [ text "Exp. Type" ]
                                    ]
                                ]
                            , tbody_
                                [ tr_ [ td_ [ text "1" ], td_ [ text "Lysozyme" ], td_ [ text "200mm" ], td_ [ text "SSD" ] ]
                                , tr_ [ td_ [ text "2" ], td_ [ text "Lysozyme" ], td_ [ text "200mm" ], td_ [ text "SSD" ] ]
                                , tr_ [ td_ [ text "3" ], td_ [ text "Lactamase" ], td_ [ text "100mm" ], td_ [ text "SSD" ] ]
                                , tr_ [ td_ [ text "4" ], td_ [ text "Lactamase" ], td_ [ text "200mm" ], td_ [ text "SSD" ] ]
                                ]
                            ]
                        , p_ [ text "Note that we just have ", em [] [ text "runs" ], text " and one ", em [] [ text "Experiment Type" ], text " for now. We don't have data sets yet. These have to be created manually, either before the experiment (if you know what you’re going to measure), or during." ]
                        , p_ [ text "To group runs and merge them, we create a data set, which is just an assignment of attributi to values. So, for example, we could create a data set for the “SSD” experiment type which sets “Detector Distance” to “200mm” and “Sample” to “Lysozyme”." ]
                        , h5_ [ text "Data Sets" ]
                        , table [ class "table table-sm table-striped" ]
                            [ thead_
                                [ tr_
                                    [ th_ [ text "ID" ]
                                    , th_ [ text "Attributi" ]
                                    ]
                                ]
                            , tbody_
                                [ tr_ [ td_ [ text "1" ], td_ [ em [] [ text "Sample" ], text ": Lysozyme, ", em [] [ text "Detector Distance" ], text ": 200mm" ] ]
                                ]
                            ]
                        , p_ [ text "From here on out, all runs with these attributi values are part of data set 1. In the analysis view, you can now easily merge all of these runs." ]
                        , p_ [ text "As described earlier, there are two ways to create data sets: either manually, on the ", a [ href (makeLink (DataSets beamtimeId)) ] [ icon { name = "arrow-right" }, text " Data Sets" ], text " page. But you can also create a Data Set from the current run, by clicking the button for that (it appears for runs which do not have a Data Set matching yet). This will take the run’s attributi and use them for the whole data set." ]
                        , viewCloseHelpButton "help-data-set"
                        ]
                    , viewDataSetTable (List.map convertAttributoFromApi rrc.attributi)
                        zone
                        (chemicalIdDict (List.map convertChemicalFromApi rrc.chemicals))
                        (convertAttributoMapFromApi dsWithFom.dataSet.attributi)
                        True
                        True
                        Nothing
                    , a
                        [ href
                            (makeLink (AnalysisDataSet beamtimeId dsWithFom.dataSet.id))
                        ]
                        [ text "→ Go to processing results" ]
                    ]


posixDiffHumanFriendlyLongDurationsExact : Zone -> Posix -> Posix -> String
posixDiffHumanFriendlyLongDurationsExact zone relative now =
    if posixToMillis now - posixToMillis relative > 48 * 60 * 60 * 1000 then
        formatPosixHumanFriendly zone relative

    else
        posixDiffHumanFriendly now relative ++ " ago "


viewCurrentRun :
    Zone
    -> BeamtimeId
    -> Posix
    -> Maybe JsonExperimentType
    -> Maybe JsonExperimentType
    -> RemoteData HttpError JsonChangeRunExperimentTypeOutput
    -> RemoteData HttpError JsonCreateDataSetFromRunOutput
    -> JsonReadRunsOverview
    -> JsonRun
    -> List (Html Msg)
viewCurrentRun zone beamtimeId now selectedExperimentType currentExperimentType changeExperimentTypeRequest dataSetFromRunRequest ({ userConfig, experimentTypes, latestIndexingResult } as rrc) ({ externalId, started, stopped } as run) =
    let
        autoPilot =
            [ div [ class "form-check form-switch mb-3" ]
                [ input_ [ type_ "checkbox", Html.Attributes.id "auto-pilot", class "form-check-input", checked userConfig.autoPilot, onInput (always (ChangeAutoPilot (not userConfig.autoPilot))) ]
                , label [ class "form-check-label", for "auto-pilot" ] [ text "Auto pilot" ]
                , viewHelpButton "help-auto-pilot"
                ]
            , div [ Html.Attributes.id "help-auto-pilot", class "collapse text-bg-light p-2" ] [ text "Manual attributi will be copied over from the previous run. Be careful not to change experimental conditions if this is active." ]
            ]

        onlineCrystFEL =
            [ div [ class "form-check form-switch mb-3" ]
                [ input_ [ type_ "checkbox", Html.Attributes.id "crystfel-online", class "form-check-input", checked userConfig.onlineCrystfel, onInput (always (ChangeOnlineCrystFEL (not userConfig.onlineCrystfel))) ]
                , label [ class "form-check-label", for "crystfel-online" ] [ text "Use CrystFEL Online" ]
                ]
            ]

        processingBar { frames, totalFrames, running } =
            Maybe.withDefault (text "") <|
                Maybe.map2
                    (\framesReal totalFramesReal ->
                        div [ class "d-flex align-items-center mb-3" ]
                            [ div [ class "me-1" ] [ span_ [ text "Frames processed:" ] ]
                            , if running then
                                div [ class "spinner-border spinner-border-sm me-1" ] []

                              else
                                text ""
                            , text (formatIntHumanFriendly framesReal ++ "/" ++ formatIntHumanFriendly totalFramesReal)
                            ]
                    )
                    frames
                    totalFrames

        header =
            case stopped of
                Nothing ->
                    [ h1_
                        [ span [ class "text-success" ] [ spinner False ]
                        , text <| " Run " ++ String.fromInt externalId
                        ]
                    , p [ class "lead text-success" ] [ strongText "Running", text <| " for " ++ posixDiffHumanFriendly now (millisToPosix started) ]
                    , case latestIndexingResult of
                        Nothing ->
                            text ""

                        Just ir ->
                            processingBar ir
                    ]

                Just realStoppedTime ->
                    [ h1_ [ icon { name = "stop-circle" }, text <| " Run " ++ String.fromInt externalId ]
                    , p [ class "lead" ] [ strongText "Stopped", text <| " " ++ posixDiffHumanFriendlyLongDurationsExact zone (millisToPosix realStoppedTime) now ]
                    , p_ [ text <| "Duration " ++ posixDiffHumanFriendly (millisToPosix started) (millisToPosix realStoppedTime) ]
                    , case latestIndexingResult of
                        Nothing ->
                            text ""

                        Just ir ->
                            processingBar ir
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
                            (option [ selected (isNothing selectedExperimentType), value "" ] [ text "«no value»" ] :: List.map viewExperimentTypeOption experimentTypes)
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
    header
        ++ autoPilot
        ++ onlineCrystFEL
        ++ dataSetSelection
        ++ dataSetInformation zone beamtimeId run dataSetFromRunRequest currentExperimentType rrc


viewEventRow : Zone -> JsonEvent -> Html Msg
viewEventRow zone e =
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
    tr_
        [ td_ [ text <| formatPosixHumanFriendly zone (millisToPosix e.created) ]
        , td_ mainContent
        ]


viewInner : Model -> LoadedModel -> List (Html Msg)
viewInner model { runsOverview, latestRun } =
    [ div [ class "container" ]
        [ case latestRun of
            Nothing ->
                loadingBar "Waiting for some runs to appear..."

            Just { run, runEditInfo } ->
                div
                    [ class "row" ]
                    [ div [ class "col-lg-6" ]
                        (viewCurrentRun
                            model.zone
                            model.beamtimeId
                            model.now
                            model.selectedExperimentType
                            model.currentExperimentType
                            model.changeExperimentTypeRequest
                            model.dataSetFromRunRequest
                            runsOverview
                            run
                        )
                    , div [ class "col-lg-6" ]
                        [ Html.map RunAttributiFormMsg <| RunAttributiForm.view runEditInfo ]
                    ]
        ]
    , hr_
    , case runsOverview.liveStream of
        Nothing ->
            Html.map EventFormMsg (EventForm.view model.eventForm)

        Just { fileId, modified } ->
            div [ class "row" ]
                [ div [ class "col-lg-6" ] [ Html.map EventFormMsg (EventForm.view model.eventForm) ]
                , div [ class "col-lg-6 text-center" ]
                    [ figure [ class "figure" ]
                        [ a [ href (makeFilesLink fileId) ] [ img_ [ src (makeFilesLink fileId ++ "?timestamp=" ++ String.fromInt (posixToMillis model.now)), style "width" "35em" ] ]
                        , figcaption [ class "figure-caption" ]
                            [ text <| "Live stream image (updated " ++ formatPosixHumanFriendly model.zone (millisToPosix modified) ++ ")"
                            ]
                        ]
                    ]
                ]
    , h4_ [ text "Logbook entries" ]
    , table [ class "table" ]
        [ thead_
            [ tr_
                [ th_ [ text "Date" ]
                , th_ [ text "Message" ]
                ]
            ]
        , tbody_ (List.map (viewEventRow model.zone) runsOverview.events)
        ]
    , a [ href (makeLink (Runs model.beamtimeId)) ] [ text "→ Go to runs table" ]
    ]


view : Model -> Html Msg
view model =
    div [ class "container" ] <|
        case model.loadedModel of
            NotAsked ->
                List.singleton <| text "Impossible state reached: time zone, but no runs in progress?"

            Loading ->
                List.singleton <| loadingBar "Loading runs..."

            Failure e ->
                List.singleton <|
                    makeAlert [ AlertDanger ] <|
                        [ h4 [ class "alert-heading" ] [ text "Failed to retrieve run overview" ], showError e ]

            Success lm ->
                viewInner model lm


withRunsResponse : Model -> (JsonReadRunsOverview -> ( Model, Cmd Msg )) -> ( Model, Cmd Msg )
withRunsResponse model f =
    case model.loadedModel of
        Success { runsOverview } ->
            f runsOverview

        _ ->
            ( model, Cmd.none )


retrieveRuns : Model -> Cmd Msg
retrieveRuns model =
    send RunsReceived (readRunsOverviewApiRunsOverviewBeamtimeIdGet model.beamtimeId)


updateLatestRun : Zone -> Maybe LatestRun -> Result HttpError JsonReadRunsOverview -> ( Maybe LatestRun, Cmd Msg )
updateLatestRun zone oldLatestRunMaybe newRequest =
    case newRequest of
        Err _ ->
            -- We've got an error in the latest run request: take old latest run - should be fine
            ( oldLatestRunMaybe, Cmd.none )

        Ok { attributi, experimentTypes, chemicals, latestRun } ->
            case latestRun of
                Just newRun ->
                    let
                        reiInitData =
                            { zone = zone
                            , attributi = attributi
                            , chemicals = List.map convertChemicalFromApi chemicals
                            , experimentTypes = experimentTypes
                            }
                    in
                    case oldLatestRunMaybe of
                        Nothing ->
                            -- We didn't have a run before, but now we do.
                            let
                                ( newRunEditInfo, subCmds ) =
                                    RunAttributiForm.init reiInitData newRun
                            in
                            ( Just { run = newRun, runEditInfo = newRunEditInfo }, Cmd.map RunAttributiFormMsg subCmds )

                        Just oldLatestRun ->
                            -- We have a latest und and a previous latest run. Update!
                            if oldLatestRun.run.id == newRun.id then
                                -- This is the same run, so we need to update.
                                let
                                    ( newRunEditInfo, subCmds ) =
                                        RunAttributiForm.update
                                            (RunAttributiForm.UpdateRun newRun)
                                            oldLatestRun.runEditInfo
                                in
                                ( Just
                                    { run = newRun
                                    , runEditInfo = newRunEditInfo
                                    }
                                , Cmd.map RunAttributiFormMsg subCmds
                                )

                            else
                                -- If we switch to a new run, ignore any changes
                                let
                                    ( newRunEditInfo, subCmds ) =
                                        RunAttributiForm.init reiInitData newRun
                                in
                                ( Just { run = newRun, runEditInfo = newRunEditInfo }, Cmd.map RunAttributiFormMsg subCmds )

                Nothing ->
                    -- We've received new information telling us that we don't have a latest run anymore.
                    -- Then forget our previous latest run.
                    ( Nothing, Cmd.none )


updateRunOverviewRequest : Zone -> Result HttpError JsonReadRunsOverview -> RemoteData HttpError LoadedModel -> ( RemoteData HttpError LoadedModel, Cmd Msg )
updateRunOverviewRequest zone newRequest oldModel =
    case oldModel of
        Success { runsOverview, latestRun } ->
            -- We had a model previously, and have one now, so let's update our structures
            let
                ( newLatestRun, cmds ) =
                    updateLatestRun zone latestRun newRequest
            in
            ( Success
                { runsOverview = Maybe.withDefault runsOverview (Result.toMaybe newRequest)
                , latestRun = newLatestRun
                }
            , cmds
            )

        _ ->
            -- We didn't have a loaded model previously, so we can create new structures
            case newRequest of
                Ok runsOverview ->
                    -- The request was successful
                    case runsOverview.latestRun of
                        -- We don't have any runs yet.
                        Nothing ->
                            ( Success { runsOverview = runsOverview, latestRun = Nothing }, Cmd.none )

                        Just latestRunReal ->
                            -- We do have runs!
                            let
                                reiInitData =
                                    { zone = zone
                                    , attributi = runsOverview.attributi
                                    , chemicals = List.map convertChemicalFromApi runsOverview.chemicals
                                    , experimentTypes = runsOverview.experimentTypes
                                    }

                                ( latestRunModel, subCmds ) =
                                    RunAttributiForm.init reiInitData latestRunReal
                            in
                            ( Success
                                { runsOverview = runsOverview
                                , latestRun = Just { run = latestRunReal, runEditInfo = latestRunModel }
                                }
                            , Cmd.map RunAttributiFormMsg subCmds
                            )

                Err e ->
                    ( Failure e, Cmd.none )


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        RunsReceived response ->
            let
                hasLiveStream =
                    case response of
                        Err _ ->
                            False

                        Ok { liveStream } ->
                            MaybeExtra.isJust liveStream

                shiftUser =
                    case response of
                        Ok { currentBeamtimeUser } ->
                            currentBeamtimeUser

                        _ ->
                            Nothing

                newEventForm =
                    EventForm.updateLiveStreamAndShiftUser model.eventForm hasLiveStream shiftUser

                newCurrentExperimentType : Maybe JsonExperimentType
                newCurrentExperimentType =
                    case response of
                        Ok { experimentTypes, userConfig } ->
                            userConfig.currentExperimentTypeId
                                |> Maybe.andThen (\cet -> ListExtra.find (\et -> et.id == cet) experimentTypes)

                        _ ->
                            model.currentExperimentType

                ( newRunOverviewRequest, subCmds ) =
                    updateRunOverviewRequest model.zone response model.loadedModel
            in
            ( { model
                | loadedModel = newRunOverviewRequest
                , eventForm = newEventForm
                , refreshRequest =
                    if isSuccess model.loadedModel then
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
            , subCmds
            )

        Refresh now ->
            case ( model.refreshRequest, model.loadedModel ) of
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
            case model.loadedModel of
                Success { latestRun } ->
                    case latestRun of
                        Nothing ->
                            ( model, Cmd.none )

                        Just { run } ->
                            ( { model | changeExperimentTypeRequest = Loading }
                            , send
                                (ChangeCurrentExperimentTypeFinished (Maybe.map .id model.selectedExperimentType))
                                (changeCurrentRunExperimentTypeApiExperimentTypesChangeForRunPost
                                    { experimentTypeId = Maybe.map .id model.selectedExperimentType
                                    , runInternalId = run.id
                                    }
                                )
                            )

                _ ->
                    ( model, Cmd.none )

        ChangeCurrentExperimentTypeFinished selectedExperimentType result ->
            withRunsResponse model <|
                \response ->
                    ( { model
                        | changeExperimentTypeRequest = fromResult result
                        , currentExperimentType = ListExtra.find (\et -> Just et.id == selectedExperimentType) response.experimentTypes
                      }
                    , retrieveRuns model
                    )

        EventDelete eventId ->
            ( model, send EventDeleteFinished (deleteEventApiEventsDelete { id = eventId }) )

        EventDeleteFinished result ->
            case result of
                Ok _ ->
                    ( model, retrieveRuns model )

                _ ->
                    ( model, Cmd.none )

        RunAttributiFormMsg subMsg ->
            case model.loadedModel of
                Success ({ latestRun } as oldLoadedModel) ->
                    case latestRun of
                        Nothing ->
                            ( model, Cmd.none )

                        Just ({ runEditInfo } as oldLatestRun) ->
                            let
                                ( newEditInfo, subCmd ) =
                                    RunAttributiForm.update subMsg runEditInfo

                                cmd =
                                    case subMsg of
                                        RunAttributiForm.SubmitFinished _ ->
                                            Cmd.batch [ Cmd.map RunAttributiFormMsg subCmd, retrieveRuns model ]

                                        _ ->
                                            Cmd.map RunAttributiFormMsg subCmd

                                newLatestRun =
                                    Just { oldLatestRun | runEditInfo = newEditInfo }

                                newLoadedModel =
                                    Success { oldLoadedModel | latestRun = newLatestRun }
                            in
                            ( { model | loadedModel = newLoadedModel }
                            , cmd
                            )

                _ ->
                    ( model, Cmd.none )
