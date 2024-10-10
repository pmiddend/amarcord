module Amarcord.Pages.AdvancedControls exposing (Model, Msg(..), init, subscriptions, update, view)

import Amarcord.API.ExperimentType exposing (ExperimentType)
import Amarcord.API.Requests
    exposing
        ( BeamtimeId
        , RunExternalId(..)
        , beamtimeIdToString
        , firstRunId
        , increaseRunExternalId
        , runExternalIdFromInt
        , runExternalIdToInt
        , runExternalIdToString
        )
import Amarcord.Bootstrap exposing (icon)
import Amarcord.CommandLineParser exposing (coparseCommandLine)
import Amarcord.Html exposing (div_, em_, form_, h2_, hr_, input_, onIntInput, p_)
import Amarcord.HttpError exposing (HttpError(..), send, showError)
import Amarcord.IndexingParameters as IndexingParameters
import Amarcord.RunsBulkUpdate as RunsBulkUpdate
import Amarcord.Util exposing (HereAndNow, forgetMsgInput)
import Api.Data exposing (JsonIndexingParameters, JsonReadRunsOverview, JsonStartRunOutput, JsonStopRunOutput, JsonUpdateOnlineIndexingParametersOutput, JsonUserConfigurationSingleOutput)
import Api.Request.Config exposing (readIndexingParametersApiUserConfigBeamtimeIdOnlineIndexingParametersGet, updateOnlineIndexingParametersApiUserConfigBeamtimeIdOnlineIndexingParametersPatch, updateUserConfigurationSingleApiUserConfigBeamtimeIdKeyValuePatch)
import Api.Request.Runs exposing (readRunsOverviewApiRunsOverviewBeamtimeIdGet, startRunApiRunsRunExternalIdStartBeamtimeIdGet, stopLatestRunApiRunsStopLatestBeamtimeIdGet)
import Html exposing (Html, a, button, div, form, h2, label, option, p, select, text)
import Html.Attributes exposing (class, disabled, for, href, id, selected, type_, value)
import Html.Events exposing (onClick, onInput)
import Maybe.Extra as MaybeExtra
import RemoteData exposing (RemoteData(..), fromResult, isLoading, isSuccess)
import Time exposing (Posix)


type alias Model =
    { runs : RemoteData HttpError JsonReadRunsOverview
    , refreshRequest : RemoteData HttpError ()
    , startOrStopRequest : RemoteData HttpError {}
    , nextRunId : RunExternalId
    , isRunning : Bool
    , manualChange : Bool
    , bulkUpdateModel : RunsBulkUpdate.Model
    , beamtimeId : BeamtimeId
    , onlineIndexingParameters : RemoteData HttpError IndexingParameters.Model
    , updateOnlineIndexingParameters : RemoteData HttpError JsonUpdateOnlineIndexingParametersOutput
    }


type Msg
    = StartRun
    | StartRunFinished (Result HttpError JsonStartRunOutput)
    | StopRun
    | StopRunFinished (Result HttpError JsonStopRunOutput)
    | RunsReceived (Result HttpError JsonReadRunsOverview)
    | IndexingParametersReceived (Result HttpError JsonIndexingParameters)
    | Refresh Posix
    | RunIdChanged (Maybe Int)
    | RunsBulkUpdateMsg RunsBulkUpdate.Msg
    | CurrentExperimentTypeChanged Int
    | ExperimentIdChanged (Result HttpError JsonUserConfigurationSingleOutput)
    | IndexingParametersMsg IndexingParameters.Msg
    | StartUpdateOnlineIndexingParameters
    | UpdateOnlineIndexingParametersDone (Result HttpError JsonUpdateOnlineIndexingParametersOutput)


subscriptions : Model -> List (Sub Msg)
subscriptions _ =
    [ Time.every 10000 Refresh ]


init : HereAndNow -> BeamtimeId -> ( Model, Cmd Msg )
init hereAndNow beamtimeId =
    ( { runs = Loading
      , refreshRequest = NotAsked
      , nextRunId = firstRunId
      , isRunning = False
      , startOrStopRequest = NotAsked
      , manualChange = False
      , bulkUpdateModel = RunsBulkUpdate.init hereAndNow beamtimeId
      , beamtimeId = beamtimeId
      , onlineIndexingParameters = Loading
      , updateOnlineIndexingParameters = NotAsked
      }
    , Cmd.batch
        [ send RunsReceived (readRunsOverviewApiRunsOverviewBeamtimeIdGet beamtimeId)
        , send IndexingParametersReceived (readIndexingParametersApiUserConfigBeamtimeIdOnlineIndexingParametersGet beamtimeId)
        ]
    )


calculateIsRunning : Result HttpError JsonReadRunsOverview -> Bool
calculateIsRunning runResponse =
    case runResponse of
        Ok { latestRun } ->
            case latestRun of
                Just latestRunReal ->
                    MaybeExtra.isNothing <| latestRunReal.stopped

                _ ->
                    False

        _ ->
            False


calculateNextRunId : RunExternalId -> Result HttpError JsonReadRunsOverview -> RunExternalId
calculateNextRunId currentRunId runResponse =
    case runResponse of
        Ok { latestRun } ->
            case latestRun of
                Just latestRunReal ->
                    if calculateIsRunning runResponse then
                        RunExternalId latestRunReal.externalId

                    else
                        increaseRunExternalId (RunExternalId latestRunReal.externalId)

                Nothing ->
                    currentRunId

        _ ->
            currentRunId


receiveRuns : Model -> Cmd Msg
receiveRuns model =
    send RunsReceived (readRunsOverviewApiRunsOverviewBeamtimeIdGet model.beamtimeId)


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        UpdateOnlineIndexingParametersDone result ->
            ( { model | updateOnlineIndexingParameters = RemoteData.fromResult result }, Cmd.none )

        StartUpdateOnlineIndexingParameters ->
            case model.onlineIndexingParameters of
                Success onlineIndexingParameters ->
                    case IndexingParameters.toCommandLine onlineIndexingParameters of
                        Err _ ->
                            ( model, Cmd.none )

                        Ok commandLine ->
                            ( { model | updateOnlineIndexingParameters = Loading }
                            , send UpdateOnlineIndexingParametersDone
                                (updateOnlineIndexingParametersApiUserConfigBeamtimeIdOnlineIndexingParametersPatch model.beamtimeId
                                    { commandLine = coparseCommandLine commandLine
                                    , geometryFile = onlineIndexingParameters.geometryFile
                                    , source = onlineIndexingParameters.source
                                    }
                                )
                            )

                _ ->
                    ( model, Cmd.none )

        IndexingParametersMsg paramsMsg ->
            case model.onlineIndexingParameters of
                Success onlineIndexingParameters ->
                    let
                        ( updatedIndexingParams, cmd ) =
                            IndexingParameters.update paramsMsg onlineIndexingParameters
                    in
                    ( { model | onlineIndexingParameters = Success updatedIndexingParams }, Cmd.map IndexingParametersMsg cmd )

                _ ->
                    ( model, Cmd.none )

        IndexingParametersReceived response ->
            case response of
                Err requestError ->
                    ( { model | onlineIndexingParameters = Failure requestError }, Cmd.none )

                Ok { commandLine } ->
                    -- Deliberately init "sources" empty, because then
                    -- we'll get an input field instead of a dropdown,
                    -- which makes sense. We don't know the source with online indexing yet
                    case IndexingParameters.convertCommandLineToModel (IndexingParameters.init [] "" "" False) commandLine of
                        Err e ->
                            ( { model | onlineIndexingParameters = Failure (BadJson e) }, Cmd.none )

                        Ok ipModel ->
                            ( { model | onlineIndexingParameters = Success ipModel }, Cmd.none )

        RunsReceived response ->
            ( { model
                | runs = fromResult response
                , refreshRequest =
                    if isSuccess model.runs then
                        Success ()

                    else
                        model.refreshRequest
                , nextRunId =
                    if model.manualChange then
                        model.nextRunId

                    else
                        calculateNextRunId model.nextRunId response
                , isRunning = calculateIsRunning response
              }
            , Cmd.none
            )

        Refresh _ ->
            ( { model | refreshRequest = Loading }
            , receiveRuns model
            )

        ExperimentIdChanged _ ->
            ( model, Cmd.none )

        CurrentExperimentTypeChanged newExperimentTypeId ->
            ( model
            , send ExperimentIdChanged (updateUserConfigurationSingleApiUserConfigBeamtimeIdKeyValuePatch model.beamtimeId "current-experiment-type-id" (String.fromInt newExperimentTypeId))
            )

        RunIdChanged int ->
            case int of
                Nothing ->
                    ( model, Cmd.none )

                Just runId ->
                    ( { model | nextRunId = runExternalIdFromInt runId, manualChange = True }, Cmd.none )

        StartRun ->
            ( { model | startOrStopRequest = Loading }, send StartRunFinished (startRunApiRunsRunExternalIdStartBeamtimeIdGet (runExternalIdToInt model.nextRunId) model.beamtimeId) )

        StopRun ->
            ( { model | startOrStopRequest = Loading, manualChange = False }, send StopRunFinished (stopLatestRunApiRunsStopLatestBeamtimeIdGet model.beamtimeId) )

        StartRunFinished result ->
            ( { model | startOrStopRequest = fromResult <| forgetMsgInput result }
            , receiveRuns model
            )

        StopRunFinished result ->
            ( { model | startOrStopRequest = fromResult <| forgetMsgInput result }
            , receiveRuns model
            )

        RunsBulkUpdateMsg msgInner ->
            let
                ( newModel, newCmds ) =
                    RunsBulkUpdate.update model.bulkUpdateModel msgInner
            in
            ( { model | bulkUpdateModel = newModel }, Cmd.map RunsBulkUpdateMsg newCmds )


viewChangeExperimentType : Model -> Html Msg
viewChangeExperimentType model =
    let
        viewOption : Maybe Int -> ExperimentType -> Html Msg
        viewOption currentExperimentType experimentType =
            option
                [ selected (Just experimentType.id == currentExperimentType)
                , value (String.fromInt experimentType.id)
                ]
                [ text experimentType.name ]
    in
    case model.runs of
        Success rrc ->
            form_
                [ p_ [ em_ [ text "One special case where you would change the experiment type here is when you do not have any runs yet, and are trying to start a run. A run needs an experiment type, so that won't work." ] ]
                , select
                    [ class "form-select"
                    , id "current-experiment-type"
                    , onIntInput CurrentExperimentTypeChanged
                    ]
                    ((case rrc.userConfig.currentExperimentTypeId of
                        Nothing ->
                            [ option [ selected True, disabled True ] [ text "«no value»" ] ]

                        Just _ ->
                            []
                     )
                        ++ List.map (viewOption rrc.userConfig.currentExperimentTypeId) rrc.experimentTypes
                    )
                ]

        _ ->
            text "Waiting for runs"


viewRunControls : Model -> Html Msg
viewRunControls model =
    div_
        [ h2_ [ icon { name = "arrow-left-right" }, text " Run controls" ]
        , p_ [ em_ [ text "Explicitly start and stop runs. Normally not needed, only in emergencies." ] ]
        , form [ class "mb-3" ]
            [ div [ class "form-floating mb-3" ]
                [ input_
                    [ type_ "number"
                    , class "form-control"
                    , value (runExternalIdToString model.nextRunId)
                    , onInput (RunIdChanged << String.toInt)
                    , disabled (model.isRunning || isLoading model.startOrStopRequest)
                    ]
                , label [ for "run-id", class "form-label" ] [ text "Run ID" ]
                ]
            , button [ type_ "button", class "btn btn-primary me-3", disabled (model.isRunning || isLoading model.startOrStopRequest), onClick StartRun ]
                [ icon { name = "play" }, text (" Start Run " ++ runExternalIdToString model.nextRunId) ]
            , button [ type_ "button", class "btn btn-secondary", disabled (not model.isRunning || isLoading model.startOrStopRequest), onClick StopRun ]
                [ icon { name = "stop" }, text " Stop Run" ]
            ]
        ]


viewOnlineIndexingParameters : Model -> Html Msg
viewOnlineIndexingParameters model =
    div_
        [ h2_ [ icon { name = "briefcase" }, text " Online Indexing" ]
        , p_ [ em_ [ text "These parameters will be used for every new run if CrystFEL online is activated." ] ]
        , case model.onlineIndexingParameters of
            Loading ->
                text ""

            NotAsked ->
                text ""

            Failure e ->
                showError e

            Success indexingParamFormModel ->
                div_
                    [ Html.map IndexingParametersMsg <| IndexingParameters.view indexingParamFormModel
                    , div [ class "mb-3 hstack gap-3" ]
                        [ button [ type_ "button", class "btn btn-primary", onClick StartUpdateOnlineIndexingParameters ]
                            [ icon { name = "send" }, text " Update parameters" ]
                        ]
                    , case model.updateOnlineIndexingParameters of
                        Success _ ->
                            div [ class "badge text-bg-success" ] [ text "Parameters changed!" ]

                        _ ->
                            text ""
                    ]
        ]


view : Model -> Html Msg
view model =
    div [ class "container" ]
        [ h2_ [ icon { name = "alt" }, text " Change current experiment type" ]
        , viewChangeExperimentType model
        , hr_
        , viewRunControls model
        , hr_
        , viewOnlineIndexingParameters model
        , hr_
        , h2_ [ icon { name = "journals" }, text " Bulk update" ]
        , p_ [ em_ [ text "Update the attributi of more than one run at once. First, select the runs you want to change and press \"Retrieve run attributi\". Then change them and press \"Update all runs\"." ] ]
        , Html.map RunsBulkUpdateMsg <| RunsBulkUpdate.view model.bulkUpdateModel
        , h2 [ class "mt-3" ] [ icon { name = "file-earmark-spreadsheet" }, text " Export" ]
        , p_ [ em_ [ text "Done with the experiment? Ready for more analyses? Just download the whole database with a single click!" ] ]
        , a [ href ("api/" ++ beamtimeIdToString model.beamtimeId ++ "/spreadsheet.zip"), class "btn btn-secondary" ] [ icon { name = "file-earmark-spreadsheet" }, text " Download spreadsheet" ]
        , p [ class "text-muted" ] [ text "Right-click and choose \"Save as\". The result will be a .zip file containing an Excel file and a list of attached files, if you have any." ]
        ]
