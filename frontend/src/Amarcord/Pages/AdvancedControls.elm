module Amarcord.Pages.AdvancedControls exposing (Model, Msg(..), init, update, view)

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
import Amarcord.Html exposing (form_, h2_, hr_, input_, onIntInput)
import Amarcord.RunsBulkUpdate as RunsBulkUpdate
import Amarcord.Util exposing (HereAndNow, forgetMsgInput)
import Api exposing (send)
import Api.Data exposing (JsonReadRuns, JsonStartRunOutput, JsonStopRunOutput, JsonUserConfigurationSingleOutput)
import Api.Request.Config exposing (updateUserConfigurationSingleApiUserConfigBeamtimeIdKeyValuePatch)
import Api.Request.Runs exposing (readRunsApiRunsBeamtimeIdGet, startRunApiRunsRunExternalIdStartBeamtimeIdGet, stopLatestRunApiRunsStopLatestBeamtimeIdGet)
import Html exposing (Html, a, button, div, form, h2, label, option, p, select, text)
import Html.Attributes exposing (class, disabled, for, href, id, selected, type_, value)
import Html.Events exposing (onClick, onInput)
import Http
import Maybe.Extra as MaybeExtra
import RemoteData exposing (RemoteData(..), fromResult, isLoading, isSuccess)
import Time exposing (Posix)


type alias Model =
    { runs : RemoteData Http.Error JsonReadRuns
    , refreshRequest : RemoteData Http.Error ()
    , startOrStopRequest : RemoteData Http.Error {}
    , nextRunId : RunExternalId
    , isRunning : Bool
    , manualChange : Bool
    , bulkUpdateModel : RunsBulkUpdate.Model
    , beamtimeId : BeamtimeId
    }


type Msg
    = StartRun
    | StartRunFinished (Result Http.Error JsonStartRunOutput)
    | StopRun
    | StopRunFinished (Result Http.Error JsonStopRunOutput)
    | RunsReceived (Result Http.Error JsonReadRuns)
    | Refresh Posix
    | RunIdChanged (Maybe Int)
    | RunsBulkUpdateMsg RunsBulkUpdate.Msg
    | CurrentExperimentTypeChanged Int
    | ExperimentIdChanged (Result Http.Error JsonUserConfigurationSingleOutput)


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
      }
    , send
        RunsReceived
        (readRunsApiRunsBeamtimeIdGet beamtimeId Nothing Nothing)
    )


calculateIsRunning : Result Http.Error JsonReadRuns -> Bool
calculateIsRunning runResponse =
    case runResponse of
        Ok { runs } ->
            case List.head runs of
                Just latestRun ->
                    MaybeExtra.isNothing <| latestRun.stopped

                _ ->
                    False

        _ ->
            False


calculateNextRunId : RunExternalId -> Result Http.Error JsonReadRuns -> RunExternalId
calculateNextRunId currentRunId runResponse =
    case runResponse of
        Ok { runs } ->
            case List.head runs of
                Just latestRun ->
                    if calculateIsRunning runResponse then
                        RunExternalId latestRun.externalId

                    else
                        increaseRunExternalId (RunExternalId latestRun.externalId)

                Nothing ->
                    currentRunId

        _ ->
            currentRunId


receiveRuns : Model -> Cmd Msg
receiveRuns model = send RunsReceived (readRunsApiRunsBeamtimeIdGet model.beamtimeId Nothing Nothing)


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        ExperimentIdChanged _ ->
            ( model, Cmd.none )

        CurrentExperimentTypeChanged newExperimentTypeId ->
            ( model
            , send ExperimentIdChanged (updateUserConfigurationSingleApiUserConfigBeamtimeIdKeyValuePatch model.beamtimeId "current-experiment-type-id" (String.fromInt newExperimentTypeId))
            )

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
                [ select
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


view : Model -> Html Msg
view model =
    div [ class "container" ]
        [ h2_ [ icon { name = "arrow-left-right" }, text " Run controls" ]
        , p [ class "lead" ] [ text "Explicitly start and stop runs. Normally not needed, only in emergencies." ]
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
        , hr_
        , h2_ [ icon { name = "alt" }, text " Change current experiment type" ]
        , viewChangeExperimentType model
        , hr_
        , h2_ [ icon { name = "journals" }, text " Bulk update" ]
        , p [ class "lead" ] [ text "Update the attributi of more than one run at once. First, select the runs you want to change and press \"Retrieve run attributi\". Then change them and press \"Update all runs\"." ]
        , Html.map RunsBulkUpdateMsg <| RunsBulkUpdate.view model.bulkUpdateModel
        , h2 [ class "mt-3" ] [ icon { name = "file-earmark-spreadsheet" }, text " Export" ]
        , p [ class "lead" ] [ text "Done with the experiment? Ready for more analyses? Just download the whole database with a single click!" ]
        , a [ href ("api/" ++ beamtimeIdToString model.beamtimeId ++ "/spreadsheet.zip"), class "btn btn-secondary" ] [ icon { name = "file-earmark-spreadsheet" }, text " Download spreadsheet" ]
        , p [ class "text-muted" ] [ text "Right-click and choose \"Save as\". The result will be a .zip file containing an Excel file and a list of attached files, if you have any." ]
        ]
