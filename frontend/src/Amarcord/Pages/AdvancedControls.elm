module Amarcord.Pages.AdvancedControls exposing (Model, Msg(..), init, update, view)

import Amarcord.API.Requests exposing (RequestError, RunsResponse, RunsResponseContent, emptyRunEventDateFilter, emptyRunFilter, httpGetRunsFilter, httpStartRun, httpStopRun)
import Amarcord.Attributo exposing (attributoStopped, retrieveAttributoValue)
import Amarcord.Bootstrap exposing (icon)
import Amarcord.Html exposing (h2_, hr_, input_)
import Amarcord.RunsBulkUpdate as RunsBulkUpdate
import Amarcord.Util exposing (HereAndNow)
import Html exposing (Html, a, button, div, form, h2, label, p, text)
import Html.Attributes exposing (class, disabled, for, href, type_, value)
import Html.Events exposing (onClick, onInput)
import Maybe.Extra as MaybeExtra
import RemoteData exposing (RemoteData(..), fromResult, isLoading, isSuccess)
import Time exposing (Posix)


type alias Model =
    { runs : RemoteData RequestError RunsResponseContent
    , refreshRequest : RemoteData RequestError ()
    , startOrStopRequest : RemoteData RequestError ()
    , nextRunId : Int
    , isRunning : Bool
    , manualChange : Bool
    , bulkUpdateModel : RunsBulkUpdate.Model
    }


type Msg
    = StartRun
    | StartRunFinished (Result RequestError ())
    | StopRun
    | StopRunFinished (Result RequestError ())
    | RunsReceived RunsResponse
    | Refresh Posix
    | RunIdChanged (Maybe Int)
    | RunsBulkUpdateMsg RunsBulkUpdate.Msg


init : HereAndNow -> ( Model, Cmd Msg )
init hereAndNow =
    ( { runs = Loading
      , refreshRequest = NotAsked
      , nextRunId = 1
      , isRunning = False
      , startOrStopRequest = NotAsked
      , manualChange = False
      , bulkUpdateModel = RunsBulkUpdate.init hereAndNow
      }
    , httpGetRunsFilter emptyRunFilter emptyRunEventDateFilter RunsReceived
    )


calculateIsRunning : Result RequestError RunsResponseContent -> Bool
calculateIsRunning runResponse =
    case runResponse of
        Ok { runs } ->
            case List.head runs of
                Just latestRun ->
                    MaybeExtra.isNothing <| retrieveAttributoValue attributoStopped latestRun.attributi

                _ ->
                    False

        _ ->
            False


calculateNextRunId : Int -> Result RequestError RunsResponseContent -> Int
calculateNextRunId currentRunId runResponse =
    case runResponse of
        Ok { runs } ->
            case List.head runs of
                Just latestRun ->
                    if calculateIsRunning runResponse then
                        latestRun.id

                    else
                        latestRun.id + 1

                Nothing ->
                    currentRunId

        _ ->
            currentRunId


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
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
            ( { model | refreshRequest = Loading }, httpGetRunsFilter emptyRunFilter emptyRunEventDateFilter RunsReceived )

        RunIdChanged int ->
            case int of
                Nothing ->
                    ( model, Cmd.none )

                Just runId ->
                    ( { model | nextRunId = runId, manualChange = True }, Cmd.none )

        StartRun ->
            ( { model | startOrStopRequest = Loading }, httpStartRun model.nextRunId StartRunFinished )

        StopRun ->
            ( { model | startOrStopRequest = Loading, manualChange = False }, httpStopRun StopRunFinished )

        StartRunFinished result ->
            ( { model | startOrStopRequest = fromResult result }, httpGetRunsFilter emptyRunFilter emptyRunEventDateFilter RunsReceived )

        StopRunFinished result ->
            ( { model | startOrStopRequest = fromResult result }, httpGetRunsFilter emptyRunFilter emptyRunEventDateFilter RunsReceived )

        RunsBulkUpdateMsg msgInner ->
            let
                ( newModel, newCmds ) =
                    RunsBulkUpdate.update model.bulkUpdateModel msgInner
            in
            ( { model | bulkUpdateModel = newModel }, Cmd.map RunsBulkUpdateMsg newCmds )


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
                    , value (String.fromInt model.nextRunId)
                    , onInput (RunIdChanged << String.toInt)
                    , disabled (model.isRunning || isLoading model.startOrStopRequest)
                    ]
                , label [ for "run-id", class "form-label" ] [ text "Run ID" ]
                ]
            , button [ type_ "button", class "btn btn-primary me-3", disabled (model.isRunning || isLoading model.startOrStopRequest), onClick StartRun ]
                [ icon { name = "play" }, text (" Start Run " ++ String.fromInt model.nextRunId) ]
            , button [ type_ "button", class "btn btn-secondary", disabled (not model.isRunning || isLoading model.startOrStopRequest), onClick StopRun ]
                [ icon { name = "stop" }, text " Stop Run" ]
            ]
        , hr_
        , h2_ [ icon { name = "journals" }, text " Bulk update" ]
        , p [ class "lead" ] [ text "Update the attributi of more than one run at once. First, select the runs you want to change and press \"Retrieve run attributi\". Then change them and press \"Update all runs\"." ]
        , Html.map RunsBulkUpdateMsg <| RunsBulkUpdate.view model.bulkUpdateModel
        , h2 [ class "mt-3" ] [ icon { name = "file-earmark-spreadsheet" }, text " Export" ]
        , p [ class "lead" ] [ text "Done with the experiment? Ready for more analyses? Just download the whole database with a single click!" ]
        , a [ href "api/spreadsheet.zip", class "btn btn-secondary" ] [ icon { name = "file-earmark-spreadsheet" }, text " Download spreadsheet" ]
        , p [ class "text-muted" ] [ text "Right-click and choose \"Save as\". The result will be a .zip file containing an Excel file and a list of attached files, if you have any." ]
        ]
