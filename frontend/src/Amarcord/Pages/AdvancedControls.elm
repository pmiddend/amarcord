module Amarcord.Pages.AdvancedControls exposing (Model, Msg(..), init, update, view)

import Amarcord.API.Requests exposing (RequestError, RunsResponse, RunsResponseContent, httpGetRuns, httpStartRun, httpStopRun)
import Amarcord.Attributo exposing (attributoStarted, attributoStopped, retrieveAttributoValue)
import Amarcord.Bootstrap exposing (icon)
import Amarcord.Html exposing (form_, h2_, input_)
import Html exposing (Html, button, div, label, text)
import Html.Attributes exposing (class, disabled, for, type_, value)
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
    }


type Msg
    = StartRun
    | StartRunFinished (Result RequestError ())
    | StopRun
    | StopRunFinished (Result RequestError ())
    | RunsReceived RunsResponse
    | Refresh Posix
    | RunIdChanged (Maybe Int)


init : ( Model, Cmd Msg )
init =
    ( { runs = Loading
      , refreshRequest = NotAsked
      , nextRunId = 1
      , isRunning = False
      , startOrStopRequest = NotAsked
      , manualChange = False
      }
    , httpGetRuns RunsReceived
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
            ( { model | refreshRequest = Loading }, httpGetRuns RunsReceived )

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
            ( { model | startOrStopRequest = fromResult result }, httpGetRuns RunsReceived )

        StopRunFinished result ->
            ( { model | startOrStopRequest = fromResult result }, httpGetRuns RunsReceived )


view : Model -> Html Msg
view model =
    div [ class "container" ]
        [ h2_ [ text "Run controls" ]
        , form_
            [ div [ class "mb-3" ]
                [ label [ for "run-id", class "form-label" ] [ text "Run ID" ]
                , input_
                    [ type_ "number"
                    , class "form-control"
                    , value (String.fromInt model.nextRunId)
                    , onInput (RunIdChanged << String.toInt)
                    , disabled (model.isRunning || isLoading model.startOrStopRequest)
                    ]
                ]
            , button [ type_ "button", class "btn btn-primary me-3", disabled (model.isRunning || isLoading model.startOrStopRequest), onClick StartRun ]
                [ icon { name = "play" }, text (" Start Run " ++ String.fromInt model.nextRunId) ]
            , button [ type_ "button", class "btn btn-secondary", disabled (not model.isRunning || isLoading model.startOrStopRequest), onClick StopRun ]
                [ icon { name = "stop" }, text " Stop Run" ]
            ]
        ]
