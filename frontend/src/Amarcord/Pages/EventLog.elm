module Amarcord.Pages.EventLog exposing (..)

import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.MarkdownUtil exposing (markupWithoutErrors)
import Amarcord.Util exposing (HereAndNow, formatPosixHumanFriendly)
import Api.Data exposing (JsonEvent, JsonReadEvents)
import Api.Request.Events exposing (readEventsApiEventsBeamtimeIdGet)
import Html exposing (Html, div, h1, table, tbody, td, text, th, thead, tr)
import Html.Attributes exposing (class)
import RemoteData exposing (RemoteData(..), fromResult)
import Time exposing (Posix, Zone, millisToPosix)


subscriptions : Model -> List (Sub Msg)
subscriptions _ =
    [ Time.every 10000 Refresh ]


type Msg
    = EventsUpdated (Result HttpError JsonReadEvents)
    | Refresh Posix


type alias Model =
    { eventRequest : RemoteData HttpError JsonReadEvents
    , beamtimeId : BeamtimeId
    , hereAndNow : HereAndNow
    }


init : HereAndNow -> BeamtimeId -> ( Model, Cmd Msg )
init hereAndNow beamtimeId =
    ( { eventRequest = Loading
      , beamtimeId = beamtimeId
      , hereAndNow = hereAndNow
      }
    , send EventsUpdated (readEventsApiEventsBeamtimeIdGet beamtimeId)
    )


viewEventRow : Zone -> JsonEvent -> Html msg
viewEventRow zone e =
    tr []
        [ td [] [ text (formatPosixHumanFriendly zone (millisToPosix e.created)) ]
        , td [] [ text e.level ]
        , td [] [ text e.source ]
        , td [] [ markupWithoutErrors e.text ]
        ]


view : Model -> Html Msg
view model =
    div [ class "container" ]
        [ h1 [] [ text "Events" ]
        , case model.eventRequest of
            Success events ->
                div []
                    [ table [ class "table table-striped" ]
                        [ thead [ class "thead-light" ]
                            [ tr []
                                [ th [] [ text "Date" ]
                                , th [] [ text "Level" ]
                                , th [] [ text "Source" ]
                                , th [] [ text "Content" ]
                                ]
                            ]
                        , tbody []
                            (List.map (viewEventRow model.hereAndNow.zone) events.events)
                        ]
                    ]

            Failure e ->
                showError e

            _ ->
                text "Loading..."
        ]


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        Refresh _ ->
            ( model, send EventsUpdated (readEventsApiEventsBeamtimeIdGet model.beamtimeId) )

        EventsUpdated events ->
            ( { model | eventRequest = fromResult events }, Cmd.none )
