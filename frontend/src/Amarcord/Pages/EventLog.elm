module Amarcord.Pages.EventLog exposing (..)

import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.Html exposing (input_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.MarkdownUtil exposing (markupWithoutErrors)
import Amarcord.Util exposing (HereAndNow, formatPosixHumanFriendly)
import Api.Data exposing (JsonEvent, JsonReadEvents)
import Api.Request.Events exposing (readEventsApiEventsBeamtimeIdGet)
import Date
import Html exposing (Html, div, h1, label, table, tbody, td, text, th, thead, tr)
import Html.Attributes exposing (checked, class, for, id, type_)
import Html.Events exposing (onClick)
import RemoteData exposing (RemoteData(..), fromResult)
import Time exposing (Posix, millisToPosix, posixToMillis, utc)
import Time.Extra exposing (partsToPosix)


subscriptions : Model -> List (Sub Msg)
subscriptions _ =
    [ Time.every 10000 Refresh ]


type Msg
    = EventsUpdated (Result HttpError JsonReadEvents)
    | Refresh Posix
    | SetRunDateFilter String


type alias Model =
    { eventRequest : RemoteData HttpError JsonReadEvents
    , beamtimeId : BeamtimeId
    , hereAndNow : HereAndNow
    , dateFilter : String
    }


pageTitle : Model -> String
pageTitle _ =
    "Event Log"


init : HereAndNow -> BeamtimeId -> ( Model, Cmd Msg )
init hereAndNow beamtimeId =
    ( { eventRequest = Loading
      , beamtimeId = beamtimeId
      , hereAndNow = hereAndNow
      , dateFilter = ""
      }
    , send EventsUpdated (readEventsApiEventsBeamtimeIdGet beamtimeId)
    )


viewEventRow : JsonEvent -> Html msg
viewEventRow e =
    tr []
        [ td [] [ text (formatPosixHumanFriendly utc (millisToPosix e.createdLocal)) ]
        , td [] [ text e.level ]
        , td [] [ text e.source ]
        , td [] [ markupWithoutErrors e.text ]
        ]


dateToStartEnd : String -> Result String ( Posix, Posix )
dateToStartEnd d =
    case Date.fromIsoString d of
        Err e ->
            Err e

        Ok parsed ->
            let
                partsStartOfDay : Time.Extra.Parts
                partsStartOfDay =
                    { year = Date.year parsed, month = Date.month parsed, day = Date.day parsed, hour = 0, minute = 0, second = 0, millisecond = 0 }

                partsEndOfDay : Time.Extra.Parts
                partsEndOfDay =
                    { year = Date.year parsed, month = Date.month parsed, day = Date.day parsed, hour = 23, minute = 59, second = 59, millisecond = 999 }
            in
            Ok ( partsToPosix utc partsStartOfDay, partsToPosix utc partsEndOfDay )


viewDateRadioOption : Model -> String -> List (Html Msg)
viewDateRadioOption model runEventDate =
    [ input_
        [ type_ "radio"
        , class "btn-check"
        , id ("filter" ++ runEventDate)
        , checked (runEventDate == model.dateFilter)
        , onClick (SetRunDateFilter runEventDate)
        ]
    , label
        [ class "btn btn-outline-primary"
        , for ("filter" ++ runEventDate)
        ]
        [ text runEventDate ]
    ]


viewDateFilterButtons : Model -> List String -> List (Html Msg)
viewDateFilterButtons model filterDates =
    List.concat
        [ [ input_
                [ type_ "radio"
                , class "btn-check"
                , id "all_dates"
                , checked
                    (String.isEmpty model.dateFilter)
                , onClick (SetRunDateFilter "")
                ]
          , label [ class "btn btn-outline-primary", for "all_dates" ] [ text "All dates" ]
          ]
        , List.concatMap (viewDateRadioOption model) filterDates
        ]


viewDateFilter : Model -> List String -> Html Msg
viewDateFilter model filterDates =
    div [ class "btn-group" ] (viewDateFilterButtons model filterDates)


view : Model -> Html Msg
view model =
    div [ class "container" ]
        [ h1 [] [ text "Events" ]
        , case model.eventRequest of
            Success events ->
                let
                    filteredEvents =
                        case dateToStartEnd model.dateFilter of
                            Err _ ->
                                events.events

                            Ok ( start, end ) ->
                                List.filter (\e -> e.created >= posixToMillis start && e.created <= posixToMillis end) events.events
                in
                div []
                    [ viewDateFilter model events.filterDates
                    , table [ class "table table-striped" ]
                        [ thead [ class "thead-light" ]
                            [ tr []
                                [ th [] [ text "Date" ]
                                , th [] [ text "Level" ]
                                , th [] [ text "Source" ]
                                , th [] [ text "Content" ]
                                ]
                            ]
                        , tbody []
                            (List.map viewEventRow filteredEvents)
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
        SetRunDateFilter n ->
            ( { model | dateFilter = n }, Cmd.none )

        Refresh _ ->
            ( model, send EventsUpdated (readEventsApiEventsBeamtimeIdGet model.beamtimeId) )

        EventsUpdated events ->
            ( { model | eventRequest = fromResult events }, Cmd.none )
