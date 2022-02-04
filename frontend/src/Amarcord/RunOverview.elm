module Amarcord.RunOverview exposing (..)

import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue, attributoDecoder, attributoMapDecoder, attributoTypeDecoder, extractDateTime, makeAttributoCell, makeAttributoHeader, retrieveAttributoValue, toAttributoName)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert, showHttpError, spinner)
import Amarcord.File exposing (File, fileDecoder)
import Amarcord.Html exposing (h5_, input_, strongText, tbody_, td_, th_, thead_, tr_)
import Amarcord.Sample exposing (Sample, sampleDecoder)
import Amarcord.UserError exposing (UserError, userErrorDecoder)
import Amarcord.Util exposing (formatPosixTimeOfDayHumanFriendly, httpDelete, posixBefore)
import Dict exposing (Dict)
import Hotkeys exposing (onEnter)
import Html exposing (Html, a, button, div, form, h4, table, td, text, tr)
import Html.Attributes exposing (class, colspan, disabled, placeholder, style, type_, value)
import Html.Events exposing (onClick, onInput)
import Http exposing (jsonBody)
import Iso8601 exposing (toTime)
import Json.Decode as Decode
import Json.Encode as Encode
import List exposing (head)
import Parser exposing (deadEndsToString)
import RemoteData exposing (RemoteData(..), fromResult, isLoading, isSuccess)
import String exposing (fromInt)
import Task
import Time exposing (Posix, Zone, here)


type alias Run =
    { id : Int
    , attributi : AttributoMap AttributoValue
    , files : List File
    }


type alias Event =
    { id : Int
    , text : String
    , source : String
    , level : String
    , created : Posix
    }


type alias RunsResponseContent =
    { runs : List Run
    , attributi : List (Attributo AttributoType)
    , events : List Event
    , samples : List (Sample Int (AttributoMap AttributoValue) File)
    }


type alias RunsResponse =
    Result Http.Error RunsResponseContent


runDecoder : Decode.Decoder Run
runDecoder =
    Decode.map3
        Run
        (Decode.field "id" Decode.int)
        (Decode.field "attributi" attributoMapDecoder)
        (Decode.field "files" (Decode.list fileDecoder))


eventDecoder : Decode.Decoder Event
eventDecoder =
    Decode.map5
        Event
        (Decode.field "id" Decode.int)
        (Decode.field "text" Decode.string)
        (Decode.field "source" Decode.string)
        (Decode.field "level" Decode.string)
        (Decode.field "created" Decode.string
            |> Decode.andThen
                (\timeString ->
                    case toTime timeString of
                        Ok v ->
                            Decode.succeed v

                        Err e ->
                            Decode.fail (deadEndsToString e)
                )
        )


encodeEvent : String.String -> String.String -> Encode.Value
encodeEvent source text =
    Encode.object [ ( "source", Encode.string source ), ( "text", Encode.string text ) ]


httpCreateEvent : String -> String -> Cmd Msg
httpCreateEvent source text =
    Http.post
        { url = "/api/events"
        , expect = Http.expectJson EventFormSubmitFinished (Decode.maybe (Decode.field "error" userErrorDecoder))
        , body = jsonBody (encodeEvent source text)
        }


httpDeleteEvent : Int -> Cmd Msg
httpDeleteEvent eventId =
    httpDelete
        { url = "/api/events"
        , body = jsonBody (Encode.object [ ( "id", Encode.int eventId ) ])
        , expect = Http.expectJson EventDeleteFinished (Decode.maybe (Decode.field "error" userErrorDecoder))
        }


httpGetRuns : (RunsResponse -> msg) -> Cmd msg
httpGetRuns f =
    Http.get
        { url = "/api/runs"
        , expect =
            Http.expectJson f <|
                Decode.map4 RunsResponseContent
                    (Decode.field "runs" <| Decode.list runDecoder)
                    (Decode.field "attributi" <| Decode.list (attributoDecoder attributoTypeDecoder))
                    (Decode.field "events" <| Decode.list eventDecoder)
                    (Decode.field "samples" <| Decode.list sampleDecoder)
        }


type Msg
    = RunsReceived RunsResponse
    | TimeZoneReceived Zone
    | Refresh
    | EventFormChange EventForm
    | EventFormSubmit
    | EventFormSubmitFinished (Result Http.Error (Maybe UserError))
    | EventDelete Int
    | EventDeleteFinished (Result Http.Error (Maybe UserError))


type alias EventForm =
    { userName : String
    , message : String
    }


emptyEventForm : EventForm
emptyEventForm =
    EventForm "P11User" ""


eventFormValid : EventForm -> Bool
eventFormValid { userName, message } =
    userName /= "" && message /= ""


type alias Model =
    { runs : RemoteData Http.Error RunsResponseContent
    , myTimeZone : Maybe Zone
    , refreshRequest : RemoteData Http.Error ()
    , eventForm : EventForm
    , eventRequest : RemoteData Http.Error ()
    }


init : () -> ( Model, Cmd Msg )
init _ =
    ( { runs = Loading
      , myTimeZone = Nothing
      , refreshRequest = NotAsked
      , eventForm = emptyEventForm
      , eventRequest = NotAsked
      }
    , Task.perform TimeZoneReceived here
    )


attributiColumnHeaders : List (Attributo AttributoType) -> List (Html msg)
attributiColumnHeaders =
    List.map (th_ << makeAttributoHeader) << List.filter (\a -> a.associatedTable == AssociatedTable.Run)


attributiColumns : Zone -> Dict Int String -> List (Attributo AttributoType) -> Run -> List (Html Msg)
attributiColumns zone sampleIds attributi run =
    List.map (makeAttributoCell { shortDateTime = True } zone sampleIds run.attributi) <| List.filter (\a -> a.associatedTable == AssociatedTable.Run) attributi


viewRunRow : Zone -> Dict Int String -> List (Attributo AttributoType) -> Run -> Html Msg
viewRunRow zone sampleIds attributi r =
    tr_ <| td_ [ text (fromInt r.id) ] :: attributiColumns zone sampleIds attributi r ++ [ td_ [] ]


viewEventRow : Zone -> Int -> Event -> Html Msg
viewEventRow zone attributoColumnCount e =
    tr [ class "bg-light" ]
        [ td_ []
        , td_ [ text <| formatPosixTimeOfDayHumanFriendly zone e.created ]
        , td [ colspan attributoColumnCount ]
            [ button [ class "btn btn-sm btn-link amarcord-small-link-button", type_ "button", onClick (EventDelete e.id) ] [ icon { name = "trash" } ]
            , strongText <| " [" ++ e.source ++ "] "
            , text <| e.text ++ " "
            ]
        ]


viewRunAndEventRows : Zone -> Dict Int String -> List (Attributo AttributoType) -> List Run -> List Event -> List (Html Msg)
viewRunAndEventRows zone sampleIds attributi runs events =
    case ( head runs, head events ) of
        -- No elements, neither runs nor events, left anymore
        ( Nothing, Nothing ) ->
            []

        -- Only runs left
        ( Just _, Nothing ) ->
            List.map (viewRunRow zone sampleIds attributi) runs

        -- Only events left
        ( Nothing, Just _ ) ->
            List.map (viewEventRow zone (List.length attributi)) events

        -- We have events and runs and have to compare the dates
        ( Just run, Just event ) ->
            case Maybe.andThen extractDateTime <| retrieveAttributoValue (toAttributoName "started") run.attributi of
                Just runStarted ->
                    if posixBefore event.created runStarted then
                        viewRunRow zone sampleIds attributi run :: viewRunAndEventRows zone sampleIds attributi (List.drop 1 runs) events

                    else
                        viewEventRow zone (List.length attributi) event :: viewRunAndEventRows zone sampleIds attributi runs (List.drop 1 events)

                -- We don't have a start time...take the run
                Nothing ->
                    viewRunRow zone sampleIds attributi run :: viewRunAndEventRows zone sampleIds attributi (List.drop 1 runs) events


viewRunsTable : Zone -> RunsResponseContent -> Html Msg
viewRunsTable zone { runs, attributi, events, samples } =
    let
        sampleIds : Dict.Dict Int String
        sampleIds =
            List.foldr (\s -> Dict.insert s.id s.name) Dict.empty samples

        runRows : List (Html Msg)
        runRows =
            viewRunAndEventRows zone sampleIds attributi runs events
    in
    table [ class "table amarcord-table-fix-head table-bordered table-hover" ]
        [ thead_
            [ tr [ class "align-top" ] <|
                th_ [ text "ID" ]
                    :: attributiColumnHeaders attributi
                    ++ [ th_ [ text "Actions" ] ]
            ]
        , tbody_ runRows
        ]


viewEventForm : RemoteData Http.Error () -> EventForm -> Html Msg
viewEventForm eventRequest { userName, message } =
    let
        eventError =
            case eventRequest of
                Success _ ->
                    makeAlert [ AlertSuccess, AlertSmall ] <| [ text "Message added!" ]

                Failure e ->
                    makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve samples" ] ] ++ showHttpError e

                _ ->
                    text ""
    in
    form [ class "col-6" ]
        [ h5_ [ text "Did something happen just now? Tell us!" ]
        , div
            [ class "mb-3 row" ]
            [ div [ class "col-sm" ]
                [ input_
                    [ value userName
                    , type_ "text"
                    , class "form-control form-control-sm"
                    , placeholder "User name"
                    , onInput (\e -> EventFormChange { userName = e, message = message })
                    ]
                ]
            , div [ class "col-sm-7" ]
                [ input_
                    [ value message
                    , type_ "text"
                    , class "form-control form-control-sm"
                    , placeholder "What happened? What did you do?"
                    , onEnter EventFormSubmit
                    , onInput (\e -> EventFormChange { userName = userName, message = e })
                    ]
                ]
            , div [ class "col-sm" ]
                [ button
                    [ onClick EventFormSubmit
                    , disabled (isLoading eventRequest || userName == "" || message == "")
                    , type_ "button"
                    , class "btn btn-primary btn-sm"
                    , style "white-space" "nowrap"
                    ]
                    [ icon { name = "plus" }, text " Add message" ]
                ]
            ]
        , eventError
        ]


viewInner : Zone -> RemoteData Http.Error () -> EventForm -> RunsResponseContent -> List (Html Msg)
viewInner zone eventRequest eventForm rrc =
    [ viewEventForm eventRequest eventForm, viewRunsTable zone rrc ]


view : Model -> Html Msg
view model =
    div [ class "container" ] <|
        case model.myTimeZone of
            Nothing ->
                [ spinner ]

            Just zone ->
                case model.runs of
                    NotAsked ->
                        List.singleton <| text "Impossible state reached: time zone, but no runs in progress?"

                    Loading ->
                        List.singleton <| loadingBar "Loading runs..."

                    Failure e ->
                        List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve runs" ] ] ++ showHttpError e

                    Success a ->
                        case model.refreshRequest of
                            Loading ->
                                viewInner zone model.eventRequest model.eventForm a ++ [ loadingBar "Refreshing..." ]

                            _ ->
                                viewInner zone model.eventRequest model.eventForm a


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
              }
            , Cmd.none
            )

        TimeZoneReceived zone ->
            ( { model | myTimeZone = Just zone }, httpGetRuns RunsReceived )

        Refresh ->
            ( { model | refreshRequest = Loading }, httpGetRuns RunsReceived )

        EventFormChange eventForm ->
            ( { model | eventForm = eventForm }, Cmd.none )

        EventFormSubmit ->
            if eventFormValid model.eventForm then
                ( { model | eventRequest = Loading }, httpCreateEvent model.eventForm.userName model.eventForm.message )

            else
                ( model, Cmd.none )

        EventFormSubmitFinished result ->
            case result of
                Err e ->
                    ( { model | eventRequest = Failure e }, Cmd.none )

                Ok (Just userError) ->
                    ( { model | eventRequest = Failure (Http.BadBody userError.title) }, Cmd.none )

                Ok Nothing ->
                    ( { model | eventRequest = Success (), eventForm = { userName = model.eventForm.userName, message = "" } }, httpGetRuns RunsReceived )

        EventDelete eventId ->
            ( model, httpDeleteEvent eventId )

        EventDeleteFinished result ->
            case result of
                Ok Nothing ->
                    ( model, httpGetRuns RunsReceived )

                _ ->
                    ( model, Cmd.none )
