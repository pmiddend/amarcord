module Amarcord.RunOverview exposing (..)

import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue, attributoDecoder, attributoMapDecoder, attributoTypeDecoder, extractDateTime, makeAttributoCell, makeAttributoHeader, retrieveAttributoValue, toAttributoName)
import Amarcord.Bootstrap exposing (AlertType(..), loadingBar, makeAlert, showHttpError, spinner)
import Amarcord.File exposing (File, fileDecoder)
import Amarcord.Html exposing (strongText, tbody_, td_, th_, thead_, tr_)
import Amarcord.Sample exposing (Sample, sampleDecoder)
import Amarcord.Util exposing (formatPosixTimeOfDayHumanFriendly, posixBefore)
import Dict exposing (Dict)
import Html exposing (Html, div, h4, table, td, text, tr)
import Html.Attributes exposing (class, colspan)
import Http
import Iso8601 exposing (toTime)
import Json.Decode as Decode
import List exposing (head)
import Parser exposing (deadEndsToString)
import RemoteData exposing (RemoteData(..), fromResult)
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


type alias Model =
    { runs : RemoteData Http.Error RunsResponseContent
    , myTimeZone : Maybe Zone
    }


init : () -> ( Model, Cmd Msg )
init _ =
    ( { runs = Loading, myTimeZone = Nothing }, Task.perform TimeZoneReceived here )


attributiColumnHeaders : List (Attributo AttributoType) -> List (Html msg)
attributiColumnHeaders =
    List.map (th_ << makeAttributoHeader) << List.filter (\a -> a.associatedTable == AssociatedTable.Run)


attributiColumns : Zone -> Dict Int String -> List (Attributo AttributoType) -> Run -> List (Html Msg)
attributiColumns zone sampleIds attributi run =
    List.map (makeAttributoCell { shortDateTime = True } zone sampleIds run.attributi) <| List.filter (\a -> a.associatedTable == AssociatedTable.Run) attributi


makeRunRow : Zone -> Dict Int String -> List (Attributo AttributoType) -> Run -> Html Msg
makeRunRow zone sampleIds attributi r =
    tr_ <| td_ [ text (fromInt r.id) ] :: attributiColumns zone sampleIds attributi r ++ [ td_ [] ]


makeEventRow : Zone -> Int -> Event -> Html Msg
makeEventRow zone attributoColumnCount e =
    tr_ [ td_ [], td_ [ text <| formatPosixTimeOfDayHumanFriendly zone e.created ], td [ colspan attributoColumnCount ] [ strongText e.source, text ": ", text e.text ] ]


makeRunRows : Zone -> Dict Int String -> List (Attributo AttributoType) -> List Run -> List Event -> List (Html Msg)
makeRunRows zone sampleIds attributi runs events =
    case ( head runs, head events ) of
        -- No elements, neither runs nor events, left anymore
        ( Nothing, Nothing ) ->
            []

        -- Only runs left
        ( Just _, Nothing ) ->
            List.map (makeRunRow zone sampleIds attributi) runs

        -- Only events left
        ( Nothing, Just _ ) ->
            List.map (makeEventRow zone (List.length attributi)) events

        -- We have events and runs and have to compare the dates
        ( Just run, Just event ) ->
            case Maybe.andThen extractDateTime <| retrieveAttributoValue (toAttributoName "started") run.attributi of
                Just runStarted ->
                    if posixBefore runStarted event.created then
                        makeRunRow zone sampleIds attributi run :: makeRunRows zone sampleIds attributi (List.drop 1 runs) events

                    else
                        makeEventRow zone (List.length attributi) event :: makeRunRows zone sampleIds attributi runs (List.drop 1 events)

                -- We don't have a start time...take the run
                Nothing ->
                    makeRunRow zone sampleIds attributi run :: makeRunRows zone sampleIds attributi (List.drop 1 runs) events


viewRunsTable : Zone -> RunsResponseContent -> Html Msg
viewRunsTable zone { runs, attributi, events, samples } =
    let
        sampleIds : Dict.Dict Int String
        sampleIds =
            List.foldr (\s -> Dict.insert s.id s.name) Dict.empty samples

        runRows : List (Html Msg)
        runRows =
            makeRunRows zone sampleIds attributi runs events
    in
    table [ class "table table-striped table-sm amarcord-table-fix-head table-bordered" ]
        [ thead_
            [ tr [ class "align-top" ] <|
                th_ [ text "ID" ]
                    :: attributiColumnHeaders attributi
                    ++ [ th_ [ text "Actions" ] ]
            ]
        , tbody_ runRows
        ]


viewInner : Zone -> RunsResponseContent -> Html Msg
viewInner zone rrc =
    viewRunsTable zone rrc


view : Model -> Html Msg
view model =
    div [ class "container" ]
        [ case model.myTimeZone of
            Nothing ->
                spinner

            Just zone ->
                case model.runs of
                    NotAsked ->
                        text "Impossible state reached: time zone, but no runs in progress?"

                    Loading ->
                        loadingBar "Loading runs..."

                    Failure e ->
                        makeAlert AlertDanger <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve runs" ] ] ++ showHttpError e

                    Success a ->
                        viewInner zone a
        ]


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        RunsReceived response ->
            ( { model | runs = fromResult response }, Cmd.none )

        TimeZoneReceived zone ->
            ( { model | myTimeZone = Just zone }, httpGetRuns RunsReceived )
