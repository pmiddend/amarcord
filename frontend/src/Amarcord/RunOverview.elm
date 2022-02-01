module Amarcord.RunOverview exposing (..)

import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue, attributoDecoder, attributoMapDecoder, attributoTypeDecoder)
import Amarcord.File exposing (File, fileDecoder)
import Html exposing (text)
import Http
import Json.Decode as Decode


type alias Run =
    { id : Int
    , attributi : AttributoMap AttributoValue
    , files : List File
    }


type alias RunsResponse =
    Result Http.Error ( List Run, List (Attributo AttributoType) )


runDecoder : Decode.Decoder Run
runDecoder =
    Decode.map3
        Run
        (Decode.field "id" Decode.int)
        (Decode.field "attributi" attributoMapDecoder)
        (Decode.field "files" (Decode.list fileDecoder))


httpGetRuns : (RunsResponse -> msg) -> Cmd msg
httpGetRuns f =
    Http.get
        { url = "/api/runs"
        , expect =
            Http.expectJson f <|
                Decode.map2 (\runs attributi -> ( runs, attributi ))
                    (Decode.field "runs" <| Decode.list runDecoder)
                    (Decode.field "attributi" <| Decode.list (attributoDecoder attributoTypeDecoder))
        }


type Msg
    = RunsReceived RunsResponse


type alias Model =
    {}


init _ =
    ( {}, Cmd.none )


view model =
    text ""


update msg model =
    ( model, Cmd.none )
