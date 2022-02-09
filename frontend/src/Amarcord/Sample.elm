module Amarcord.Sample exposing (..)

import Amarcord.Attributo exposing (AttributoMap, AttributoValue, attributoMapDecoder, encodeAttributoMap)
import Amarcord.File exposing (File, fileDecoder)
import Dict
import Json.Decode as Decode
import Json.Encode as Encode
import Maybe.Extra exposing (unwrap)


type alias Sample idType attributiType fileType =
    { id : idType
    , name : String
    , attributi : attributiType
    , files : List fileType
    }


type alias SampleId =
    Int


sampleMapAttributi : (b -> c) -> Sample a b x -> Sample a c x
sampleMapAttributi f { id, name, attributi, files } =
    { id = id, name = name, attributi = f attributi, files = files }


sampleMapId : (a -> b) -> Sample a c x -> Sample b c x
sampleMapId f { id, name, attributi, files } =
    { id = f id, name = name, attributi = attributi, files = files }


sampleDecoder : Decode.Decoder (Sample SampleId (AttributoMap AttributoValue) File)
sampleDecoder =
    Decode.map4
        Sample
        (Decode.field "id" Decode.int)
        (Decode.field "name" Decode.string)
        (Decode.field "attributi" attributoMapDecoder)
        (Decode.field "files" (Decode.list fileDecoder))


encodeSample : Sample (Maybe Int) (AttributoMap AttributoValue) Int -> Encode.Value
encodeSample s =
    Encode.object <|
        [ ( "name", Encode.string s.name )
        , ( "attributi", encodeAttributoMap s.attributi )
        , ( "fileIds", Encode.list Encode.int s.files )
        ]
            ++ unwrap [] (\id -> [ ( "id", Encode.int id ) ]) s.id


sampleIdDict : List (Sample Int b c) -> Dict.Dict Int String
sampleIdDict =
    List.foldr (\s -> Dict.insert s.id s.name) Dict.empty
