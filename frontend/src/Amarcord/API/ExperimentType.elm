module Amarcord.API.ExperimentType exposing (..)

import Json.Decode as Decode


type alias ExperimentTypeId =
    Int


type alias ExperimentType =
    { id : ExperimentTypeId
    , name : String
    , attributiNames : List String
    }


experimentTypeDecoder : Decode.Decoder ExperimentType
experimentTypeDecoder =
    Decode.map3 ExperimentType
        (Decode.field "id" Decode.int)
        (Decode.field "name" Decode.string)
        (Decode.field "attributi-names" <| Decode.list Decode.string)
