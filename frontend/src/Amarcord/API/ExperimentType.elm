module Amarcord.API.ExperimentType exposing (..)

import Amarcord.API.AttributoWithRole exposing (AttributoWithRole, attributoWithRoleDecoder)
import Dict
import Json.Decode as Decode


type alias ExperimentTypeId =
    Int


type alias ExperimentType =
    { id : ExperimentTypeId
    , name : String
    , attributi : List AttributoWithRole
    }


experimentTypeDecoder : Decode.Decoder ExperimentType
experimentTypeDecoder =
    Decode.map3 ExperimentType
        (Decode.field "id" Decode.int)
        (Decode.field "name" Decode.string)
        (Decode.field "attributi" <| Decode.list attributoWithRoleDecoder)


experimentTypeIdDict : List ExperimentType -> Dict.Dict Int String
experimentTypeIdDict =
    List.foldr (\s -> Dict.insert s.id s.name) Dict.empty
