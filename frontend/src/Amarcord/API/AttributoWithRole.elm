module Amarcord.API.AttributoWithRole exposing (..)

import Amarcord.Chemical exposing (ChemicalType, chemicalTypeDecoder, encodeChemicalType)
import Json.Decode as Decode
import Json.Encode as Encode


type alias AttributoWithRole =
    { name : String
    , role : ChemicalType
    }


encodeAttributoWithRole : AttributoWithRole -> Encode.Value
encodeAttributoWithRole { name, role } =
    Encode.object
        [ ( "name", Encode.string name )
        , ( "role", encodeChemicalType role )
        ]


attributoWithRoleDecoder : Decode.Decoder AttributoWithRole
attributoWithRoleDecoder =
    Decode.map2
        AttributoWithRole
        (Decode.field "name" Decode.string)
        (Decode.field "role" chemicalTypeDecoder)
