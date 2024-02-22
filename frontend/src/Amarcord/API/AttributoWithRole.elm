module Amarcord.API.AttributoWithRole exposing (..)

import Amarcord.Chemical exposing (chemicalTypeDecoder, chemicalTypeFromApi, encodeChemicalType)
import Api.Data exposing (ChemicalType, JsonAttributiIdAndRole)
import Json.Decode as Decode
import Json.Encode as Encode


type alias AttributoWithRole =
    { id : Int
    , role : ChemicalType
    }


encodeAttributoWithRole : AttributoWithRole -> Encode.Value
encodeAttributoWithRole { id, role } =
    Encode.object
        [ ( "id", Encode.int id )
        , ( "role", encodeChemicalType role )
        ]


attributoWithRoleDecoder : Decode.Decoder AttributoWithRole
attributoWithRoleDecoder =
    Decode.map2
        AttributoWithRole
        (Decode.field "id" Decode.int)
        (Decode.field "role" chemicalTypeDecoder)


convertAttributoWithRoleFromApi : JsonAttributiIdAndRole -> AttributoWithRole
convertAttributoWithRoleFromApi x =
    { id = x.id
    , role = chemicalTypeFromApi x.role
    }
