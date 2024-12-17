module Amarcord.API.AttributoWithRole exposing (..)

import Api.Data exposing (ChemicalType)


type alias AttributoWithRole =
    { id : Int
    , role : ChemicalType
    }
