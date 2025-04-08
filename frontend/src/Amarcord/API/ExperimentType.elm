module Amarcord.API.ExperimentType exposing (..)

import Amarcord.API.AttributoWithRole exposing (AttributoWithRole)
import Dict


type alias ExperimentTypeId =
    Int


type alias ExperimentType =
    { id : ExperimentTypeId
    , name : String
    , attributi : List AttributoWithRole
    }


experimentTypeIdDict : List ExperimentType -> Dict.Dict Int String
experimentTypeIdDict =
    List.foldr (\s -> Dict.insert s.id s.name) Dict.empty
