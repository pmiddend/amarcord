module Amarcord.DataSet exposing (..)

import Amarcord.Attributo exposing (AttributoMap, AttributoValue)
import Dict exposing (Dict)


type alias DataSetSummary =
    { numberOfRuns : Int
    , hits : Int
    , frames : Int
    }


type alias DataSet =
    { id : Int
    , experimentType : String
    , attributi : AttributoMap AttributoValue
    , summary : Maybe DataSetSummary
    }
