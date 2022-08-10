module Amarcord.DataSet exposing (..)

import Amarcord.Attributo exposing (AttributoMap, AttributoValue)


type alias DataSetSummary =
    { numberOfRuns : Int
    , frames : Int
    , hitRate : Maybe Float
    }


type alias DataSet =
    { id : Int
    , experimentType : String
    , attributi : AttributoMap AttributoValue
    , summary : Maybe DataSetSummary
    }
