module Amarcord.DataSet exposing (..)

import Amarcord.Attributo exposing (AttributoMap, AttributoValue)


type alias DataSetSummary =
    { hitRate : Maybe Float
    , indexingRate : Maybe Float
    , indexedFrames : Int
    }


emptySummary : DataSetSummary
emptySummary =
    { hitRate = Nothing, indexingRate = Nothing, indexedFrames = 0 }


type alias DataSet =
    { id : Int
    , experimentType : String
    , attributi : AttributoMap AttributoValue
    , summary : Maybe DataSetSummary
    }
