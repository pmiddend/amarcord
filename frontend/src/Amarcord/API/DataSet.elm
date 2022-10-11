module Amarcord.API.DataSet exposing (..)

import Amarcord.API.ExperimentType exposing (ExperimentTypeId)
import Amarcord.Attributo exposing (AttributoMap, AttributoValue, attributoMapDecoder)
import Json.Decode as Decode


type alias DataSetId =
    Int


type alias DataSetSummary =
    { hitRate : Maybe Float
    , indexingRate : Maybe Float
    , indexedFrames : Int
    }


emptySummary : DataSetSummary
emptySummary =
    { hitRate = Nothing, indexingRate = Nothing, indexedFrames = 0 }


type alias DataSet =
    { id : DataSetId
    , experimentTypeId : ExperimentTypeId
    , attributi : AttributoMap AttributoValue
    , summary : Maybe DataSetSummary
    }


dataSetSummaryDecoder : Decode.Decoder DataSetSummary
dataSetSummaryDecoder =
    Decode.map3
        DataSetSummary
        (Decode.field "hit-rate" (Decode.maybe Decode.float))
        (Decode.field "indexing-rate" (Decode.maybe Decode.float))
        (Decode.field "indexed-frames" Decode.int)


dataSetDecoder : Decode.Decoder DataSet
dataSetDecoder =
    Decode.map4
        DataSet
        (Decode.field "id" Decode.int)
        (Decode.field "experiment-type-id" Decode.int)
        (Decode.field "attributi" attributoMapDecoder)
        (Decode.maybe <| Decode.field "summary" dataSetSummaryDecoder)
