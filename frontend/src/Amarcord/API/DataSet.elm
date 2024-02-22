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
    , detectorShiftX : Maybe Float
    , detectorShiftY : Maybe Float
    }


emptySummary : DataSetSummary
emptySummary =
    { hitRate = Nothing, indexingRate = Nothing, indexedFrames = 0, detectorShiftX = Nothing, detectorShiftY = Nothing }


type alias DataSet =
    { id : DataSetId
    , experimentTypeId : ExperimentTypeId
    , attributi : AttributoMap AttributoValue
    , summary : Maybe DataSetSummary
    }


dataSetSummaryDecoder : Decode.Decoder DataSetSummary
dataSetSummaryDecoder =
    Decode.map5
        DataSetSummary
        (Decode.field "hit_rate" (Decode.maybe Decode.float))
        (Decode.field "indexing_rate" (Decode.maybe Decode.float))
        (Decode.field "indexed_frames" Decode.int)
        (Decode.field "detector_shift_x_mm" (Decode.maybe Decode.float))
        (Decode.field "detector_shift_y_mm" (Decode.maybe Decode.float))


dataSetDecoder : Decode.Decoder DataSet
dataSetDecoder =
    Decode.map4
        DataSet
        (Decode.field "id" Decode.int)
        (Decode.field "experiment_type_id" Decode.int)
        (Decode.field "attributi" attributoMapDecoder)
        (Decode.maybe <| Decode.field "summary" dataSetSummaryDecoder)
