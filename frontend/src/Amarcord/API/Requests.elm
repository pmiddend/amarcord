module Amarcord.API.Requests exposing (DataSet, DataSetResult, ExperimentType, httpCreateDataSet, httpCreateExperimentType, httpDeleteDataSet, httpDeleteExperimentType, httpGetDataSets, httpGetExperimentTypes)

import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue, attributoDecoder, attributoMapDecoder, attributoTypeDecoder, encodeAttributoMap)
import Amarcord.File exposing (File)
import Amarcord.Sample exposing (Sample, SampleId, sampleDecoder)
import Amarcord.UserError exposing (UserError, userErrorDecoder)
import Amarcord.Util exposing (httpDelete)
import Http exposing (jsonBody)
import Json.Decode as Decode
import Json.Encode as Encode


userErrorToHttpError : { a | title : String } -> Http.Error
userErrorToHttpError =
    Http.BadBody << .title


userErrorToError : UserError -> Result Http.Error a
userErrorToError =
    Err << userErrorToHttpError


maybeErrorToResult : Result Http.Error (Maybe UserError) -> Result Http.Error ()
maybeErrorToResult x =
    case x of
        Ok (Just e) ->
            userErrorToError e

        Ok Nothing ->
            Ok ()

        Err error ->
            Err error


errorDecoder : Decode.Decoder UserError
errorDecoder =
    Decode.field "error" userErrorDecoder


maybeErrorDecoder : Decode.Decoder (Maybe UserError)
maybeErrorDecoder =
    Decode.maybe errorDecoder


valueOrError : Decode.Decoder value -> Decode.Decoder (Result UserError value)
valueOrError valueDecoder =
    Decode.oneOf [ Decode.map Err errorDecoder, Decode.map Ok valueDecoder ]


userErrorOrHttpError : Result Http.Error (Result UserError value) -> Result Http.Error value
userErrorOrHttpError x =
    case x of
        Err y ->
            Err y

        Ok (Err y) ->
            Err (userErrorToHttpError y)

        Ok (Ok y) ->
            Ok y


type alias ExperimentType =
    { name : String
    , attributeNames : List String
    }


experimentTypeDecoder : Decode.Decoder ExperimentType
experimentTypeDecoder =
    Decode.map2 ExperimentType (Decode.field "name" Decode.string) (Decode.field "attributo-names" <| Decode.list Decode.string)


httpCreateExperimentType : (Result Http.Error () -> msg) -> String -> List String -> Cmd msg
httpCreateExperimentType f name attributiNames =
    Http.post
        { url = "/api/experiment-types"
        , expect =
            Http.expectJson (f << maybeErrorToResult) maybeErrorDecoder
        , body =
            jsonBody
                (Encode.object
                    [ ( "name", Encode.string name )
                    , ( "attributi-names", Encode.list Encode.string attributiNames )
                    ]
                )
        }


httpDeleteExperimentType : (Result Http.Error () -> msg) -> String -> Cmd msg
httpDeleteExperimentType f experimentTypeName =
    httpDelete
        { url = "/api/experiment-types"
        , body = jsonBody (Encode.object [ ( "name", Encode.string experimentTypeName ) ])
        , expect = Http.expectJson (f << maybeErrorToResult) maybeErrorDecoder
        }


httpGetExperimentTypes : (Result Http.Error (List ExperimentType) -> msg) -> Cmd msg
httpGetExperimentTypes f =
    Http.get
        { url = "/api/experiment-types"
        , expect = Http.expectJson (f << userErrorOrHttpError) (valueOrError <| Decode.field "experiment-types" <| Decode.list experimentTypeDecoder)
        }


type alias DataSet =
    { id : Int
    , experimentType : String
    , attributi : AttributoMap AttributoValue
    }


dataSetDecoder : Decode.Decoder DataSet
dataSetDecoder =
    Decode.map3 DataSet (Decode.field "id" Decode.int) (Decode.field "experiment-type" Decode.string) (Decode.field "attributi" attributoMapDecoder)


httpCreateDataSet : (Result Http.Error () -> msg) -> String -> AttributoMap AttributoValue -> Cmd msg
httpCreateDataSet f experimentType attributi =
    Http.post
        { url = "/api/data-sets"
        , expect =
            Http.expectJson (f << maybeErrorToResult) maybeErrorDecoder
        , body =
            jsonBody
                (Encode.object
                    [ ( "experiment-type", Encode.string experimentType )
                    , ( "attributi", encodeAttributoMap attributi )
                    ]
                )
        }


httpDeleteDataSet : (Result Http.Error () -> msg) -> Int -> Cmd msg
httpDeleteDataSet f id =
    httpDelete
        { url = "/api/data-sets"
        , body = jsonBody (Encode.object [ ( "id", Encode.int id ) ])
        , expect = Http.expectJson (f << maybeErrorToResult) maybeErrorDecoder
        }


type alias DataSetResult =
    { dataSets : List DataSet
    , attributi : List (Attributo AttributoType)
    , samples : List (Sample SampleId (AttributoMap AttributoValue) File)
    , experimentTypes : List ExperimentType
    }


dataSetResultDecoder : Decode.Decoder DataSetResult
dataSetResultDecoder =
    Decode.map4
        DataSetResult
        (Decode.field "data-sets" <| Decode.list dataSetDecoder)
        (Decode.field "attributi" <| Decode.list (attributoDecoder attributoTypeDecoder))
        (Decode.field "samples" <| Decode.list sampleDecoder)
        (Decode.field "experiment-types" <| Decode.list experimentTypeDecoder)


httpGetDataSets : (Result Http.Error DataSetResult -> msg) -> Cmd msg
httpGetDataSets f =
    Http.get
        { url = "/api/data-sets"
        , expect = Http.expectJson (f << userErrorOrHttpError) (valueOrError <| dataSetResultDecoder)
        }
