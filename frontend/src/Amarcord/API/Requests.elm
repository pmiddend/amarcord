module Amarcord.API.Requests exposing (DataSetResult, Event, ExperimentType, Run, RunsResponse, RunsResponseContent, httpCreateDataSet, httpCreateEvent, httpCreateExperimentType, httpDeleteDataSet, httpDeleteEvent, httpDeleteExperimentType, httpGetDataSets, httpGetExperimentTypes, httpGetRuns, httpUpdateRun)

import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue, attributoDecoder, attributoMapDecoder, attributoTypeDecoder, encodeAttributoMap)
import Amarcord.DataSet exposing (DataSet, DataSetSummary)
import Amarcord.File exposing (File, fileDecoder)
import Amarcord.Sample exposing (Sample, SampleId, sampleDecoder)
import Amarcord.UserError exposing (UserError, userErrorDecoder)
import Amarcord.Util exposing (httpDelete, httpPatch)
import Http exposing (jsonBody)
import Json.Decode as Decode
import Json.Encode as Encode
import Time exposing (Posix, millisToPosix)


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


dataSetSummaryDecoder : Decode.Decoder DataSetSummary
dataSetSummaryDecoder =
    Decode.map3
        DataSetSummary
        (Decode.field "number-of-runs" Decode.int)
        (Decode.field "hits" Decode.int)
        (Decode.field "frames" Decode.int)


dataSetDecoder : Decode.Decoder DataSet
dataSetDecoder =
    Decode.map4
        DataSet
        (Decode.field "id" Decode.int)
        (Decode.field "experiment-type" Decode.string)
        (Decode.field "attributi" attributoMapDecoder)
        (Decode.maybe <| Decode.field "summary" dataSetSummaryDecoder)


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


type alias Run =
    { id : Int
    , attributi : AttributoMap AttributoValue
    , files : List File
    , dataSets : List Int
    }


type alias Event =
    { id : Int
    , text : String
    , source : String
    , level : String
    , created : Posix
    }


type alias RunsResponseContent =
    { runs : List Run
    , attributi : List (Attributo AttributoType)
    , events : List Event
    , samples : List (Sample Int (AttributoMap AttributoValue) File)
    , dataSets : List DataSet
    }


httpGetRuns : (RunsResponse -> msg) -> Cmd msg
httpGetRuns f =
    Http.get
        { url = "/api/runs"
        , expect =
            Http.expectJson f <|
                Decode.map5 RunsResponseContent
                    (Decode.field "runs" <| Decode.list runDecoder)
                    (Decode.field "attributi" <| Decode.list (attributoDecoder attributoTypeDecoder))
                    (Decode.field "events" <| Decode.list eventDecoder)
                    (Decode.field "samples" <| Decode.list sampleDecoder)
                    (Decode.field "data-sets" <| Decode.list dataSetDecoder)
        }


type alias RunsResponse =
    Result Http.Error RunsResponseContent


runDecoder : Decode.Decoder Run
runDecoder =
    Decode.map4
        Run
        (Decode.field "id" Decode.int)
        (Decode.field "attributi" attributoMapDecoder)
        (Decode.field "files" (Decode.list fileDecoder))
        (Decode.field "data-sets" (Decode.list Decode.int))


eventDecoder : Decode.Decoder Event
eventDecoder =
    Decode.map5
        Event
        (Decode.field "id" Decode.int)
        (Decode.field "text" Decode.string)
        (Decode.field "source" Decode.string)
        (Decode.field "level" Decode.string)
        (Decode.field "created" (Decode.map millisToPosix Decode.int))


encodeEvent : String.String -> String.String -> Encode.Value
encodeEvent source text =
    Encode.object [ ( "source", Encode.string source ), ( "text", Encode.string text ) ]


encodeRun : Run -> Encode.Value
encodeRun run =
    Encode.object [ ( "id", Encode.int run.id ), ( "attributi", encodeAttributoMap run.attributi ) ]


httpCreateEvent : (Result Http.Error (Maybe UserError) -> msg) -> String -> String -> Cmd msg
httpCreateEvent f source text =
    Http.post
        { url = "/api/events"
        , expect = Http.expectJson f (Decode.maybe (Decode.field "error" userErrorDecoder))
        , body = jsonBody (encodeEvent source text)
        }


httpUpdateRun : (Result Http.Error (Maybe UserError) -> msg) -> Run -> Cmd msg
httpUpdateRun f a =
    httpPatch
        { url = "/api/runs"
        , expect = Http.expectJson f (Decode.maybe (Decode.field "error" userErrorDecoder))
        , body = jsonBody (encodeRun a)
        }


httpDeleteEvent : (Result Http.Error (Maybe UserError) -> msg) -> Int -> Cmd msg
httpDeleteEvent f eventId =
    httpDelete
        { url = "/api/events"
        , body = jsonBody (Encode.object [ ( "id", Encode.int eventId ) ])
        , expect = Http.expectJson f (Decode.maybe (Decode.field "error" userErrorDecoder))
        }
