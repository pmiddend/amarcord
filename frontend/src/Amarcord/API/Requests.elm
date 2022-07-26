module Amarcord.API.Requests exposing
    ( AnalysisResultsExperimentType
    , AnalysisResultsRoot
    , AppConfig
    , CfelAnalysisResult
    , ConversionFlags
    , DataSetResult
    , Event
    , ExperimentType
    , ExperimentTypesResponse
    , IncludeLiveStream(..)
    , LatestDark
    , RequestError(..)
    , Run
    , RunEventDate
    , RunEventDateFilter(..)
    , RunFilter(..)
    , RunsBulkGetResponse
    , RunsResponse
    , RunsResponseContent
    , SamplesResponse
    , StandardUnitCheckResult(..)
    , emptyRunEventDateFilter
    , emptyRunFilter
    , httpChangeCurrentExperimentType
    , httpCheckStandardUnit
    , httpCreateAttributo
    , httpCreateDataSet
    , httpCreateDataSetFromRun
    , httpCreateEvent
    , httpCreateExperimentType
    , httpCreateFile
    , httpCreateLiveStreamSnapshot
    , httpCreateSample
    , httpDeleteAttributo
    , httpDeleteDataSet
    , httpDeleteEvent
    , httpDeleteExperimentType
    , httpDeleteSample
    , httpEditAttributo
    , httpGetAnalysisResults
    , httpGetAndDecodeAttributi
    , httpGetConfig
    , httpGetDataSets
    , httpGetExperimentTypes
    , httpGetRunsBulk
    , httpGetRunsFilter
    , httpGetSamples
    , httpStartRun
    , httpStopRun
    , httpUpdateRun
    , httpUpdateRunsBulk
    , httpUpdateSample
    , httpUserConfigurationSetAutoPilot
    , runEventDateFilter
    , runEventDateToString
    , runFilterToString
    , specificRunEventDateFilter
    )

import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoName, AttributoType, AttributoValue(..), attributoDecoder, attributoTypeDecoder)
import Amarcord.DataSet exposing (DataSet, DataSetSummary)
import Amarcord.File exposing (File)
import Amarcord.JsonSchema exposing (JsonSchema, encodeJsonSchema)
import Amarcord.Sample exposing (Sample, SampleId)
import Amarcord.UserError exposing (CustomError, customErrorDecoder)
import Amarcord.Util exposing (httpDelete, httpPatch)
import Dict exposing (Dict)
import File as ElmFile
import Http exposing (emptyBody, filePart, jsonBody, multipartBody, stringPart)
import Json.Decode as Decode
import Json.Decode.Pipeline exposing (required)
import Json.Encode as Encode
import Maybe.Extra as MaybeExtra exposing (unwrap)
import Set exposing (Set)
import Time exposing (Posix, millisToPosix)
import Tuple exposing (pair)


type alias ConversionFlags =
    { ignoreUnits : Bool
    }


errorDecoder : Decode.Decoder CustomError
errorDecoder =
    Decode.field "error" customErrorDecoder


valueOrError : Decode.Decoder value -> Decode.Decoder (Result CustomError value)
valueOrError valueDecoder =
    Decode.oneOf [ Decode.map Err errorDecoder, Decode.map Ok valueDecoder ]


type alias ExperimentType =
    { name : String
    , attributeNames : List String
    }


experimentTypeDecoder : Decode.Decoder ExperimentType
experimentTypeDecoder =
    Decode.map2 ExperimentType (Decode.field "name" Decode.string) (Decode.field "attributo-names" <| Decode.list Decode.string)


httpCreateExperimentType : (Result RequestError () -> msg) -> String -> List String -> Cmd msg
httpCreateExperimentType f name attributiNames =
    Http.post
        { url = "api/experiment-types"
        , expect =
            Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body =
            jsonBody
                (Encode.object
                    [ ( "name", Encode.string name )
                    , ( "attributi-names", Encode.list Encode.string attributiNames )
                    ]
                )
        }


httpDeleteExperimentType : (Result RequestError () -> msg) -> String -> Cmd msg
httpDeleteExperimentType f experimentTypeName =
    httpDelete
        { url = "api/experiment-types"
        , body = jsonBody (Encode.object [ ( "name", Encode.string experimentTypeName ) ])
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        }


type alias ExperimentTypesResponse =
    { experimentTypes : List ExperimentType
    , attributi : List (Attributo AttributoType)
    }


httpGetExperimentTypes : (Result RequestError ExperimentTypesResponse -> msg) -> Cmd msg
httpGetExperimentTypes f =
    Http.get
        { url = "api/experiment-types"
        , expect =
            Http.expectJson (f << httpResultToRequestError)
                (valueOrError <|
                    Decode.map2
                        ExperimentTypesResponse
                        (Decode.field "experiment-types" <| Decode.list experimentTypeDecoder)
                        (Decode.field "attributi" (Decode.list (attributoDecoder attributoTypeDecoder)))
                )
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


httpCreateDataSet : (Result RequestError () -> msg) -> String -> AttributoMap AttributoValue -> Cmd msg
httpCreateDataSet f experimentType attributi =
    Http.post
        { url = "api/data-sets"
        , expect =
            Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body =
            jsonBody
                (Encode.object
                    [ ( "experiment-type", Encode.string experimentType )
                    , ( "attributi", encodeAttributoMap attributi )
                    ]
                )
        }


httpCreateDataSetFromRun : (Result RequestError () -> msg) -> String -> Int -> Cmd msg
httpCreateDataSetFromRun f experimentType runId =
    Http.post
        { url = "api/data-sets/from-run"
        , expect =
            Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body =
            jsonBody
                (Encode.object
                    [ ( "experiment-type", Encode.string experimentType )
                    , ( "run-id", Encode.int runId )
                    ]
                )
        }


httpChangeCurrentExperimentType : (Result RequestError () -> msg) -> Maybe String -> Int -> Cmd msg
httpChangeCurrentExperimentType f experimentType runId =
    Http.post
        { url = "api/experiment-types/change-for-run"
        , expect =
            Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body =
            jsonBody
                (Encode.object
                    [ ( "experiment-type", MaybeExtra.unwrap Encode.null Encode.string experimentType )
                    , ( "run-id", Encode.int runId )
                    ]
                )
        }


httpDeleteDataSet : (Result RequestError () -> msg) -> Int -> Cmd msg
httpDeleteDataSet f id =
    httpDelete
        { url = "api/data-sets"
        , body = jsonBody (Encode.object [ ( "id", Encode.int id ) ])
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        }


type alias DataSetResult =
    { dataSets : List DataSet
    , attributi : List (Attributo AttributoType)
    , samples : List (Sample SampleId (AttributoMap AttributoValue) File)
    , experimentTypes : List ExperimentType
    }


type alias CfelAnalysisResult =
    { id : Int
    , directoryName : String
    , dataSetId : Int
    , resolution : String
    , rsplit : Float
    , cchalf : Float
    , ccstar : Float
    , snr : Float
    , completeness : Float
    , multiplicity : Float
    , totalMeasurements : Int
    , uniqueReflections : Int
    , numPatterns : Int
    , numHits : Int
    , indexedPatterns : Int
    , indexedCrystals : Int
    , crystfelVersion : String
    , ccstarRSplit : Float
    , created : Posix
    , files : List File
    }


cfelAnalysisDecoder : Decode.Decoder CfelAnalysisResult
cfelAnalysisDecoder =
    Decode.succeed CfelAnalysisResult
        |> required "id" Decode.int
        |> required "directoryName" Decode.string
        |> required "dataSetId" Decode.int
        |> required "resolution" Decode.string
        |> required "rsplit" Decode.float
        |> required "cchalf" Decode.float
        |> required "ccstar" Decode.float
        |> required "snr" Decode.float
        |> required "completeness" Decode.float
        |> required "multiplicity" Decode.float
        |> required "totalMeasurements" Decode.int
        |> required "uniqueReflections" Decode.int
        |> required "numPatterns" Decode.int
        |> required "numHits" Decode.int
        |> required "indexedPatterns" Decode.int
        |> required "indexedCrystals" Decode.int
        |> required "crystfelVersion" Decode.string
        |> required "ccstarRSplit" Decode.float
        |> required "created" decodePosix
        |> required "files" (Decode.list fileDecoder)


type alias AnalysisResultsExperimentType =
    { dataSet : DataSet
    , analysisResults : List CfelAnalysisResult
    , runs : List String
    }


type alias AnalysisResultsRoot =
    { experimentTypes : Dict String (List AnalysisResultsExperimentType)
    , attributi : List (Attributo AttributoType)
    , sampleIdToName : Dict Int String
    }


analysisResultsExperimentTypeDecoder : Decode.Decoder AnalysisResultsExperimentType
analysisResultsExperimentTypeDecoder =
    Decode.map3
        AnalysisResultsExperimentType
        (Decode.field "data-set" dataSetDecoder)
        (Decode.field "analysis-results" (Decode.list cfelAnalysisDecoder))
        (Decode.field "runs" (Decode.list Decode.string))


analysisResultsRootDecoder : Decode.Decoder AnalysisResultsRoot
analysisResultsRootDecoder =
    Decode.map3 AnalysisResultsRoot
        (Decode.field "experiment-types" <| Decode.dict (Decode.list analysisResultsExperimentTypeDecoder))
        (Decode.field "attributi" <| Decode.list (attributoDecoder attributoTypeDecoder))
        (Decode.field "sample-id-to-name" <|
            Decode.map Dict.fromList <|
                Decode.list <|
                    Decode.map2 pair (Decode.index 0 Decode.int) (Decode.index 1 Decode.string)
        )


type RequestError
    = HttpError Http.Error
    | UserError CustomError


httpResultToRequestError : Result Http.Error (Result CustomError value) -> Result RequestError value
httpResultToRequestError x =
    case x of
        Err y ->
            Err (HttpError y)

        Ok (Err y) ->
            Err (UserError y)

        Ok (Ok y) ->
            Ok y


httpGetAnalysisResults : (Result RequestError AnalysisResultsRoot -> msg) -> Cmd msg
httpGetAnalysisResults f =
    Http.get
        { url = "api/analysis/analysis-results"
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| analysisResultsRootDecoder)
        }


dataSetResultDecoder : Decode.Decoder DataSetResult
dataSetResultDecoder =
    Decode.map4
        DataSetResult
        (Decode.field "data-sets" <| Decode.list dataSetDecoder)
        (Decode.field "attributi" <| Decode.list (attributoDecoder attributoTypeDecoder))
        (Decode.field "samples" <| Decode.list sampleDecoder)
        (Decode.field "experiment-types" <| Decode.list experimentTypeDecoder)


httpGetDataSets : (Result RequestError DataSetResult -> msg) -> Cmd msg
httpGetDataSets f =
    Http.get
        { url = "api/data-sets"
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| dataSetResultDecoder)
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
    , files : List File
    }


type alias LatestDark =
    { id : Int
    , started : Posix
    }


latestDarkDecoder : Decode.Decoder LatestDark
latestDarkDecoder =
    Decode.map2 LatestDark (Decode.field "id" Decode.int) (Decode.field "started" decodePosix)


type alias RunsResponseContent =
    { runs : List Run
    , runsDates : List RunEventDate
    , latestDark : Maybe LatestDark
    , attributi : List (Attributo AttributoType)
    , events : List Event
    , samples : List (Sample Int (AttributoMap AttributoValue) File)
    , dataSets : List DataSet
    , experimentTypes : Dict String (Set String)
    , runInformationCopy : Bool
    , jetStreamFileId : Maybe Int
    }


httpUserConfigurationSetAutoPilot : (Result RequestError Bool -> msg) -> Bool -> Cmd msg
httpUserConfigurationSetAutoPilot f autoPilot =
    httpPatch
        { url =
            "api/user-config/auto-pilot/"
                ++ (if autoPilot then
                        "True"

                    else
                        "False"
                   )
        , expect =
            Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.field "value" Decode.bool)
        , body = emptyBody
        }


httpCreateLiveStreamSnapshot : (Result RequestError File -> msg) -> Cmd msg
httpCreateLiveStreamSnapshot f =
    Http.get
        { url = "api/live-stream/snapshot"
        , expect =
            Http.expectJson (f << httpResultToRequestError) <|
                valueOrError <|
                    fileDecoder
        }


type RunFilter
    = RunFilter String


emptyRunFilter : RunFilter
emptyRunFilter =
    RunFilter ""


runFilterToString : RunFilter -> String
runFilterToString (RunFilter s) =
    s


type RunEventDate
    = RunEventDate String


type RunEventDateFilter
    = RunEventDateFilter (Maybe RunEventDate)


emptyRunEventDateFilter : RunEventDateFilter
emptyRunEventDateFilter =
    RunEventDateFilter Nothing


specificRunEventDateFilter : RunEventDate -> RunEventDateFilter
specificRunEventDateFilter rd =
    RunEventDateFilter (Just rd)


runEventDateFilter : RunEventDateFilter -> Maybe RunEventDate
runEventDateFilter (RunEventDateFilter rdf) =
    case rdf of
        Nothing ->
            Nothing

        Just rd ->
            Just rd


maybeRunEventDateToString : Maybe RunEventDate -> String
maybeRunEventDateToString rd =
    case rd of
        Nothing ->
            ""

        Just (RunEventDate s) ->
            s


runEventDateToString : RunEventDate -> String
runEventDateToString (RunEventDate s) =
    s


getRuns : String -> (RunsResponse -> msg) -> Cmd msg
getRuns path f =
    Http.get
        { url = path
        , expect =
            Http.expectJson (f << httpResultToRequestError) <|
                valueOrError
                    (Decode.succeed RunsResponseContent
                        |> required "runs" (Decode.list runDecoder)
                        |> required "filter-dates" (Decode.list decodeRunEventDate)
                        |> required "latest-dark" (Decode.maybe latestDarkDecoder)
                        |> required "attributi" (Decode.list (attributoDecoder attributoTypeDecoder))
                        |> required "events" (Decode.list eventDecoder)
                        |> required "samples" (Decode.list sampleDecoder)
                        |> required "data-sets" (Decode.list dataSetDecoder)
                        |> required "experiment-types" (Decode.dict (Decode.map Set.fromList <| Decode.list Decode.string))
                        |> required "auto-pilot" Decode.bool
                        |> required "live-stream-file-id" (Decode.maybe Decode.int)
                    )
        }


httpGetRunsFilter : RunFilter -> RunEventDateFilter -> (RunsResponse -> msg) -> Cmd msg
httpGetRunsFilter (RunFilter filter) redf f =
    getRuns ("api/runs?filter=" ++ filter ++ "&date=" ++ maybeRunEventDateToString (runEventDateFilter redf)) f


type alias RunsResponse =
    Result RequestError RunsResponseContent


encodeAttributoMap : AttributoMap AttributoValue -> Encode.Value
encodeAttributoMap =
    Encode.dict identity encodeAttributoValue


encodeAttributoValue : AttributoValue -> Encode.Value
encodeAttributoValue x =
    case x of
        ValueNone ->
            Encode.null

        ValueInt int ->
            Encode.int int

        ValueString string ->
            Encode.string string

        ValueList attributoValues ->
            Encode.list encodeAttributoValue attributoValues

        ValueNumber float ->
            Encode.float float

        ValueBoolean bool ->
            Encode.bool bool


attributoValueDecoder : Decode.Decoder AttributoValue
attributoValueDecoder =
    Decode.oneOf
        [ Decode.map ValueString Decode.string
        , Decode.map ValueInt Decode.int
        , Decode.map ValueNumber Decode.float
        , Decode.map ValueBoolean Decode.bool
        , Decode.null ValueNone
        , Decode.map ValueList (Decode.list (Decode.lazy (\_ -> attributoValueDecoder)))
        ]


attributoMapDecoder : Decode.Decoder (AttributoMap AttributoValue)
attributoMapDecoder =
    Decode.dict attributoValueDecoder


runDecoder : Decode.Decoder Run
runDecoder =
    Decode.map4
        Run
        (Decode.field "id" Decode.int)
        (Decode.field "attributi" attributoMapDecoder)
        (Decode.field "files" (Decode.list fileDecoder))
        (Decode.field "data-sets" (Decode.list Decode.int))


decodePosix : Decode.Decoder Posix
decodePosix =
    Decode.map millisToPosix Decode.int


decodeRunEventDate : Decode.Decoder RunEventDate
decodeRunEventDate =
    Decode.map RunEventDate Decode.string


eventDecoder : Decode.Decoder Event
eventDecoder =
    Decode.map6
        Event
        (Decode.field "id" Decode.int)
        (Decode.field "text" Decode.string)
        (Decode.field "source" Decode.string)
        (Decode.field "level" Decode.string)
        (Decode.field "created" decodePosix)
        (Decode.field "files" (Decode.list fileDecoder))


encodeEvent : String -> String -> List Int -> Encode.Value
encodeEvent source text fileIds =
    Encode.object [ ( "source", Encode.string source ), ( "text", Encode.string text ), ( "fileIds", Encode.list Encode.int fileIds ) ]


encodeRun : Run -> Encode.Value
encodeRun run =
    Encode.object [ ( "id", Encode.int run.id ), ( "attributi", encodeAttributoMap run.attributi ) ]


type IncludeLiveStream
    = NoLiveStream
    | WithLiveStream


includeLiveStreamBool : IncludeLiveStream -> Bool
includeLiveStreamBool x =
    case x of
        NoLiveStream ->
            False

        _ ->
            True


httpCreateEvent : (Result RequestError () -> msg) -> IncludeLiveStream -> String -> String -> List Int -> Cmd msg
httpCreateEvent f includeLiveStream source text files =
    Http.post
        { url = "api/events"
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body =
            jsonBody
                (Encode.object
                    [ ( "withLiveStream", Encode.bool <| includeLiveStreamBool includeLiveStream )
                    , ( "event", encodeEvent source text files )
                    ]
                )
        }


httpUpdateRun : (Result RequestError () -> msg) -> Run -> Cmd msg
httpUpdateRun f a =
    httpPatch
        { url = "api/runs"
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body = jsonBody (encodeRun a)
        }


type alias RunsBulkUpdateRequest =
    { runIds : List Int
    , attributi : AttributoMap AttributoValue
    }


encodeRunsBulkUpdateRequest : RunsBulkUpdateRequest -> Encode.Value
encodeRunsBulkUpdateRequest bu =
    Encode.object
        [ ( "run-ids", Encode.list Encode.int bu.runIds )
        , ( "attributi", encodeAttributoMap bu.attributi )
        ]


httpUpdateRunsBulk : (Result RequestError () -> msg) -> RunsBulkUpdateRequest -> Cmd msg
httpUpdateRunsBulk f a =
    httpPatch
        { url = "api/runs/bulk"
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body = jsonBody (encodeRunsBulkUpdateRequest a)
        }


type alias RunsBulkGetRequest =
    { runIds : List Int }


encodeRunsBulkGetRequest : RunsBulkGetRequest -> Encode.Value
encodeRunsBulkGetRequest bg =
    Encode.object
        [ ( "run-ids", Encode.list Encode.int bg.runIds )
        ]


type alias RunsBulkGetResponse =
    { attributiMap : AttributoMap (List AttributoValue)
    , attributi : List (Attributo AttributoType)
    , samples : List (Sample SampleId (AttributoMap AttributoValue) File)
    }


httpGetRunsBulk : (Result RequestError RunsBulkGetResponse -> msg) -> RunsBulkGetRequest -> Cmd msg
httpGetRunsBulk f a =
    Http.post
        { url = "api/runs/bulk"
        , body = jsonBody (encodeRunsBulkGetRequest a)
        , expect =
            Http.expectJson (f << httpResultToRequestError) <|
                valueOrError <|
                    Decode.map3 RunsBulkGetResponse
                        (Decode.field "attributi-map" <| Decode.dict (Decode.list attributoValueDecoder))
                        (Decode.field "attributi" <| Decode.list (attributoDecoder attributoTypeDecoder))
                        (Decode.field "samples" <| Decode.list sampleDecoder)
        }


httpDeleteEvent : (Result RequestError () -> msg) -> Int -> Cmd msg
httpDeleteEvent f eventId =
    httpDelete
        { url = "api/events"
        , body = jsonBody (Encode.object [ ( "id", Encode.int eventId ) ])
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        }


attributoRequestDecoder : Decode.Decoder (List (Attributo AttributoType))
attributoRequestDecoder =
    Decode.field "attributi" (Decode.list (attributoDecoder attributoTypeDecoder))


httpGetAndDecodeAttributi : (Result RequestError (List (Attributo AttributoType)) -> msg) -> Cmd msg
httpGetAndDecodeAttributi f =
    Http.get
        { url = "api/attributi"
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| attributoRequestDecoder)
        }


httpDeleteAttributo : (Result RequestError () -> msg) -> AttributoName -> Cmd msg
httpDeleteAttributo f attributoName =
    httpDelete
        { url = "api/attributi"
        , body = jsonBody (Encode.object [ ( "name", Encode.string attributoName ) ])
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        }


encodeConversionFlags : ConversionFlags -> Encode.Value
encodeConversionFlags { ignoreUnits } =
    Encode.object [ ( "ignoreUnits", Encode.bool ignoreUnits ) ]


httpCreateAttributo : (Result RequestError () -> msg) -> Attributo JsonSchema -> Cmd msg
httpCreateAttributo f a =
    Http.post
        { url = "api/attributi"
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body = jsonBody (encodeAttributo encodeJsonSchema a)
        }


type StandardUnitCheckResult
    = StandardUnitValid { input : String, normalized : String }
    | StandardUnitInvalid { input : String, error : String }


httpCheckStandardUnit : (Result RequestError StandardUnitCheckResult -> msg) -> String -> Cmd msg
httpCheckStandardUnit f unit =
    let
        decodeCheckUnitResult input normalized error =
            case normalized of
                Nothing ->
                    StandardUnitInvalid { input = input, error = Maybe.withDefault "unknown error" error }

                Just normalizedReal ->
                    StandardUnitValid { input = input, normalized = normalizedReal }
    in
    Http.post
        { url = "api/unit"
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.map3 decodeCheckUnitResult (Decode.field "input" Decode.string) (Decode.maybe (Decode.field "normalized" Decode.string)) (Decode.maybe (Decode.field "error" Decode.string)))
        , body = jsonBody (Encode.object [ ( "input", Encode.string unit ) ])
        }


type alias AppConfig =
    { title : String
    }


configDecoder : Decode.Decoder AppConfig
configDecoder =
    Decode.map AppConfig (Decode.field "title" Decode.string)


httpGetConfig : (Result RequestError AppConfig -> msg) -> Cmd msg
httpGetConfig f =
    Http.get
        { url = "api/config"
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| configDecoder)
        }


httpEditAttributo : (Result RequestError () -> msg) -> ConversionFlags -> AttributoName -> Attributo JsonSchema -> Cmd msg
httpEditAttributo f conversionFlags nameBefore a =
    httpPatch
        { url = "api/attributi"
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body =
            jsonBody
                (Encode.object
                    [ ( "newAttributo", encodeAttributo encodeJsonSchema a )
                    , ( "nameBefore", Encode.string nameBefore )
                    , ( "conversionFlags", encodeConversionFlags conversionFlags )
                    ]
                )
        }


encodeAttributo : (a -> Encode.Value) -> Attributo a -> Encode.Value
encodeAttributo typeEncoder a =
    Encode.object
        [ ( "name", Encode.string a.name )
        , ( "description", Encode.string a.description )
        , ( "group", Encode.string a.group )
        , ( "associatedTable", encodeAssociatedTable a.associatedTable )
        , ( "type", typeEncoder a.type_ )
        ]


encodeAssociatedTable : AssociatedTable.AssociatedTable -> Encode.Value
encodeAssociatedTable x =
    case x of
        AssociatedTable.Run ->
            Encode.string "run"

        AssociatedTable.Sample ->
            Encode.string "sample"


sampleDecoder : Decode.Decoder (Sample SampleId (AttributoMap AttributoValue) File)
sampleDecoder =
    Decode.map4
        Sample
        (Decode.field "id" Decode.int)
        (Decode.field "name" Decode.string)
        (Decode.field "attributi" attributoMapDecoder)
        (Decode.field "files" (Decode.list fileDecoder))


encodeSample : Sample (Maybe Int) (AttributoMap AttributoValue) Int -> Encode.Value
encodeSample s =
    Encode.object <|
        [ ( "name", Encode.string s.name )
        , ( "attributi", encodeAttributoMap s.attributi )
        , ( "fileIds", Encode.list Encode.int s.files )
        ]
            ++ unwrap [] (\id -> [ ( "id", Encode.int id ) ]) s.id


type alias SamplesResponse =
    Result RequestError ( List (Sample SampleId (AttributoMap AttributoValue) File), List (Attributo AttributoType) )


httpGetSamples : (SamplesResponse -> msg) -> Cmd msg
httpGetSamples f =
    Http.get
        { url = "api/samples"
        , expect =
            Http.expectJson (f << httpResultToRequestError) <|
                valueOrError <|
                    Decode.map2 (\samples attributi -> ( samples, attributi ))
                        (Decode.field "samples" <| Decode.list sampleDecoder)
                        (Decode.field "attributi" <| Decode.list (attributoDecoder attributoTypeDecoder))
        }


httpCreateSample : (Result RequestError () -> msg) -> Sample (Maybe Int) (AttributoMap AttributoValue) Int -> Cmd msg
httpCreateSample f a =
    Http.post
        { url = "api/samples"
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body = jsonBody (encodeSample a)
        }


httpUpdateSample : (Result RequestError () -> msg) -> Sample (Maybe Int) (AttributoMap AttributoValue) Int -> Cmd msg
httpUpdateSample f a =
    httpPatch
        { url = "api/samples"
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body = jsonBody (encodeSample a)
        }


httpDeleteSample : (Result RequestError () -> msg) -> Int -> Cmd msg
httpDeleteSample f sampleId =
    httpDelete
        { url = "api/samples"
        , body = jsonBody (Encode.object [ ( "id", Encode.int sampleId ) ])
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        }


fileDecoder : Decode.Decoder File
fileDecoder =
    Decode.map6
        File
        (Decode.field "id" Decode.int)
        (Decode.field "type_" Decode.string)
        (Decode.field "fileName" Decode.string)
        (Decode.field "description" Decode.string)
        (Decode.field "sizeInBytes" Decode.int)
        (Decode.field "originalPath" (Decode.maybe Decode.string))


httpCreateFile : (Result RequestError File -> msg) -> String -> ElmFile.File -> Cmd msg
httpCreateFile f description file =
    Http.post
        { url = "api/files"
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| fileDecoder)
        , body =
            multipartBody
                [ filePart "file" file
                , stringPart "metadata" (Encode.encode 0 <| Encode.object [ ( "description", Encode.string description ) ])
                ]
        }


httpStartRun : Int -> (Result RequestError () -> msg) -> Cmd msg
httpStartRun runId f =
    Http.get
        { url = "api/runs/" ++ String.fromInt runId ++ "/start"
        , expect =
            Http.expectJson (f << httpResultToRequestError) <|
                valueOrError <|
                    Decode.succeed ()
        }


httpStopRun : (Result RequestError () -> msg) -> Cmd msg
httpStopRun f =
    Http.get
        { url = "api/runs/stop-latest"
        , expect =
            Http.expectJson (f << httpResultToRequestError) <|
                valueOrError <|
                    Decode.succeed ()
        }
