module Amarcord.API.Requests exposing
    ( AnalysisResultsExperimentType
    , AnalysisResultsRoot
    , AppConfig
    , ChemicalsResponse
    , ConversionFlags
    , DataSetResult
    , Event
    , ExperimentType
    , ExperimentTypesResponse
    , IncludeLiveStream(..)
    , RequestError(..)
    , Run
    , RunEventDate
    , RunEventDateFilter(..)
    , RunFilter(..)
    , RunsBulkGetResponse
    , RunsResponse
    , RunsResponseContent
    , ScheduleEntry
    , ScheduleResponse
    , StandardUnitCheckResult(..)
    , emptyRunEventDateFilter
    , emptyRunFilter
    , httpChangeCurrentExperimentType
    , httpCheckStandardUnit
    , httpCreateAttributo
    , httpCreateChemical
    , httpCreateDataSet
    , httpCreateDataSetFromRun
    , httpCreateEvent
    , httpCreateExperimentType
    , httpCreateFile
    , httpCreateLiveStreamSnapshot
    , httpDeleteAttributo
    , httpDeleteChemical
    , httpDeleteDataSet
    , httpDeleteEvent
    , httpDeleteExperimentType
    , httpEditAttributo
    , httpGetAnalysisResults
    , httpGetAndDecodeAttributi
    , httpGetChemicals
    , httpGetConfig
    , httpGetDataSets
    , httpGetExperimentTypes
    , httpGetRunsBulk
    , httpGetRunsFilter
    , httpGetSchedule
    , httpStartRun
    , httpStopRun
    , httpUpdateChemical
    , httpUpdateRun
    , httpUpdateRunsBulk
    , httpUpdateSchedule
    , httpUserConfigurationSetAutoPilot
    , httpUserConfigurationSetOnlineCrystFEL
    , runEventDateFilter
    , runEventDateToString
    , runFilterToString
    , specificRunEventDateFilter
    )

import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoName, AttributoType, AttributoValue(..), attributoDecoder, attributoTypeDecoder)
import Amarcord.Chemical exposing (Chemical, ChemicalId)
import Amarcord.DataSet exposing (DataSet, DataSetSummary)
import Amarcord.File exposing (File)
import Amarcord.JsonSchema exposing (JsonSchema, encodeJsonSchema)
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
        (Decode.field "hit-rate" (Decode.maybe Decode.float))
        (Decode.field "indexing-rate" (Decode.maybe Decode.float))
        (Decode.field "indexed-frames" Decode.int)


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
    , chemicals : List (Chemical ChemicalId (AttributoMap AttributoValue) File)
    , experimentTypes : List ExperimentType
    }


type alias AnalysisResultsExperimentType =
    { dataSet : DataSet
    , runs : List String
    }


type alias AnalysisResultsRoot =
    { experimentTypes : Dict String (List AnalysisResultsExperimentType)
    , attributi : List (Attributo AttributoType)
    , chemicalIdToName : Dict Int String
    }


analysisResultsExperimentTypeDecoder : Decode.Decoder AnalysisResultsExperimentType
analysisResultsExperimentTypeDecoder =
    Decode.map2
        AnalysisResultsExperimentType
        (Decode.field "data-set" dataSetDecoder)
        (Decode.field "runs" (Decode.list Decode.string))


analysisResultsRootDecoder : Decode.Decoder AnalysisResultsRoot
analysisResultsRootDecoder =
    Decode.map3 AnalysisResultsRoot
        (Decode.field "experiment-types" <| Decode.dict (Decode.list analysisResultsExperimentTypeDecoder))
        (Decode.field "attributi" <| Decode.list (attributoDecoder attributoTypeDecoder))
        (Decode.field "chemical-id-to-name" <|
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
        (Decode.field "chemicals" <| Decode.list chemicalDecoder)
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
    , summary : DataSetSummary
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


type alias RunsResponseContent =
    { runs : List Run
    , runsDates : List RunEventDate
    , attributi : List (Attributo AttributoType)
    , events : List Event
    , chemicals : List (Chemical Int (AttributoMap AttributoValue) File)
    , dataSets : List DataSet
    , experimentTypes : Dict String (Set String)
    , autoPilot : Bool
    , onlineCrystFEL : Bool
    , jetStreamFileId : Maybe Int
    }


httpUserConfigurationSetBoolean : String -> (Result RequestError Bool -> msg) -> Bool -> Cmd msg
httpUserConfigurationSetBoolean description f newValue =
    httpPatch
        { url =
            "api/user-config/"
                ++ description
                ++ "/"
                ++ (if newValue then
                        "True"

                    else
                        "False"
                   )
        , expect =
            Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.field "value" Decode.bool)
        , body = emptyBody
        }


httpUserConfigurationSetOnlineCrystFEL : (Result RequestError Bool -> msg) -> Bool -> Cmd msg
httpUserConfigurationSetOnlineCrystFEL =
    httpUserConfigurationSetBoolean "online-crystfel"


httpUserConfigurationSetAutoPilot : (Result RequestError Bool -> msg) -> Bool -> Cmd msg
httpUserConfigurationSetAutoPilot =
    httpUserConfigurationSetBoolean "auto-pilot"


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
                        |> required "attributi" (Decode.list (attributoDecoder attributoTypeDecoder))
                        |> required "events" (Decode.list eventDecoder)
                        |> required "chemicals" (Decode.list chemicalDecoder)
                        |> required "data-sets" (Decode.list dataSetDecoder)
                        |> required "experiment-types" (Decode.dict (Decode.map Set.fromList <| Decode.list Decode.string))
                        |> required "auto-pilot" Decode.bool
                        |> required "online-crystfel" Decode.bool
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
    Decode.map5
        Run
        (Decode.field "id" Decode.int)
        (Decode.field "attributi" attributoMapDecoder)
        (Decode.field "summary" dataSetSummaryDecoder)
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
    , chemicals : List (Chemical ChemicalId (AttributoMap AttributoValue) File)
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
                        (Decode.field "chemicals" <| Decode.list chemicalDecoder)
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

        AssociatedTable.Chemical ->
            Encode.string "chemical"


chemicalDecoder : Decode.Decoder (Chemical ChemicalId (AttributoMap AttributoValue) File)
chemicalDecoder =
    Decode.map4
        Chemical
        (Decode.field "id" Decode.int)
        (Decode.field "name" Decode.string)
        (Decode.field "attributi" attributoMapDecoder)
        (Decode.field "files" (Decode.list fileDecoder))


encodeChemical : Chemical (Maybe Int) (AttributoMap AttributoValue) Int -> Encode.Value
encodeChemical s =
    Encode.object <|
        [ ( "name", Encode.string s.name )
        , ( "attributi", encodeAttributoMap s.attributi )
        , ( "fileIds", Encode.list Encode.int s.files )
        ]
            ++ unwrap [] (\id -> [ ( "id", Encode.int id ) ]) s.id


type alias ChemicalsResponse =
    Result RequestError ( List (Chemical ChemicalId (AttributoMap AttributoValue) File), List (Attributo AttributoType) )


httpGetChemicals : (ChemicalsResponse -> msg) -> Cmd msg
httpGetChemicals f =
    Http.get
        { url = "api/chemicals"
        , expect =
            Http.expectJson (f << httpResultToRequestError) <|
                valueOrError <|
                    Decode.map2 (\chemicals attributi -> ( chemicals, attributi ))
                        (Decode.field "chemicals" <| Decode.list chemicalDecoder)
                        (Decode.field "attributi" <| Decode.list (attributoDecoder attributoTypeDecoder))
        }


httpCreateChemical : (Result RequestError () -> msg) -> Chemical (Maybe Int) (AttributoMap AttributoValue) Int -> Cmd msg
httpCreateChemical f a =
    Http.post
        { url = "api/chemicals"
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body = jsonBody (encodeChemical a)
        }


httpUpdateChemical : (Result RequestError () -> msg) -> Chemical (Maybe Int) (AttributoMap AttributoValue) Int -> Cmd msg
httpUpdateChemical f a =
    httpPatch
        { url = "api/chemicals"
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body = jsonBody (encodeChemical a)
        }


httpDeleteChemical : (Result RequestError () -> msg) -> Int -> Cmd msg
httpDeleteChemical f chemicalId =
    httpDelete
        { url = "api/chemicals"
        , body = jsonBody (Encode.object [ ( "id", Encode.int chemicalId ) ])
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


type alias ScheduleResponse =
    { schedule : List ScheduleEntry
    }


type alias ScheduleEntry =
    { users : String
    , chemicalId : Maybe Int
    , date : String
    , shift : String
    , comment : String
    , tdSupport : String
    }


scheduleEntryDecoder : Decode.Decoder ScheduleEntry
scheduleEntryDecoder =
    Decode.map6
        ScheduleEntry
        (Decode.field "users" Decode.string)
        (Decode.maybe (Decode.field "chemical_id" Decode.int))
        (Decode.field "date" Decode.string)
        (Decode.field "shift" Decode.string)
        (Decode.field "comment" Decode.string)
        (Decode.field "td_support" Decode.string)


httpGetSchedule : (Result RequestError ScheduleResponse -> msg) -> Cmd msg
httpGetSchedule f =
    Http.get
        { url = "api/schedule"
        , expect =
            Http.expectJson (f << httpResultToRequestError)
                (valueOrError <|
                    Decode.map
                        ScheduleResponse
                        (Decode.field "schedule" <| Decode.list scheduleEntryDecoder)
                )
        }


httpUpdateSchedule : (Result RequestError () -> msg) -> List ScheduleEntry -> Cmd msg
httpUpdateSchedule req scheduleEntries =
    Http.post
        { url = "api/schedule"
        , expect = Http.expectJson (req << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body = jsonBody (Encode.object [ ( "schedule", Encode.list encodeScheduleList scheduleEntries ) ])
        }


encodeScheduleList : ScheduleEntry -> Encode.Value
encodeScheduleList se =
    Encode.object
        [ ( "users", Encode.string se.users )
        , ( "shift", Encode.string se.shift )
        , ( "date", Encode.string se.date )
        , ( "chemical_id", MaybeExtra.unwrap Encode.null Encode.int se.chemicalId )
        , ( "comment", Encode.string se.comment )
        , ( "td_support", Encode.string se.tdSupport )
        ]
