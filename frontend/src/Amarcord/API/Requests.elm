module Amarcord.API.Requests exposing
    ( AnalysisResultsExperimentType
    , AnalysisResultsRoot
    , AppConfig
    , ChemicalsResponse
    , ConversionFlags
    , DataSetResult
    , Event
    , ExperimentTypeWithRuns
    , ExperimentTypesResponse
    , IncludeLiveStream(..)
    , MergeFom
    , MergeResult
    , MergeResultState(..)
    , MergeShellFom
    , RefinementResult
    , RequestError(..)
    , Run
    , RunEventDate
    , RunEventDateFilter(..)
    , RunFilter(..)
    , RunId
    , RunsBulkGetResponse
    , RunsResponse
    , RunsResponseContent
    , ScheduleEntry
    , ScheduleResponse
    , StandardUnitCheckResult(..)
    , emptyRunEventDateFilter
    , emptyRunFilter
    , httpChangeCurrentExperimentTypeForRun
    , httpCheckStandardUnit
    , httpCreateAttributo
    , httpCreateChemical
    , httpCreateDataSet
    , httpCreateDataSetFromRun
    , httpCreateEvent
    , httpCreateExperimentType
    , httpCreateFile
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
    , httpStartMergeJobForDataSet
    , httpStartRun
    , httpStopRun
    , httpUpdateChemical
    , httpUpdateRun
    , httpUpdateRunsBulk
    , httpUpdateSchedule
    , httpUserConfigurationSetAutoPilot
    , httpUserConfigurationSetInt
    , httpUserConfigurationSetOnlineCrystFEL
    , runEventDateFilter
    , runEventDateToString
    , runFilterToString
    , sortAnalysisResultsExperimentType
    , specificRunEventDateFilter
    )

import Amarcord.API.AttributoWithRole exposing (AttributoWithRole, encodeAttributoWithRole)
import Amarcord.API.DataSet exposing (DataSet, DataSetId, DataSetSummary, dataSetDecoder, dataSetSummaryDecoder)
import Amarcord.API.ExperimentType exposing (ExperimentType, ExperimentTypeId, experimentTypeDecoder)
import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoName, AttributoType, AttributoValue(..), attributoDecoder, attributoMapDecoder, attributoTypeDecoder, attributoValueDecoder)
import Amarcord.Chemical exposing (Chemical, ChemicalId, chemicalTypeDecoder, encodeChemicalType)
import Amarcord.CrystFELMerge exposing (MergeModel(..), MergeParametersInput, Polarisation, ScaleIntensities(..), mergeModelSubtitleFromString)
import Amarcord.File exposing (File)
import Amarcord.JsonSchema exposing (JsonSchema, encodeJsonSchema)
import Amarcord.PointGroupChooser exposing (PointGroup(..), pointGroupToString)
import Amarcord.UserError exposing (CustomError, customErrorDecoder)
import Amarcord.Util exposing (httpDelete, httpPatch)
import Dict exposing (Dict)
import File as ElmFile
import Http exposing (emptyBody, filePart, jsonBody, multipartBody, stringPart)
import Json.Decode as Decode
import Json.Decode.Pipeline exposing (custom, required)
import Json.Encode as Encode
import Maybe.Extra as MaybeExtra exposing (unwrap)
import Time exposing (Posix, millisToPosix)
import Tuple exposing (pair)


type alias RunId =
    Int


type alias ConversionFlags =
    { ignoreUnits : Bool
    }


errorDecoder : Decode.Decoder CustomError
errorDecoder =
    Decode.field "error" customErrorDecoder


valueOrError : Decode.Decoder value -> Decode.Decoder (Result CustomError value)
valueOrError valueDecoder =
    Decode.oneOf [ Decode.map Err errorDecoder, Decode.map Ok valueDecoder ]


httpCreateExperimentType : (Result RequestError () -> msg) -> String -> List AttributoWithRole -> Cmd msg
httpCreateExperimentType f name attributiWithRoles =
    Http.post
        { url = "api/experiment-types"
        , expect =
            Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body =
            jsonBody
                (Encode.object
                    [ ( "name", Encode.string name )
                    , ( "attributi", Encode.list encodeAttributoWithRole attributiWithRoles )
                    ]
                )
        }


httpDeleteExperimentType : (Result RequestError () -> msg) -> ExperimentTypeId -> Cmd msg
httpDeleteExperimentType f experimentTypeId =
    httpDelete
        { url = "api/experiment-types"
        , body = jsonBody (Encode.object [ ( "id", Encode.int experimentTypeId ) ])
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        }


type alias ExperimentTypeWithRuns =
    { experimentTypeId : Int, runs : List String }


type alias ExperimentTypesResponse =
    { experimentTypes : List ExperimentType
    , experimentTypeIdsToRuns : List ExperimentTypeWithRuns
    , attributi : List (Attributo AttributoType)
    }


httpGetExperimentTypes : (Result RequestError ExperimentTypesResponse -> msg) -> Cmd msg
httpGetExperimentTypes f =
    Http.get
        { url = "api/experiment-types"
        , expect =
            Http.expectJson (f << httpResultToRequestError)
                (valueOrError <|
                    Decode.map3
                        ExperimentTypesResponse
                        (Decode.field "experiment-types" <| Decode.list experimentTypeDecoder)
                        (Decode.field "experiment-type-id-to-run" <|
                            Decode.list
                                (Decode.map2 ExperimentTypeWithRuns
                                    (Decode.field "id" Decode.int)
                                    (Decode.field "runs" (Decode.list Decode.string))
                                )
                        )
                        (Decode.field "attributi" (Decode.list (attributoDecoder attributoTypeDecoder)))
                )
        }


encodeMergeModel : MergeModel -> Encode.Value
encodeMergeModel mm =
    Encode.string <|
        case mm of
            Offset ->
                "offset"

            Unity ->
                "unity"

            XSphere ->
                "xsphere"

            Ggpm ->
                "ggpm"


encodeScaleIntensities : ScaleIntensities -> Encode.Value
encodeScaleIntensities si =
    Encode.string <|
        case si of
            Off ->
                "off"

            Normal ->
                "normal"

            DebyeWaller ->
                "debyewaller"


encodePolarisation : Polarisation -> Encode.Value
encodePolarisation p =
    Encode.object [ ( "angle", Encode.int p.angle ), ( "percent", Encode.int p.percent ) ]


encodePointGroup : PointGroup -> Encode.Value
encodePointGroup =
    Encode.string << pointGroupToString


encodeOptional : (a -> Encode.Value) -> Maybe a -> Encode.Value
encodeOptional f =
    Maybe.withDefault Encode.null << Maybe.map f


httpStartMergeJobForDataSet : (Result RequestError () -> msg) -> DataSetId -> MergeParametersInput -> Cmd msg
httpStartMergeJobForDataSet f dataSetId merge =
    Http.post
        { url = "api/merging/" ++ String.fromInt dataSetId ++ "/start"
        , expect =
            Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body =
            jsonBody
                (Encode.object
                    [ ( "strict-mode", Encode.bool False )
                    , ( "merge-model", encodeMergeModel merge.model )
                    , ( "scale-intensities", encodeScaleIntensities merge.scaleIntensities )
                    , ( "post-refinement", Encode.bool merge.postRefinement )
                    , ( "iterations", Encode.int merge.iterations )
                    , ( "polarisation", encodeOptional encodePolarisation merge.polarisation )
                    , ( "start-after", encodeOptional Encode.int merge.startAfter )
                    , ( "stop-after", encodeOptional Encode.int merge.stopAfter )
                    , ( "rel-b", Encode.float merge.relB )
                    , ( "no-pr", Encode.bool merge.noPr )
                    , ( "force-bandwidth", encodeOptional Encode.float merge.forceBandwidth )
                    , ( "force-radius", encodeOptional Encode.float merge.forceRadius )
                    , ( "force-lambda", encodeOptional Encode.float merge.forceLambda )
                    , ( "no-delta-cc-half", Encode.bool merge.noDeltaCcHalf )
                    , ( "max-adu", encodeOptional Encode.float merge.maxAdu )
                    , ( "min-measurements", Encode.int merge.minMeasurements )
                    , ( "logs", Encode.bool merge.logs )
                    , ( "min-res", encodeOptional Encode.float merge.minRes )
                    , ( "push-res", encodeOptional Encode.float merge.pushRes )
                    , ( "w", encodeOptional encodePointGroup merge.w )
                    ]
                )
        }


httpCreateDataSet : (Result RequestError () -> msg) -> ExperimentTypeId -> AttributoMap AttributoValue -> Cmd msg
httpCreateDataSet f experimentTypeId attributi =
    Http.post
        { url = "api/data-sets"
        , expect =
            Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body =
            jsonBody
                (Encode.object
                    [ ( "experiment-type-id", Encode.int experimentTypeId )
                    , ( "attributi", encodeAttributoMap attributi )
                    ]
                )
        }


httpCreateDataSetFromRun : (Result RequestError () -> msg) -> ExperimentTypeId -> RunId -> Cmd msg
httpCreateDataSetFromRun f experimentTypeId runId =
    Http.post
        { url = "api/data-sets/from-run"
        , expect =
            Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body =
            jsonBody
                (Encode.object
                    [ ( "experiment-type-id", Encode.int experimentTypeId )
                    , ( "run-id", Encode.int runId )
                    ]
                )
        }


httpChangeCurrentExperimentTypeForRun : (Result RequestError () -> msg) -> Maybe ExperimentTypeId -> RunId -> Cmd msg
httpChangeCurrentExperimentTypeForRun f experimentTypeId runId =
    Http.post
        { url = "api/experiment-types/change-for-run"
        , expect =
            Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.succeed ())
        , body =
            jsonBody
                (Encode.object
                    [ ( "experiment-type-id", MaybeExtra.unwrap Encode.null Encode.int experimentTypeId )
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


type alias MergeOuterShell =
    { resolution : Float
    , ccStar : Float
    , rSplit : Float
    , cc : Float
    , uniqueReflections : Int
    , completeness : Float
    , redundancy : Float
    , snr : Float
    , minRes : Float
    , maxRes : Float
    }


mergeOuterShellDecoder : Decode.Decoder MergeOuterShell
mergeOuterShellDecoder =
    Decode.succeed MergeOuterShell
        |> required "resolution" Decode.float
        |> required "cc-star" Decode.float
        |> required "r-split" Decode.float
        |> required "cc" Decode.float
        |> required "unique-reflections" Decode.int
        |> required "completeness" Decode.float
        |> required "redundancy" Decode.float
        |> required "snr" Decode.float
        |> required "min-res" Decode.float
        |> required "max-res" Decode.float


mergeShellFomDecoder : Decode.Decoder MergeShellFom
mergeShellFomDecoder =
    Decode.succeed MergeShellFom
        |> required "one-over-d-centre" Decode.float
        |> required "nref" Decode.int
        |> required "d-over-a" Decode.float
        |> required "min-res" Decode.float
        |> required "max-res" Decode.float
        |> required "cc" Decode.float
        |> required "ccstar" Decode.float
        |> required "r-split" Decode.float
        |> required "reflections-possible" Decode.int
        |> required "completeness" Decode.float
        |> required "measurements" Decode.int
        |> required "redundancy" Decode.float
        |> required "snr" Decode.float
        |> required "mean-i" Decode.float


type alias MergeShellFom =
    { oneOverDCentre : Float
    , nRef : Int
    , dOverA : Float
    , minRes : Float
    , maxRes : Float
    , cc : Float
    , ccStar : Float
    , rSplit : Float
    , reflectionsPossible : Int
    , completeness : Float
    , measurements : Int
    , redundancy : Float
    , snr : Float
    , meanI : Float
    }


type alias MergeFom =
    { snr : Float
    , wilson : Maybe Float
    , lnK : Maybe Float
    , discardedReflections : Int
    , oneOverDFrom : Float
    , oneOverDTo : Float
    , redundancy : Float
    , completeness : Float
    , measurementsTotal : Int
    , reflectionsTotal : Int
    , reflectionsPossible : Int
    , rSplit : Float
    , r1i : Float
    , r2 : Float
    , cc : Float
    , ccStar : Float
    , ccAno : Maybe Float
    , ccRdAno : Maybe Float
    , rAno : Maybe Float
    , rAnoOverRSplit : Maybe Float
    , d1sig : Float
    , d2sig : Float
    , outerShell : MergeOuterShell
    }


mergeFomDecoder : Decode.Decoder MergeFom
mergeFomDecoder =
    Decode.succeed MergeFom
        |> required "snr" Decode.float
        |> optional "wilson" Decode.float
        |> optional "ln-k" Decode.float
        |> required "discarded-reflections" Decode.int
        |> required "one-over-d-from" Decode.float
        |> required "one-over-d-to" Decode.float
        |> required "redundancy" Decode.float
        |> required "completeness" Decode.float
        |> required "measurements-total" Decode.int
        |> required "reflections-total" Decode.int
        |> required "reflections-possible" Decode.int
        |> required "r-split" Decode.float
        |> required "r1i" Decode.float
        |> required "r2" Decode.float
        |> required "cc" Decode.float
        |> required "cc-star" Decode.float
        |> optional "cc-ano" Decode.float
        |> optional "crd-ano" Decode.float
        |> optional "r-ano" Decode.float
        |> optional "r-ano-over-r-split" Decode.float
        |> required "d1sig" Decode.float
        |> required "d2sig" Decode.float
        |> required "outer-shell" mergeOuterShellDecoder


type MergeResultState
    = MergeResultQueued
    | MergeResultRunning Posix Int String
    | MergeResultError Posix Posix String String
    | MergeResultDone Posix Posix Int MergeFom (List MergeShellFom)


decodeMergeResultState : Decode.Decoder MergeResultState
decodeMergeResultState =
    let
        decodeRunning : Decode.Decoder MergeResultState
        decodeRunning =
            Decode.field "state-running" <|
                Decode.map3 MergeResultRunning
                    (Decode.field "started" decodePosix)
                    (Decode.field "job-id" Decode.int)
                    (Decode.field "latest-log" Decode.string)

        decodeQueued : Decode.Decoder MergeResultState
        decodeQueued =
            Decode.field "state-queued" <| Decode.succeed MergeResultQueued

        decodeDone : Decode.Decoder MergeResultState
        decodeDone =
            Decode.field "state-done" <|
                Decode.map5 MergeResultDone
                    (Decode.field "started" decodePosix)
                    (Decode.field "stopped" decodePosix)
                    (Decode.field "result" <| Decode.field "mtz-file-id" Decode.int)
                    (Decode.field "result" <| Decode.field "fom" <| mergeFomDecoder)
                    (Decode.field "result" <| Decode.field "detailed-foms" <| Decode.list <| mergeShellFomDecoder)

        decodeError : Decode.Decoder MergeResultState
        decodeError =
            Decode.field "state-error" <|
                Decode.map4 MergeResultError
                    (Decode.field "started" decodePosix)
                    (Decode.field "stopped" decodePosix)
                    (Decode.field "error" Decode.string)
                    (Decode.field "latest-log" Decode.string)
    in
    Decode.oneOf [ decodeRunning, decodeDone, decodeError, decodeQueued ]


type alias RefinementResult =
    { id : Int
    , pdbFileId : Int
    , mtzFileId : Int
    , rFree : Float
    , rWork : Float
    , rmsBondAngle : Float
    , rmsBondLength : Float
    }


refinementResultDecoder : Decode.Decoder RefinementResult
refinementResultDecoder =
    Decode.map7 RefinementResult
        (Decode.field "id" Decode.int)
        (Decode.field "pdb-file-id" Decode.int)
        (Decode.field "mtz-file-id" Decode.int)
        (Decode.field "r-free" Decode.float)
        (Decode.field "r-work" Decode.float)
        (Decode.field "rms-bond-angle" Decode.float)
        (Decode.field "rms-bond-length" Decode.float)


type alias MergeResult =
    { id : Int
    , created : Posix
    , runs : List String
    , pointGroup : String
    , cellDescription : String
    , parameters : MergeParametersInput
    , refinementResults : List RefinementResult
    , state : MergeResultState
    }


decodeFromResult : Result String v -> Decode.Decoder v
decodeFromResult x =
    case x of
        Err message ->
            Decode.fail message

        Ok v ->
            Decode.succeed v


mergeModelDecoder : Decode.Decoder MergeModel
mergeModelDecoder =
    Decode.string
        |> Decode.andThen
            (decodeFromResult << Result.fromMaybe "invalid merge model" << mergeModelSubtitleFromString)


polarisationDecoder : Decode.Decoder Polarisation
polarisationDecoder =
    Decode.succeed Polarisation |> required "angle" Decode.int |> required "percent" Decode.int


scaleIntensitiesDecoder : Decode.Decoder ScaleIntensities
scaleIntensitiesDecoder =
    let
        scaleIntensitiesFromString : String -> Result String ScaleIntensities
        scaleIntensitiesFromString x =
            case x of
                "off" ->
                    Ok Off

                "debyewaller" ->
                    Ok DebyeWaller

                "normal" ->
                    Ok Normal

                _ ->
                    Err ("invalid scaling intensities " ++ x)
    in
    Decode.string
        |> Decode.andThen
            (decodeFromResult << scaleIntensitiesFromString)


pointGroupDecoder : Decode.Decoder PointGroup
pointGroupDecoder =
    Decode.map PointGroup Decode.string


optional : String -> Decode.Decoder a -> Decode.Decoder (Maybe a -> b) -> Decode.Decoder b
optional name x =
    required name (Decode.maybe x)


mergeParametersDecoder : Decode.Decoder MergeParametersInput
mergeParametersDecoder =
    Decode.succeed MergeParametersInput
        |> required "merge-model" mergeModelDecoder
        |> required "scale-intensities" scaleIntensitiesDecoder
        |> required "post-refinement" Decode.bool
        |> required "iterations" Decode.int
        |> optional "polarisation" polarisationDecoder
        |> optional "start-after" Decode.int
        |> optional "stop-after" Decode.int
        |> required "rel-b" Decode.float
        |> required "no-pr" Decode.bool
        |> optional "force-bandwidth" Decode.float
        |> optional "force-radius" Decode.float
        |> optional "force-lambda" Decode.float
        |> required "no-delta-cc-half" Decode.bool
        |> required "max-adu" (Decode.maybe Decode.float)
        |> required "min-measurements" Decode.int
        |> required "logs" Decode.bool
        |> optional "min-res" Decode.float
        |> optional "push-res" Decode.float
        |> required "w" (Decode.maybe pointGroupDecoder)
        |> optional "negative-handling" Decode.string


mergeResultDecoder : Decode.Decoder MergeResult
mergeResultDecoder =
    Decode.succeed MergeResult
        |> required "id" Decode.int
        |> required "created" decodePosix
        |> required "runs" (Decode.list Decode.string)
        |> required "point-group" Decode.string
        |> required "cell-description" Decode.string
        |> required "parameters" mergeParametersDecoder
        |> required "refinement-results" (Decode.list refinementResultDecoder)
        |> custom decodeMergeResultState


type alias AnalysisResultsExperimentType =
    { dataSet : DataSet
    , runs : List String
    , mergeResults : List MergeResult
    , numberOfIndexingResults : Int
    }


sortAnalysisResultsExperimentType : AnalysisResultsExperimentType -> AnalysisResultsExperimentType -> Order
sortAnalysisResultsExperimentType a b =
    compare a.dataSet.id b.dataSet.id


type alias AnalysisResultsRoot =
    { experimentTypes : List ExperimentType
    , dataSets : Dict ExperimentTypeId (List AnalysisResultsExperimentType)
    , attributi : List (Attributo AttributoType)
    , chemicalIdToName : Dict Int String
    }


analysisResultsExperimentTypeDecoder : Decode.Decoder AnalysisResultsExperimentType
analysisResultsExperimentTypeDecoder =
    Decode.map4
        AnalysisResultsExperimentType
        (Decode.field "data-set" dataSetDecoder)
        (Decode.field "runs" (Decode.list Decode.string))
        (Decode.field "merge-results" (Decode.list mergeResultDecoder))
        (Decode.field "number-of-indexing-results" Decode.int)


analysisResultsRootDecoder : Decode.Decoder AnalysisResultsRoot
analysisResultsRootDecoder =
    Decode.map4 AnalysisResultsRoot
        (Decode.field "experiment-types" <| Decode.list experimentTypeDecoder)
        (Decode.field "data-sets" <|
            Decode.map
                Dict.fromList
                (Decode.list
                    (Decode.map2
                        (\et ds -> ( et, ds ))
                        (Decode.field "experiment-type" Decode.int)
                        (Decode.field "data-sets" (Decode.list analysisResultsExperimentTypeDecoder))
                    )
                )
        )
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
    , experimentTypeId : Int
    , summary : DataSetSummary
    , files : List File
    , dataSets : List Int
    , runningIndexingJobs : List Int
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
    , experimentTypes : List ExperimentType
    , userConfig : UserConfig
    , jetStreamFileId : Maybe Int
    }


httpUserConfigurationSetInt : String -> (Result RequestError Int -> msg) -> Int -> Cmd msg
httpUserConfigurationSetInt description f newValue =
    httpPatch
        { url =
            "api/user-config/"
                ++ description
                ++ "/"
                ++ String.fromInt newValue
        , expect =
            Http.expectJson (f << httpResultToRequestError) (valueOrError <| Decode.field "value" Decode.int)
        , body = emptyBody
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
                        |> required "experiment-types" (Decode.list experimentTypeDecoder)
                        |> required "user-config" userConfigDecoder
                        |> optional "live-stream-file-id" Decode.int
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


runDecoder : Decode.Decoder Run
runDecoder =
    Decode.map7
        Run
        (Decode.field "id" Decode.int)
        (Decode.field "attributi" attributoMapDecoder)
        (Decode.field "experiment-type-id" Decode.int)
        (Decode.field "summary" dataSetSummaryDecoder)
        (Decode.field "files" (Decode.list fileDecoder))
        (Decode.field "data-sets" (Decode.list Decode.int))
        (Decode.field "running-indexing-jobs" (Decode.list Decode.int))


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
    Encode.object
        [ ( "id", Encode.int run.id )
        , ( "attributi", encodeAttributoMap run.attributi )
        , ( "experiment-type-id", Encode.int run.experimentTypeId )
        ]


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
    { runIds : List RunId
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
    { runIds : List RunId }


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


type alias UserConfig =
    { autoPilot : Bool
    , onlineCrystFEL : Bool
    , currentExperimentTypeId : Maybe ExperimentTypeId
    }


type alias AppConfig =
    { title : String
    }


appConfigDecoder : Decode.Decoder AppConfig
appConfigDecoder =
    Decode.map AppConfig (Decode.field "title" Decode.string)


userConfigDecoder : Decode.Decoder UserConfig
userConfigDecoder =
    Decode.map3 UserConfig
        (Decode.field "auto-pilot" Decode.bool)
        (Decode.field "online-crystfel" Decode.bool)
        (Decode.field "current-experiment-type-id" (Decode.maybe Decode.int))


httpGetConfig : (Result RequestError AppConfig -> msg) -> Cmd msg
httpGetConfig f =
    Http.get
        { url = "api/config"
        , expect = Http.expectJson (f << httpResultToRequestError) (valueOrError <| appConfigDecoder)
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
    Decode.map6
        Chemical
        (Decode.field "id" Decode.int)
        (Decode.field "name" Decode.string)
        (Decode.field "responsiblePerson" Decode.string)
        (Decode.field "type" chemicalTypeDecoder)
        (Decode.field "attributi" attributoMapDecoder)
        (Decode.field "files" (Decode.list fileDecoder))


encodeChemical : Chemical (Maybe Int) (AttributoMap AttributoValue) Int -> Encode.Value
encodeChemical s =
    Encode.object <|
        [ ( "name", Encode.string s.name )
        , ( "responsiblePerson", Encode.string s.responsiblePerson )
        , ( "attributi", encodeAttributoMap s.attributi )
        , ( "type", encodeChemicalType s.type_ )
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


httpStartRun : RunId -> (Result RequestError () -> msg) -> Cmd msg
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
    , chemicals : List Int
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
        (Decode.field "chemicals" <| Decode.list Decode.int)
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
        , ( "chemicals", Encode.list Encode.int se.chemicals )
        , ( "comment", Encode.string se.comment )
        , ( "td_support", Encode.string se.tdSupport )
        ]
