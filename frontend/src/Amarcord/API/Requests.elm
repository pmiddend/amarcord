module Amarcord.API.Requests exposing
    ( BeamtimeId
    , ConversionFlags
    , ExperimentTypeId
    , IndexingParametersId(..)
    , IndexingParametersIdSet(..)
    , IndexingResultId(..)
    , MergeResultId
    , RunEventDate(..)
    , RunEventDateFilter(..)
    , RunExternalId(..)
    , RunFilter(..)
    , RunInternalId(..)
    , beamtimeIdToString
    , emptyIndexingParametersIdSet
    , emptyRunEventDateFilter
    , emptyRunFilter
    , firstRunId
    , increaseRunExternalId
    , indexingParametersIdToInt
    , indexingParametersIdToString
    , indexingResultIdToString
    , insertIndexingParametersIdSet
    , invalidBeamtimeId
    , memberIndexingParametersIdSet
    , removeIndexingParametersIdSet
    , runEventDateFilter
    , runEventDateToString
    , runExternalIdFromInt
    , runExternalIdToInt
    , runExternalIdToString
    , runFilterToString
    , runInternalIdToInt
    , specificRunEventDateFilter
    )

import Set


type IndexingParametersId
    = IndexingParametersId Int


indexingParametersIdToInt : IndexingParametersId -> Int
indexingParametersIdToInt (IndexingParametersId i) =
    i


type alias BeamtimeId =
    Int


type alias ExperimentTypeId =
    Int


type alias MergeResultId =
    Int


type IndexingResultId
    = IndexingResultId Int


indexingResultIdToString : IndexingResultId -> String
indexingResultIdToString (IndexingResultId i) =
    String.fromInt i


indexingParametersIdToString : IndexingParametersId -> String
indexingParametersIdToString (IndexingParametersId i) =
    String.fromInt i


type IndexingParametersIdSet
    = IndexingParametersIdSet (Set.Set Int)


emptyIndexingParametersIdSet : IndexingParametersIdSet
emptyIndexingParametersIdSet =
    IndexingParametersIdSet Set.empty


memberIndexingParametersIdSet : IndexingParametersId -> IndexingParametersIdSet -> Bool
memberIndexingParametersIdSet (IndexingParametersId i) (IndexingParametersIdSet set) =
    Set.member i set


removeIndexingParametersIdSet : IndexingParametersId -> IndexingParametersIdSet -> IndexingParametersIdSet
removeIndexingParametersIdSet (IndexingParametersId i) (IndexingParametersIdSet set) =
    IndexingParametersIdSet (Set.remove i set)


insertIndexingParametersIdSet : IndexingParametersId -> IndexingParametersIdSet -> IndexingParametersIdSet
insertIndexingParametersIdSet (IndexingParametersId i) (IndexingParametersIdSet set) =
    IndexingParametersIdSet (Set.insert i set)


beamtimeIdToString : BeamtimeId -> String
beamtimeIdToString =
    String.fromInt


invalidBeamtimeId : BeamtimeId
invalidBeamtimeId =
    0


type RunInternalId
    = RunInternalId Int


runInternalIdToInt : RunInternalId -> Int
runInternalIdToInt (RunInternalId x) =
    x


type RunExternalId
    = RunExternalId Int


runExternalIdToString : RunExternalId -> String
runExternalIdToString (RunExternalId x) =
    String.fromInt x


runExternalIdToInt : RunExternalId -> Int
runExternalIdToInt (RunExternalId x) =
    x


runExternalIdFromInt : Int -> RunExternalId
runExternalIdFromInt =
    RunExternalId


increaseRunExternalId : RunExternalId -> RunExternalId
increaseRunExternalId (RunExternalId x) =
    RunExternalId (x + 1)


firstRunId : RunExternalId
firstRunId =
    RunExternalId 1


type alias ConversionFlags =
    { ignoreUnits : Bool
    }



-- sortAnalysisResultsExperimentType : AnalysisResultsExperimentType -> AnalysisResultsExperimentType -> Order
-- sortAnalysisResultsExperimentType a b =
--     compare a.dataSet.id b.dataSet.id


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


runEventDateToString : RunEventDate -> String
runEventDateToString (RunEventDate s) =
    s
