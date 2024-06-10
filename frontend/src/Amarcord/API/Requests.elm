module Amarcord.API.Requests exposing
    ( BeamtimeId
    , ConversionFlags
    , RunEventDate(..)
    , RunEventDateFilter(..)
    , RunExternalId(..)
    , RunFilter(..)
    , RunInternalId(..)
    , beamtimeIdToString
    , emptyRunEventDateFilter
    , emptyRunFilter
    , firstRunId
    , increaseRunExternalId
    , invalidBeamtimeId
    , runEventDateFilter
    , runEventDateToString
    , runExternalIdFromInt
    , runExternalIdToInt
    , runExternalIdToString
    , runFilterToString
    , runInternalIdToInt
    , runInternalIdToString
    , specificRunEventDateFilter
    )


type alias BeamtimeId =
    Int


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


runInternalIdToString : RunInternalId -> String
runInternalIdToString (RunInternalId x) =
    String.fromInt x


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
