module App.API where

import Affjax (Error, Response, printError)
import Affjax as AX
import Affjax.RequestBody (RequestBody(..))
import Affjax.ResponseFormat as ResponseFormat
import Affjax.StatusCode (StatusCode(..))
import App.Config (baseUrl)
import App.Event (Event)
import App.Formatting (printDateTimeInIso)
import App.MiniSample (MiniSample)
import App.Overview (OverviewRow)
import App.PuckType (PuckType(..))
import App.QualifiedColumn (QualifiedColumn, qualifiedColumnToString)
import App.Since (Since, serializeSince)
import App.SortOrder (SortOrder(..), sortToString)
import App.Utils (stringMapToJsonObject, stringMaybeMapToJsonObject)
import Control.Bind (bind)
import Control.Monad (class Monad, pure)
import Data.Argonaut (class DecodeJson, JsonDecodeError, fromObject, fromString, stringify)
import Data.Argonaut as Argonaut
import Data.Argonaut.Core (Json, stringifyWithIndent)
import Data.Argonaut.Decode (decodeJson, printJsonDecodeError)
import Data.Argonaut.Encode (encodeJson)
import Data.DateTime (DateTime)
import Data.Either (Either(..))
import Data.Eq ((==))
import Data.Foldable (intercalate)
import Data.FormURLEncoded (fromArray)
import Data.Function (($), (<<<))
import Data.Functor ((<$>))
import Data.Map (Map)
import Data.Map as Map
import Data.Maybe (Maybe(..), maybe)
import Data.Monoid ((<>))
import Data.Semigroup (append)
import Data.Show (show)
import Data.Tuple (Tuple(..))
import Data.Unfoldable (fromMaybe)
import Effect.Aff.Class (class MonadAff)
import Foreign.Object as FO
import Halogen (liftAff, liftEffect)
import Prelude (discard)
import Web.File.File (File, toBlob)
import Web.XHR.FormData as FormData

type IngestedCrystal = {
    crystalId :: String
  , runId :: Int
  , warnings :: Array String
  , ingested :: Boolean
  , processingResultIngested :: Boolean
  }

type P11IngestResponse = {
    error :: Maybe String
  , crystalWarnings :: Array String
  , crystals :: Array IngestedCrystal
  }

type OverviewResponse
  = { overviewRows :: Array OverviewRow
    }

type AnalysisRow
  = Array Json

type AnalysisColumnsResponse
  = { columns :: Array String
    }

type AnalysisResponse
  = { analysis :: Array AnalysisRow
    , totalRows :: Int
    , capped :: Boolean
    , queryTime :: Int
    , sqlError :: Maybe String
    }

type APIAddCrystalRequest
  = { puckId :: String
    , puckPosition :: Int
    , targetId :: String
    , wellId :: String
    , crystalTrayId :: String
    , dropId :: Int
    , dateOfHarvesting :: DateTime
    , creator :: String
    }

type RunJobResponse
  = { jobIds :: Array Int
    }

type ReductionCountResult
  = { totalResults :: Int
    }

type EventsResponse
  = { events :: Array Event
    }

type MiniSamplesResponse
  = { samples :: Array MiniSample
    }

type APIPuck
  = { puckId :: String, puckType :: PuckType }

type APITarget
  = { id :: String, name :: String }

type Crystal
  = { crystalId :: String
    , puckId :: Maybe String
    , puckPosition :: Maybe Int
    , attributi :: Array (Tuple String Json)
    }

type APIPucksResponse
  = { columnInfo :: Array GenericColumn
    , rows :: Array (Array Json)
    }

type APITargetsResponse
  = { columnInfo :: Array GenericColumn
    , rows :: Array (Array Json)
    }

type GenericColumn
  = { header :: String
    , identifier :: String
    , "type" :: Maybe String
    }

type APICrystalsResponse
  = { columnInfo :: Array GenericColumn
    , rows :: Array (Array Json)
    }

type APIAddCrystalResponse
  = { lastCrystalId :: Maybe String
    , errorMessage :: Maybe String
    }

type APIRemoveCrystalResponse
  = { errorMessage :: Maybe String
    }

type CrystalFilter
  = { columnName :: String
    , values :: Array (Maybe String)
    }

type CrystalFilterResponse
  = { columnsWithValues :: Array CrystalFilter
    }

type APICrystalFormDataResponse
  = { puckIds :: Array String
    , targetIds :: Array String
    , crystalTrayIds :: Array String
    , dateOfHarvesting :: String
    }

type ToolInput
  = { name :: String
    , "type" :: String
    }

type ToolInputMap
  = Map.Map String String

type DiffractionList
  = Array (Tuple String Int)

type Tool
  = { toolId :: Int
    , created :: String
    , name :: String
    , executablePath :: String
    , extraFiles :: Array String
    , commandLine :: String
    , description :: String
    , inputs :: Array ToolInput
    }

type ToolsResponse
  = { tools :: Array Tool
    }

type Job
  = { jobId :: Int
    , started :: Maybe String
    , stopped :: Maybe String
    , queued :: String
    , status :: String
    , failureReason :: Maybe String
    , comment :: Maybe String
    , metadata :: Maybe Json
    , outputDir :: Maybe String
    , toolId :: Int
    , toolInputs :: Json
    , lastStderr :: Maybe String
    , lastStdout :: Maybe String
    }

type Compound
  = { compoundId :: String
    , crystalTrayId :: String
    , wellId :: String
    , name :: Maybe String
    }

type CompoundUploadError
  = { message :: String
    , level :: String
    }

type CompoundsInTrayResponse
  = { compounds :: Array Compound
    }

type CompoundUploadResponse
  = { errors :: Array CompoundUploadError
    , newCompounds :: Array Compound
    , oldCompounds :: Array Compound
    }

type AugmentedJob
  = { jobId :: Int
    , started :: Maybe String
    , stopped :: Maybe String
    , queued :: String
    , status :: String
    , tag :: Maybe String
    , failureReason :: Maybe String
    , comment :: Maybe String
    , metadata :: Maybe Json
    , outputDir :: Maybe String
    , diffraction ::
        Maybe
          { runId :: Int
          , crystalId :: String
          }
    , reduction :: Maybe { dataReductionId :: Int, mtzPath :: String, runId :: Int, crystalId :: String }
    , tool :: String
    , toolInputs :: Json
    }

type JobsResponse
  = { jobs :: Array AugmentedJob
    , tags :: Array String
    }

type JobResponse
  = { job :: Job
    }

type DewarEntry
  = { puckId :: String
    , dewarPosition :: Int
    , totalCrystals :: Int
    , totalScreenedCrystals :: Int
    , etaSeconds :: Int
    }

type DewarResponse
  = { dewarTable :: Array DewarEntry
    }

type DiffractionEntry
  = { crystalId :: String
    , runId :: Maybe Int
    , compoundId :: Maybe String
    , dewarPosition :: Maybe Int
    , diffraction :: Maybe String
    , comment :: Maybe String
    , puckPositionId :: Int
    }

type DiffractionResponse
  = { diffractions :: Array DiffractionEntry, fortune :: Maybe String }

handleResponse :: forall a m. Monad m => DecodeJson a => Either Error (Response Json) -> m (Either String a)
handleResponse response = do
  case response of
    Left httpError -> pure (Left (printError httpError))
    Right httpResult -> do
      let
        httpBody :: Json
        httpBody = httpResult.body

        decodedJson :: Either JsonDecodeError a
        decodedJson = decodeJson httpBody
      case httpResult.status of
        StatusCode 200 -> case decodedJson of
          Left jsonError -> pure (Left (printJsonDecodeError jsonError))
          Right jsonResult -> pure (Right jsonResult)
        StatusCode sc -> pure (Left ("Status code: " <> show sc <> "\n\nJSON response:\n" <> stringifyWithIndent 2 httpResult.body))

type BeamtimeId = Int
type XdsSubpath = String

p11Ingest :: forall m. MonadAff m => BeamtimeId -> XdsSubpath -> Boolean -> Boolean -> m (Either String P11IngestResponse)
p11Ingest beamtimeId xdsSubpath dryRun oldLayout = do
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl <> "/api/p11-ingest")
          ( Just
              ( Json
                  ( encodeJson
                      { beamtimeId
                      , dryRun
                      , oldLayout
                      , xdsSubpath
                      }
                  )
              )
          )
  handleResponse response

retrieveDiffractionCount :: forall m. MonadAff m => Maybe String -> Map String (Maybe String) -> Boolean -> Boolean -> m (Either String ReductionCountResult)
retrieveDiffractionCount filterQuery crystalFiltersMap onlyUnprocessed onlyWithoutJobs = do
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl <> "/api/diffraction-count")
          ( Just
              ( Json
                  ( encodeJson
                      { onlyUnprocessed
                      , onlyWithoutJobs
                      , crystalFilters: stringMaybeMapToJsonObject crystalFiltersMap
                      , filterQuery
                      }
                  )
              )
          )
  handleResponse response

retrieveReductionCount :: forall m. MonadAff m => Maybe String -> Map String (Maybe String) -> Maybe String -> Boolean -> Boolean -> m (Either String ReductionCountResult)
retrieveReductionCount filterQuery crystalFiltersMap reductionMethod onlyNonrefined onlyWithoutJobs = do
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl <> "/api/reduction-count")
          ( Just
              ( Json
                  ( encodeJson
                      { reductionMethod
                      , onlyNonrefined
                      , onlyWithoutJobs
                      , crystalFilters: stringMaybeMapToJsonObject crystalFiltersMap
                      , filterQuery
                      }
                  )
              )
          )
  handleResponse response

startReductionJobsSimple :: forall m. MonadAff m => Int -> String -> String -> String -> Maybe Int -> Map String String -> Maybe String -> Map String (Maybe String) -> Maybe String -> Boolean -> Boolean -> m (Either String RunJobResponse)
startReductionJobsSimple toolId baseDirectory comment tag limit toolInputs filterQuery crystalFiltersMap reductionMethod onlyNonrefined onlyWithoutJobs = do
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl <> "/api/workflows/jobs-simple/start-refining/" <> show toolId)
          ( Just
              ( Json
                  ( encodeJson
                      { reductionMethod
                      , baseDirectory
                      , onlyNonrefined
                      , onlyWithoutJobs
                      , limit
                      , comment
                      , tag
                      , filterQuery
                      , inputs: stringMapToJsonObject toolInputs
                      , crystalFilters: stringMaybeMapToJsonObject crystalFiltersMap
                      }
                  )
              )
          )
  handleResponse response

startProcessJobsSimple :: forall m. MonadAff m => Int -> String -> String -> String -> Maybe Int -> Map String String -> Map String (Maybe String) -> Maybe String -> Boolean -> Boolean -> m (Either String RunJobResponse)
startProcessJobsSimple toolId baseDirectory comment tag limit toolInputs crystalFiltersMap filterQuery onlyUnprocessed onlyWithoutJobs = do
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl <> "/api/workflows/jobs-simple/start-process/" <> show toolId)
          ( Just
              ( Json
                  ( encodeJson
                      { onlyUnprocessed
                      , baseDirectory
                      , onlyWithoutJobs
                      , limit
                      , tag
                      , comment
                      , filterQuery
                      , inputs: stringMapToJsonObject toolInputs
                      , crystalFilters: stringMaybeMapToJsonObject crystalFiltersMap
                      }
                  )
              )
          )
  handleResponse response

retrieveAnalysisColumns :: forall m. MonadAff m => m (Either String AnalysisColumnsResponse)
retrieveAnalysisColumns = do
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl <> "/api/analysis-columns")
  handleResponse response

retrieveTools :: forall m. MonadAff m => m (Either String ToolsResponse)
retrieveTools = do
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl <> "/api/workflows/tools")
  handleResponse response

retrieveCompoundsInCrystalTray :: forall m. MonadAff m => String -> m (Either String CompoundsInTrayResponse)
retrieveCompoundsInCrystalTray crystalTrayId = do
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl <> "/api/crystal-trays/" <> crystalTrayId)
  handleResponse response

uploadCompounds :: forall m. MonadAff m => Boolean -> File -> m (Either String CompoundUploadResponse)
uploadCompounds ignoreErrors f = do
  fd <- liftEffect FormData.new
  liftEffect (FormData.appendBlob (FormData.EntryName "file") (toBlob f) Nothing fd)
  liftEffect (FormData.append (FormData.EntryName "ignoreErrors") (if ignoreErrors then "true" else "false") fd)
  response <- liftAff $ AX.post ResponseFormat.json (baseUrl <> "/api/compounds") (Just (FormData fd))
  handleResponse response

retrieveCrystalFilters :: forall m. MonadAff m => m (Either String CrystalFilterResponse)
retrieveCrystalFilters = do
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl <> "/api/crystal-filters")
  handleResponse response

retrieveCrystalFormData :: forall m. MonadAff m => m (Either String APICrystalFormDataResponse)
retrieveCrystalFormData = do
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl <> "/api/crystal-form-data")
  handleResponse response

retrieveJob :: forall m. MonadAff m => Int -> m (Either String JobResponse)
retrieveJob jobId = do
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl <> "/api/workflows/jobs/" <> show jobId)
  handleResponse response

retrieveJobs :: forall m. MonadAff m => Int -> Maybe String -> Maybe String -> Maybe Since -> m (Either String JobsResponse)
retrieveJobs limit statusFilter tagFilter humanDuration = do
  let
    statusFilterSuffix = case statusFilter of
      Nothing -> []
      Just f -> [ "statusFilter=" <> f ]

    tagFilterSuffix = case tagFilter of
      Nothing -> []
      Just f -> [ "tagFilter=" <> f ]

    humanDurationSuffix = case humanDuration of
      Nothing -> []
      Just f -> [ "since=" <> serializeSince f ]

    parameters = [ "limit=" <> show limit ] <> statusFilterSuffix <> tagFilterSuffix <> humanDurationSuffix
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl <> "/api/workflows/jobs?" <> intercalate "&" parameters)
  handleResponse response

retrieveOverview :: forall m. MonadAff m => Maybe String -> m (Either String OverviewResponse)
retrieveOverview query = do
  response <- liftAff $ AX.post ResponseFormat.json (baseUrl <> "/api/overview") (Just (FormURLEncoded (fromArray ([ Tuple "query" query ]))))
  handleResponse response

retrievePucks :: forall m. MonadAff m => m (Either String APIPucksResponse)
retrievePucks = do
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl <> "/api/pucks")
  handleResponse response

retrieveTargets :: forall m. MonadAff m => m (Either String APITargetsResponse)
retrieveTargets = do
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl <> "/api/targets")
  handleResponse response

retrieveCrystals :: forall m. MonadAff m => Maybe String -> SortOrder -> m (Either String APICrystalsResponse)
retrieveCrystals sortColumn sortOrder = do
  let
    queryString = (maybe [] (\sc -> [ "sortColumn=" <> sc ]) sortColumn) <> [ "sortOrder=" <> sortToString sortOrder ]
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl <> "/api/crystals?" <> intercalate "&" queryString)
  handleResponse response

retrieveDewarTable :: forall m. MonadAff m => m (Either String DewarResponse)
retrieveDewarTable = do
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl <> "/api/dewar")
  handleResponse response

retrieveDiffractions :: forall m. MonadAff m => String -> m (Either String DiffractionResponse)
retrieveDiffractions puckId = do
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl <> "/api/diffraction/" <> puckId)
  handleResponse response

mapToObject :: Map.Map String String -> Json
mapToObject m =
  let
    -- Map with value String -> Map with value Json
    -- Map with value Json to List of tuples
    tuples :: Array (Tuple String Json)
    tuples = Map.toUnfoldable (fromString <$> m)

    -- List of Tuples to Object
    obj :: FO.Object Json
    obj = FO.fromFoldable tuples
  in
    -- Object to argonaut Json
    fromObject obj

-- startJob :: Int -> ToolInputMap -> String -> String -> Maybe Int -> m (Either String RunJobResponse)
-- startJob toolId inputs comment filterQuery limit = do
--   baseUrl' <- asks (_.baseUrl)
--   let
--     url :: String
--     url = baseUrl' <> "/api/workflows/jobs/start/" <> show toolId
--   response <-
--     liftAff
--       $ AX.post ResponseFormat.json url
--           ( Just
--               ( Json
--                   ( encodeJson
--                       { inputs: mapToObject inputs
--                       , filterQuery
--                       , comment
--                       , limit
--                       }
--                   )
--               )
--           )
--   handleResponse response

addTool :: forall m. MonadAff m => Tool -> m (Either String ToolsResponse)
addTool tool = do
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl <> "/api/workflows/tools")
          ( Just
              ( FormURLEncoded
                  ( fromArray
                      ( [ Tuple "name" (Just tool.name)
                        , Tuple "executablePath" (Just tool.executablePath)
                        , Tuple "extraFiles" (Just (stringify (Argonaut.fromArray (Argonaut.fromString <$> tool.extraFiles))))
                        , Tuple "commandLine" (Just tool.commandLine)
                        , Tuple "description" (Just tool.description)
                        ]
                      )
                  )
              )
          )
  handleResponse response

editTool :: forall m. MonadAff m => Tool -> m (Either String ToolsResponse)
editTool tool = do
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl <> "/api/workflows/tools/" <> show tool.toolId)
          ( Just
              ( FormURLEncoded
                  ( fromArray
                      ( [ Tuple "name" (Just tool.name)
                        , Tuple "executablePath" (Just tool.executablePath)
                        , Tuple "extraFiles" (Just (stringify (Argonaut.fromArray (Argonaut.fromString <$> tool.extraFiles))))
                        , Tuple "commandLine" (Just tool.commandLine)
                        , Tuple "description" (Just tool.description)
                        ]
                      )
                  )
              )
          )
  handleResponse response

addCrystal :: forall m. MonadAff m => APIAddCrystalRequest -> m (Either String APIAddCrystalResponse)
addCrystal { puckId, puckPosition, targetId, wellId, crystalTrayId, dropId, dateOfHarvesting, creator } = do
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl <> "/api/crystals")
          ( Just
              ( Json $ encodeJson { puckId, puckPosition, targetId, wellId, crystalTrayId, dropId, dateOfHarvesting: printDateTimeInIso dateOfHarvesting, creator }
              )
          )
  handleResponse response

puckTypeToAPI :: PuckType -> String
puckTypeToAPI Uni = "UNI"

puckTypeToAPI Spine = "SPINE"

addPuck :: forall m. MonadAff m => String -> PuckType -> m (Either String APIPucksResponse)
addPuck puckId puckType = do
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl <> "/api/pucks")
          ( Just
              ( FormURLEncoded
                  ( fromArray
                      ( [ Tuple "puckId" (Just puckId)
                        , Tuple "puckType" (Just (puckTypeToAPI puckType))
                        ]
                      )
                  )
              )
          )
  handleResponse response

addTarget :: forall m. MonadAff m => String -> String -> m (Either String APITargetsResponse)
addTarget targetId targetName = do
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl <> "/api/targets")
          ( Just
              ( FormURLEncoded
                  ( fromArray
                      ( [ Tuple "targetId" (Just targetId)
                        , Tuple "name" (Just targetName)
                        ]
                      )
                  )
              )
          )
  handleResponse response

addPuckToTable :: forall m. MonadAff m => Int -> String -> m (Either String DewarResponse)
addPuckToTable dewarPosition puckId = do
  let
    url :: String
    url = (baseUrl <> "/api/dewar/" <> show dewarPosition <> "/" <> puckId)
  response <- liftAff $ AX.get ResponseFormat.json url
  handleResponse response

removeSingleDewarEntry :: forall m. MonadAff m => Int -> m (Either String DewarResponse)
removeSingleDewarEntry dewarPosition = do
  response <- liftAff $ AX.delete ResponseFormat.json (baseUrl <> "/api/dewar/" <> show dewarPosition)
  handleResponse response

removeTool :: forall m. MonadAff m => Int -> m (Either String ToolsResponse)
removeTool toolId = do
  response <- liftAff $ AX.delete ResponseFormat.json (baseUrl <> "/api/workflows/tools/" <> show toolId)
  handleResponse response

removePuck :: forall m. MonadAff m => String -> m (Either String APIPucksResponse)
removePuck puckId = do
  response <- liftAff $ AX.delete ResponseFormat.json (baseUrl <> "/api/pucks/" <> puckId)
  handleResponse response

removeTarget :: forall m. MonadAff m => String -> m (Either String APITargetsResponse)
removeTarget targetId = do
  response <- liftAff $ AX.delete ResponseFormat.json (baseUrl <> "/api/targets/" <> targetId)
  handleResponse response

removeCrystal :: forall m. MonadAff m => String -> m (Either String APIRemoveCrystalResponse)
removeCrystal crystalId = do
  response <- liftAff $ AX.delete ResponseFormat.json (baseUrl <> "/api/crystals/" <> crystalId)
  handleResponse response

removeWholeTable :: forall m. MonadAff m => m (Either String DewarResponse)
removeWholeTable = do
  response <- liftAff $ AX.delete ResponseFormat.json (baseUrl <> "/api/dewar")
  handleResponse response

addDiffraction :: forall m. MonadAff m => 
  { crystalId :: String
  , runId :: Int
  , diffraction :: String
  , comment :: String
  } ->
  String ->
  m (Either String DiffractionResponse)
addDiffraction { crystalId, runId, diffraction, comment } puckId = do
  let
    url :: String
    url = (baseUrl <> "/api/diffraction/" <> puckId)
  response <-
    liftAff
      $ AX.post ResponseFormat.json url
          ( Just
              ( FormURLEncoded
                  ( fromArray
                      ( [ Tuple "crystalId" (Just crystalId)
                        , Tuple "runId" (Just (show runId))
                        , Tuple "diffraction" (Just diffraction)
                        , Tuple "comment" (Just comment)
                        ]
                      )
                  )
              )
          )
  handleResponse response

retrieveEvents :: forall m. MonadAff m => m (Either String EventsResponse)
retrieveEvents = do
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl <> "/api/events")
  handleResponse response

retrieveMiniSamples :: forall m. MonadAff m => m (Either String MiniSamplesResponse)
retrieveMiniSamples = do
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl <> "/api/minisamples")
  handleResponse response

retrieveAnalysis :: forall m. MonadAff m => String -> Maybe QualifiedColumn -> SortOrder -> Array QualifiedColumn -> Int -> m (Either String AnalysisResponse)
retrieveAnalysis filterQuery sortColumn order columns limit = do
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl <> "/api/analysis")
          ( Just
              ( Json
                  ( encodeJson
                      { sortColumn: qualifiedColumnToString <$> sortColumn
                      , sortOrderDesc: order == Descending
                      , filterQuery
                      , columns: qualifiedColumnToString <$> columns
                      , limit
                      }
                  )
              )
          )
  handleResponse response

createDownloadLink :: String -> Maybe QualifiedColumn -> SortOrder -> Array QualifiedColumn -> String
createDownloadLink filterQuery sortColumn order columns =
  let
    values :: Array String
    values = fromMaybe (((append "sortColumn=") <<< qualifiedColumnToString) <$> sortColumn) <> [ "sortOrderDesc=" <> (if order == Descending then "true" else "false"), "columns=" <> (intercalate "," (qualifiedColumnToString <$> columns)), "filterQuery=" <> filterQuery ]
  in
    baseUrl <> "/api/analysis-download?" <> intercalate "&" values

changeRunSample :: forall m. MonadAff m => Int -> Int -> m (Either String {})
changeRunSample runId sampleId = do
  let
    url :: String
    url = (baseUrl <> ("/api/change_run_sample/" <> show runId <> "/" <> show sampleId))
  response <- liftAff $ AX.get ResponseFormat.json url
  handleResponse response
