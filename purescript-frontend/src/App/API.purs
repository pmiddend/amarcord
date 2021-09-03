module App.API where

import Prelude

import Affjax (Error, Response, printError)
import Affjax as AX
import Affjax.RequestBody (RequestBody(..))
import Affjax.ResponseFormat as ResponseFormat
import Affjax.StatusCode (StatusCode(..))
import App.AppMonad (AppMonad)
import App.Attributo (Attributo)
import App.Event (Event)
import App.MiniSample (MiniSample)
import App.Overview (OverviewRow)
import App.SortOrder (SortOrder, sortToString)
import App.Utils (stringMapToJsonObject, stringMaybeMapToJsonObject)
import Control.Monad.Reader (asks)
import Data.Argonaut (class DecodeJson, JsonDecodeError, fromObject, fromString, stringify)
import Data.Argonaut as Argonaut
import Data.Argonaut.Core (Json, stringifyWithIndent)
import Data.Argonaut.Decode (decodeJson, printJsonDecodeError)
import Data.Argonaut.Encode (encodeJson)
import Data.Either (Either(..))
import Data.Foldable (intercalate)
import Data.FormURLEncoded (fromArray)
import Data.Map (Map)
import Data.Map as Map
import Data.Maybe (Maybe(..), maybe)
import Data.Tuple (Tuple(..), fst, snd)
import Foreign.Object as FO
import Halogen (liftAff)
import URI.Query as Query

type OverviewResponse
  = { overviewRows :: Array OverviewRow
    }

type AnalysisRow
  = Array Json

data Since
  = LastDay
  | LastWeek
  | LastMonth

derive instance eqSince :: Eq Since

derive instance ordSince :: Ord Since

serializeSince :: Since -> String
serializeSince LastDay = "last_day"

serializeSince LastWeek = "last_week"

serializeSince LastMonth = "last_month"

deserializeSince :: String -> Maybe Since
deserializeSince "last_day" = Just LastDay

deserializeSince "last_week" = Just LastWeek

deserializeSince "last_month" = Just LastMonth

deserializeSince _ = Nothing

type AnalysisResponse
  = { analysis :: Array AnalysisRow
    , analysisColumns :: Array String
    , totalRows :: Int
    , totalDiffractions :: Int
    , totalReductions :: Int
    , sqlError :: Maybe String
    }

type AttributiResponse
  = { attributi :: Array Attributo
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

type Puck
  = { puckId :: String, puckType :: String }

type Crystal
  = { crystalId :: String, puckId :: Maybe String, puckPosition :: Maybe Int }

type PucksResponse
  = { pucks :: Array Puck
    }

type CrystalFilter
  = { columnName :: String
    , values :: Array (Maybe String)
    }

type CrystalFilterResponse
  = { columnsWithValues :: Array CrystalFilter
    }

type SampleResponse
  = { pucks :: Array Puck
    , crystals :: Array Crystal
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
    , comment :: String
    , metadata :: Maybe Json
    , outputDir :: Maybe String
    , toolId :: Int
    , toolInputs :: Json
    , lastStderr :: Maybe String
    , lastStdout :: Maybe String
    }

type AugmentedJob
  = { jobId :: Int
    , started :: Maybe String
    , stopped :: Maybe String
    , queued :: String
    , status :: String
    , failureReason :: Maybe String
    , comment :: String
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
    }

type JobResponse
  = { job :: Job
    }

type DewarEntry
  = { puckId :: String, dewarPosition :: Int }

type DewarResponse
  = { dewarTable :: Array DewarEntry
    }

type DiffractionEntry
  = { crystalId :: String
    , runId :: Maybe Int
    , dewarPosition :: Maybe Int
    , diffraction :: Maybe String
    , comment :: Maybe String
    , puckPositionId :: Int
    }

type DiffractionResponse
  = { diffractions :: Array DiffractionEntry }

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

retrieveReductionCount :: Map String (Maybe String) -> Maybe String -> Boolean -> AppMonad (Either String ReductionCountResult)
retrieveReductionCount crystalFiltersMap reductionMethod onlyNonrefined = do
  baseUrl' <- asks (_.baseUrl)
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl' <> "/api/reduction-count")
          ( Just
              ( Json
                  ( encodeJson
                      { reductionMethod
                      , onlyNonrefined
                      , crystalFilters: stringMaybeMapToJsonObject crystalFiltersMap
                      }
                  )
              )
          )
  handleResponse response

startJobsSimple :: Int -> String -> Maybe Int -> Map String String -> Map String (Maybe String) -> Maybe String -> Boolean -> AppMonad (Either String RunJobResponse)
startJobsSimple toolId comment limit toolInputs crystalFiltersMap reductionMethod onlyNonrefined = do
  baseUrl' <- asks (_.baseUrl)
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl' <> "/api/workflows/jobs-simple/start/" <> show toolId)
          ( Just
              ( Json
                  ( encodeJson
                      { reductionMethod
                      , onlyNonrefined
                      , limit
                      , comment
                      , inputs: stringMapToJsonObject toolInputs
                      , crystalFilters: stringMaybeMapToJsonObject crystalFiltersMap
                      }
                  )
              )
          )
  handleResponse response

retrieveTools :: AppMonad (Either String ToolsResponse)
retrieveTools = do
  baseUrl' <- asks (_.baseUrl)
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl' <> "/api/workflows/tools")
  handleResponse response

retrieveCrystalFilters :: AppMonad (Either String CrystalFilterResponse)
retrieveCrystalFilters = do
  baseUrl' <- asks (_.baseUrl)
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl' <> "/api/crystal-filters")
  handleResponse response

retrieveJob :: Int -> AppMonad (Either String JobResponse)
retrieveJob jobId = do
  baseUrl' <- asks (_.baseUrl)
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl' <> "/api/workflows/jobs/" <> show jobId)
  handleResponse response

retrieveJobs :: Int -> Maybe String -> Maybe Since -> AppMonad (Either String JobsResponse)
retrieveJobs limit statusFilter humanDuration = do
  baseUrl' <- asks (_.baseUrl)
  let
    statusFilterSuffix = case statusFilter of
      Nothing -> []
      Just f -> [ "statusFilter=" <> f ]

    humanDurationSuffix = case humanDuration of
      Nothing -> []
      Just f -> [ "since=" <> serializeSince f ]

    parameters = [ "limit=" <> show limit ] <> statusFilterSuffix <> humanDurationSuffix
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl' <> "/api/workflows/jobs?" <> intercalate "&" parameters)
  handleResponse response

retrieveOverview :: Maybe String -> AppMonad (Either String OverviewResponse)
retrieveOverview query = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/api/overview")
  response <- liftAff $ AX.post ResponseFormat.json url (Just (FormURLEncoded (fromArray ([ Tuple "query" query ]))))
  handleResponse response

retrievePucks :: AppMonad (Either String PucksResponse)
retrievePucks = do
  baseUrl' <- asks (_.baseUrl)
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl' <> "/api/pucks")
  handleResponse response

retrieveSample :: AppMonad (Either String SampleResponse)
retrieveSample = do
  baseUrl' <- asks (_.baseUrl)
  response <- liftAff $ AX.get ResponseFormat.json (baseUrl' <> "/api/sample")
  handleResponse response

retrieveDewarTable :: AppMonad (Either String DewarResponse)
retrieveDewarTable = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/api/dewar")
  response <- liftAff $ AX.get ResponseFormat.json url
  handleResponse response

retrieveDiffractions :: String -> AppMonad (Either String DiffractionResponse)
retrieveDiffractions puckId = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/api/diffraction/" <> puckId)
  response <- liftAff $ AX.get ResponseFormat.json url
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

startJob :: Int -> ToolInputMap -> String -> String -> Maybe Int -> AppMonad (Either String RunJobResponse)
startJob toolId inputs comment filterQuery limit = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = baseUrl' <> "/api/workflows/jobs/start/" <> show toolId
  response <-
    liftAff
      $ AX.post ResponseFormat.json url
          ( Just
              ( Json
                  ( encodeJson
                      { inputs: mapToObject inputs
                      , filterQuery
                      , comment
                      , limit
                      }
                  )
              )
          )
  handleResponse response

addTool :: Tool -> AppMonad (Either String ToolsResponse)
addTool tool = do
  baseUrl' <- asks (_.baseUrl)
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl' <> "/api/workflows/tools")
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

editTool :: Tool -> AppMonad (Either String ToolsResponse)
editTool tool = do
  baseUrl' <- asks (_.baseUrl)
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl' <> "/api/workflows/tools/" <> show tool.toolId)
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

addCrystal :: String -> Maybe (Tuple String Int) -> AppMonad (Either String SampleResponse)
addCrystal crystalId puckIdAndPosition = do
  baseUrl' <- asks (_.baseUrl)
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl' <> "/api/crystals")
          ( Just
              ( FormURLEncoded
                  ( fromArray
                      ( [ Tuple "puckId" (Just (maybe "" fst puckIdAndPosition))
                        , Tuple "crystalId" (Just crystalId)
                        , Tuple "puckPosition" (Just (maybe "0" (show <<< snd) puckIdAndPosition))
                        ]
                      )
                  )
              )
          )
  handleResponse response

addPuck :: String -> AppMonad (Either String PucksResponse)
addPuck puckId = do
  baseUrl' <- asks (_.baseUrl)
  response <-
    liftAff
      $ AX.post ResponseFormat.json (baseUrl' <> "/api/pucks")
          ( Just
              ( FormURLEncoded
                  ( fromArray
                      ( [ Tuple "puckId" (Just puckId)
                        ]
                      )
                  )
              )
          )
  handleResponse response

addPuckToTable :: Int -> String -> AppMonad (Either String DewarResponse)
addPuckToTable dewarPosition puckId = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/api/dewar/" <> show dewarPosition <> "/" <> puckId)
  response <- liftAff $ AX.get ResponseFormat.json url
  handleResponse response

removeSingleDewarEntry :: Int -> AppMonad (Either String DewarResponse)
removeSingleDewarEntry dewarPosition = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/api/dewar/" <> show dewarPosition)
  response <- liftAff $ AX.delete ResponseFormat.json url
  handleResponse response

removeTool :: Int -> AppMonad (Either String ToolsResponse)
removeTool toolId = do
  baseUrl' <- asks (_.baseUrl)
  response <- liftAff $ AX.delete ResponseFormat.json (baseUrl' <> "/api/workflows/tools/" <> show toolId)
  handleResponse response

removePuck :: String -> AppMonad (Either String SampleResponse)
removePuck puckId = do
  baseUrl' <- asks (_.baseUrl)
  response <- liftAff $ AX.delete ResponseFormat.json (baseUrl' <> "/api/pucks/" <> puckId)
  handleResponse response

removeCrystal :: String -> AppMonad (Either String SampleResponse)
removeCrystal crystalId = do
  baseUrl' <- asks (_.baseUrl)
  response <- liftAff $ AX.delete ResponseFormat.json (baseUrl' <> "/api/crystals/" <> crystalId)
  handleResponse response

removeWholeTable :: AppMonad (Either String DewarResponse)
removeWholeTable = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/api/dewar")
  response <- liftAff $ AX.delete ResponseFormat.json url
  handleResponse response

addDiffraction ::
  { crystalId :: String
  , runId :: Int
  , diffraction :: String
  , beamIntensity :: String
  , comment :: String
  } ->
  String ->
  AppMonad (Either String DiffractionResponse)
addDiffraction { crystalId, runId, diffraction, beamIntensity, comment } puckId = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/api/diffraction/" <> puckId)
  response <-
    liftAff
      $ AX.post ResponseFormat.json url
          ( Just
              ( FormURLEncoded
                  ( fromArray
                      ( [ Tuple "crystalId" (Just crystalId)
                        , Tuple "runId" (Just (show runId))
                        , Tuple "diffraction" (Just diffraction)
                        , Tuple "beamIntensity" (Just beamIntensity)
                        , Tuple "comment" (Just comment)
                        ]
                      )
                  )
              )
          )
  handleResponse response

retrieveAttributi :: AppMonad (Either String AttributiResponse)
retrieveAttributi = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/api/attributi")
  response <- liftAff $ AX.get ResponseFormat.json url
  handleResponse response

retrieveEvents :: AppMonad (Either String EventsResponse)
retrieveEvents = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/api/events")
  response <- liftAff $ AX.get ResponseFormat.json url
  handleResponse response

retrieveMiniSamples :: AppMonad (Either String MiniSamplesResponse)
retrieveMiniSamples = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/api/minisamples")
  response <- liftAff $ AX.get ResponseFormat.json url
  handleResponse response

retrieveAnalysis :: String -> String -> SortOrder -> AppMonad (Either String AnalysisResponse)
retrieveAnalysis filterQuery col order = do
  baseUrl' <- asks (_.baseUrl)
  --log Info ("to stringed" <>  (toString (fromString filterQuery)))
  let
    url :: String
    url = (baseUrl' <> "/api/analysis?sortColumn=" <> col <> "&sortOrder=" <> sortToString order) <> "&filterQuery=" <> (Query.unsafeToString (Query.fromString filterQuery))
  response <- liftAff $ AX.get ResponseFormat.json url
  handleResponse response

changeRunSample :: Int -> Int -> AppMonad (Either String {})
changeRunSample runId sampleId = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> ("/api/change_run_sample/" <> show runId <> "/" <> show sampleId))
  response <- liftAff $ AX.get ResponseFormat.json url
  handleResponse response
