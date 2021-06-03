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
import Control.Monad.Reader (asks)
import Data.Argonaut (class DecodeJson, JsonDecodeError)
import Data.Argonaut.Core (Json, stringifyWithIndent)
import Data.Argonaut.Decode (decodeJson, printJsonDecodeError)
import Data.Either (Either(..))
import Data.FormURLEncoded (fromArray)
import Data.Maybe (Maybe(..))
import Data.Tuple (Tuple(..))
import Halogen (liftAff)

type OverviewResponse
  = { overviewRows :: Array OverviewRow
    }

data AnalysisColumn
  = CrystalID
  | AnalysisTime
  | Puck
  | RunID
  | Comment
  | DataReductionID
  | ResolutionCC
  | ResolutionIsigI
  | CellA
  | CellB
  | CellC
  | CellAlpha
  | CellBeta
  | CellGamma

derive instance eqRoute :: Eq AnalysisColumn

derive instance ordRoute :: Ord AnalysisColumn

analysisColumnToString :: AnalysisColumn -> String
analysisColumnToString CrystalID = "crystalId"

analysisColumnToString AnalysisTime = "analysisTime"

analysisColumnToString Puck = "puck"

analysisColumnToString RunID = "runId"

analysisColumnToString Comment = "comment"

analysisColumnToString DataReductionID = "drid"

analysisColumnToString ResolutionCC = "resCC"

analysisColumnToString ResolutionIsigI = "resI"

analysisColumnToString CellA = "a"

analysisColumnToString CellB = "b"

analysisColumnToString CellC = "c"

analysisColumnToString CellAlpha = "alpha"

analysisColumnToString CellBeta = "beta"

analysisColumnToString CellGamma = "gamma"

stringToAnalysisColumn :: String -> Either String AnalysisColumn
stringToAnalysisColumn "crystalId" = Right CrystalID

stringToAnalysisColumn "analysisTime" = Right AnalysisTime

stringToAnalysisColumn "puck" = Right Puck

stringToAnalysisColumn "runId" = Right RunID

stringToAnalysisColumn "comment" = Right Comment

stringToAnalysisColumn "drid" = Right DataReductionID

stringToAnalysisColumn "resCC" = Right ResolutionCC

stringToAnalysisColumn "resI" = Right ResolutionIsigI

stringToAnalysisColumn "a" = Right CellA

stringToAnalysisColumn "b" = Right CellB

stringToAnalysisColumn "c" = Right CellC

stringToAnalysisColumn "alpha" = Right CellAlpha

stringToAnalysisColumn "beta" = Right CellBeta

stringToAnalysisColumn "gamma" = Right CellGamma

stringToAnalysisColumn val = Left ("not a valid column: " <> val)

type AnalysisRow
  = { crystalId :: String
    , analysisTime :: String
    , puckId :: Maybe String
    , puckPositionId :: Maybe Int
    , runId :: Int
    , comment :: Maybe String
    , dataReductionId :: Int
    , resolutionCc :: Maybe Number
    , resolutionIsigma :: Maybe Number
    , a :: Number
    , b :: Number
    , c :: Number
    , alpha :: Number
    , beta :: Number
    , gamma :: Number
    }

type AnalysisResponse
  = { analysis :: Array AnalysisRow
    }

type AttributiResponse
  = { attributi :: Array Attributo
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

type SampleResponse
  = { pucks :: Array Puck
    , crystals :: Array Crystal
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

removePuck :: String -> AppMonad (Either String SampleResponse)
removePuck puckId = do
  baseUrl' <- asks (_.baseUrl)
  response <- liftAff $ AX.delete ResponseFormat.json (baseUrl' <> "/api/pucks/" <> puckId)
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

retrieveAnalysis :: AnalysisColumn -> SortOrder -> AppMonad (Either String AnalysisResponse)
retrieveAnalysis col order = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/api/analysis?sortColumn=" <> analysisColumnToString col <> "&sortOrder=" <> sortToString order)
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
