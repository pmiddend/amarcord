module App.API where

import Prelude
import Affjax (printError)
import Affjax as AX
import Affjax.ResponseFormat as ResponseFormat
import Affjax.StatusCode (StatusCode(..))
import App.AppMonad (AppMonad)
import App.Run (Run)
import App.RunProperty (RunProperty)
import Control.Monad.Reader (asks)
import Data.Argonaut (JsonDecodeError)
import Data.Argonaut.Core (Json, stringifyWithIndent)
import Data.Argonaut.Decode (decodeJson, printJsonDecodeError)
import Data.Either (Either(..))
import Halogen (liftAff)

type RunsResponse
  = { runs :: Array Run
    }

retrieveRuns :: AppMonad (Either String RunsResponse)
retrieveRuns = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/1/runs")
  response <- liftAff $ AX.get ResponseFormat.json url
  case response of
    Left httpError -> pure (Left (printError httpError))
    Right httpResult -> do
      let
        httpBody :: Json
        httpBody = httpResult.body

        decodedJson :: Either JsonDecodeError RunsResponse
        decodedJson = decodeJson httpBody
      case httpResult.status of
        StatusCode 200 -> case decodedJson of
          Left jsonError -> pure (Left (printJsonDecodeError jsonError))
          Right jsonResult -> pure (Right jsonResult)
        StatusCode sc -> pure (Left ("Status code: " <> show sc <> "\n\nJSON response:\n" <> stringifyWithIndent 2 httpResult.body))

type RunPropertiesResponse
  = { metadata :: Array RunProperty
    }

retrieveRunProperties :: AppMonad (Either String RunPropertiesResponse)
retrieveRunProperties = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/run_properties")
  response <- liftAff $ AX.get ResponseFormat.json url
  case response of
    Left httpError -> pure (Left (printError httpError))
    Right httpResult -> case httpResult.status of
      StatusCode 200 -> case decodeJson (httpResult.body) of
        Left jsonError -> pure (Left (printJsonDecodeError jsonError))
        Right jsonResult -> pure (Right jsonResult)
      StatusCode sc -> pure (Left ("Status code: " <> show sc <> "\n\nJSON response:\n" <> stringifyWithIndent 2 httpResult.body))

type RunResponse
  = { run :: Run
    , manual_properties :: Array String
    }

retrieveRun :: Int -> AppMonad (Either String RunResponse)
retrieveRun runId = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/run/" <> show runId)
  response <- liftAff $ AX.get ResponseFormat.json url
  case response of
    Left httpError -> pure (Left (printError httpError))
    Right httpResult -> case httpResult.status of
      StatusCode 200 -> case decodeJson (httpResult.body) of
        Left jsonError -> pure (Left (printJsonDecodeError jsonError))
        Right jsonResult -> pure (Right jsonResult)
      StatusCode sc -> pure (Left ("Status code: " <> show sc <> "\n\nJSON response:\n" <> stringifyWithIndent 2 httpResult.body))
