module App.API where

import Prelude

import Affjax (Error, Response, printError)
import Affjax as AX
import Affjax.ResponseFormat as ResponseFormat
import Affjax.StatusCode (StatusCode(..))
import App.AppMonad (AppMonad)
import App.Attributo (Attributo)
import App.Event (Event)
import App.Overview (OverviewRow)
import Control.Monad.Reader (asks)
import Data.Argonaut (class DecodeJson, JsonDecodeError)
import Data.Argonaut.Core (Json, stringifyWithIndent)
import Data.Argonaut.Decode (decodeJson, printJsonDecodeError)
import Data.Either (Either(..))
import Halogen (liftAff)

type OverviewResponse
  = { overviewRows :: Array OverviewRow
    }

type AttributiResponse
  = { attributi :: Array Attributo
    }

type EventsResponse
  = { events :: Array Event
    }

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

retrieveOverview :: AppMonad (Either String OverviewResponse)
retrieveOverview = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/api/overview")
  response <- liftAff $ AX.get ResponseFormat.json url
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
