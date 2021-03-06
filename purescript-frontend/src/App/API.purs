module App.API where

import Prelude

import Affjax (Error, Response, printError)
import Affjax as AX
import Affjax.RequestBody (RequestBody(..))
import Affjax.ResponseFormat as ResponseFormat
import Affjax.StatusCode (StatusCode(..))
import App.AppMonad (AppMonad)
import App.Env (Env)
import App.Run (Run)
import App.RunProperty (RunProperty)
import App.UnfinishedComment (UnfinishedComment)
import Control.Monad.Reader (class MonadAsk, asks)
import Data.Argonaut (class DecodeJson, JsonDecodeError, encodeJson)
import Data.Argonaut.Core (Json, stringifyWithIndent)
import Data.Argonaut.Decode (decodeJson, printJsonDecodeError)
import Data.Either (Either(..))
import Data.Maybe (Maybe(..))
import Effect.Aff.Class (class MonadAff)
import Halogen (liftAff)

type RunsResponse
  = { runs :: Array Run
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

retrieveRuns :: AppMonad (Either String RunsResponse)
retrieveRuns = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/1/runs")
  response <- liftAff $ AX.get ResponseFormat.json url
  handleResponse response

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
  handleResponse response

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
  handleResponse response

addComment :: forall m. MonadAsk Env m => MonadAff m => Int -> UnfinishedComment -> m (Either String {})
addComment runId comment = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/run/" <> show runId <> "/comment")
  response <- liftAff $ AX.post ResponseFormat.json url (Just (Json (encodeJson comment)))
  handleResponse response

deleteComment :: forall m. MonadAsk Env m => MonadAff m => Int -> Int -> m (Either String {})
deleteComment rid cid = do
  baseUrl' <- asks (_.baseUrl)
  let
    url :: String
    url = (baseUrl' <> "/run/" <> show rid <> "/comment/" <> show cid)
  response <- liftAff $ AX.delete ResponseFormat.json url
  handleResponse response
