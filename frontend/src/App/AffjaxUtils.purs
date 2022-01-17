module App.AffjaxUtils
  ( handleResponse
  ) where

import Affjax (Error, Response, printError)
import Affjax.StatusCode (StatusCode(..))
import App.RequestError (RequestError)
import Control.Applicative (pure)
import Data.Argonaut (class DecodeJson, Json, JsonDecodeError, decodeJson, printJsonDecodeError, stringifyWithIndent)
import Data.Either (Either(..))
import Data.Maybe (Maybe(..))
import Data.Semigroup ((<>))
import Effect.Aff.Class (class MonadAff)

type ErrorResponse =
  { error :: RequestError
  }

handleResponse :: forall a m. MonadAff m => DecodeJson a => Either Error (Response Json) -> m (Either RequestError a)
handleResponse response = do
  case response of
    Left httpError -> pure
      ( Left
          { code: Nothing
          , title: "Request error"
          , description: printError httpError
          }
      )
    Right httpResult ->
      case httpResult.status of
        StatusCode 200 ->
          let
            decodedError :: Either JsonDecodeError ErrorResponse
            decodedError = decodeJson httpResult.body
          in
            case decodedError of
              Right { error: e } -> pure (Left e)
              Left _ -> case decodeJson httpResult.body of
                Left jsonError -> pure (Left { code: Nothing, title: "Error decoding JSON response", description: printJsonDecodeError jsonError })
                Right jsonResult -> pure (Right jsonResult)
        StatusCode sc -> pure
          ( Left
              { code: Just sc
              , title: "HTTP error"
              , description: "JSON response:\n" <> stringifyWithIndent 2 httpResult.body
              }
          )
