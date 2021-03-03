module App.JSONSchemaType where

import Prelude

import Data.Argonaut (class DecodeJson, JsonDecodeError(..), decodeJson, (.:))
import Data.Either (Either(..))

data JSONSchemaType
  = JSONNumber
  | JSONString
  | JSONArray
  | JSONInteger

derive instance eqJSONSchemaType :: Eq JSONSchemaType

instance jsonSchemaTypeDecode :: DecodeJson JSONSchemaType where
  decodeJson json = do
    obj <- decodeJson json
    type_ <- obj .: "type"
    case type_ of
      "number" -> pure JSONNumber
      "integer" -> pure JSONInteger
      "string" -> pure JSONString
      "array" -> pure JSONArray
      _ -> Left (TypeMismatch $ "invalid \"type\" property: " <> type_)
