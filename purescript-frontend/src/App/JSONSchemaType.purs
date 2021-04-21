module App.JSONSchemaType where

import Prelude

import App.NumericRange (NumericRange, fromMaybes)
import Data.Argonaut (class DecodeJson, JsonDecodeError(..), decodeJson, (.:), (.:?))
import Data.Either (Either(..))
import Data.Lens (Lens')
import Data.Lens.Prism (Prism', prism')
import Data.Lens.Record (prop)
import Data.Maybe (Maybe(..))
import Data.Symbol (SProxy(..))

type JSONNumberData
  = { suffix :: Maybe String
    , range :: Maybe (NumericRange Number)
    }

_suffix :: Lens' JSONNumberData (Maybe String)
_suffix = prop (SProxy :: SProxy "suffix")

_range :: Lens' JSONNumberData (Maybe (NumericRange Number))
_range = prop (SProxy :: SProxy "range")

type JSONArrayData = {
    minItems :: Maybe Int
  , maxItems :: Maybe Int
  , items :: JSONSchemaType
  }

data JSONSchemaType
  = JSONNumber JSONNumberData
  | JSONString
  | JSONArray JSONArrayData
  | JSONComments
  | JSONInteger

_JSONNumber :: Prism' JSONSchemaType JSONNumberData
_JSONNumber = prism' JSONNumber case _ of
  JSONNumber x -> Just x
  _ -> Nothing

derive instance eqJSONSchemaType :: Eq JSONSchemaType
derive instance ordJSONSchemaType :: Ord JSONSchemaType

instance showJsonSchema :: Show JSONSchemaType where
  show JSONString = "string"
  show (JSONArray ad) = "array"
  show JSONInteger = "int"
  show JSONComments = "comments"
  show (JSONNumber _) = "number"

instance jsonSchemaTypeDecode :: DecodeJson JSONSchemaType where
  decodeJson json = do
    obj <- decodeJson json
    type_ <- obj .: "type"
    case type_ of
      "number" -> do
        minimum <- obj .:? "minimum"
        maximum <- obj .:? "maximum"
        exclusiveMinimum <- obj .:? "exclusiveMinimum"
        exclusiveMaximum <- obj .:? "exclusiveMaximum"
        suffix <- obj .:? "suffix"
        pure
          ( JSONNumber
              { suffix: suffix
              , range: fromMaybes minimum exclusiveMinimum maximum exclusiveMaximum
              }
          )
      "integer" -> pure JSONInteger
      "string" -> pure JSONString
      "array" -> do
        minItems <- obj .:? "minItems"
        maxItems <- obj .:? "maxItems"
        format <- obj .:? "format"
        case format of
          Just "comments" -> pure JSONComments
          _ -> do
            items <- obj .: "items"
            pure (JSONArray { minItems, maxItems, items })
      _ -> Left (TypeMismatch $ "invalid \"type\" property: " <> type_)
