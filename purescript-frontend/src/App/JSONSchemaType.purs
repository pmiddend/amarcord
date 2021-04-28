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

type JSONIntegerData
  = { format :: Maybe String
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

data JSONSchemaStringFormat = JSONStringDateTime
                            | JSONStringDuration
                            | JSONStringUserName
                            | JSONStringTag

derive instance eqJSONSchemaStringFormat :: Eq JSONSchemaStringFormat
derive instance ordJSONSchemaStringFormat :: Ord JSONSchemaStringFormat

data JSONSchemaType
  = JSONNumber JSONNumberData
  | JSONString (Maybe JSONSchemaStringFormat)
  | JSONArray JSONArrayData
  | JSONComments
  | JSONInteger JSONIntegerData

_JSONNumber :: Prism' JSONSchemaType JSONNumberData
_JSONNumber = prism' JSONNumber case _ of
  JSONNumber x -> Just x
  _ -> Nothing

derive instance eqJSONSchemaType :: Eq JSONSchemaType
derive instance ordJSONSchemaType :: Ord JSONSchemaType

instance showJsonSchema :: Show JSONSchemaType where
  show (JSONString _) = "string"
  show (JSONArray ad) = "array"
  show (JSONInteger _) = "int"
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
      "integer" -> do
        format <- obj .:? "format"
        pure (JSONInteger { format })
      "string" -> do
        format <- obj .:? "format"
        pure case format of
          Just "date-time" -> JSONString (Just JSONStringDateTime)
          Just "duration" -> JSONString (Just JSONStringDuration)
          Just "user-name" -> JSONString (Just JSONStringUserName)
          Just "tag" -> JSONString (Just JSONStringTag)
          _ -> (JSONString Nothing)
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
