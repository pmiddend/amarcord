module App.JSONSchemaType where

import Prelude
import App.NumericRange (NumericRange, fromMaybes)
import Data.Argonaut (class DecodeJson, JsonDecodeError(..), decodeJson, (.:), (.:?))
import Data.Either (Either(..))
import Data.Lens (Lens')
import Data.Lens.Record (prop)
import Data.Maybe (Maybe)
import Data.Symbol (SProxy(..))

type JSONNumberData
  = { suffix :: Maybe String
    , range :: Maybe (NumericRange Number)
    }

_suffix :: Lens' JSONNumberData (Maybe String)
_suffix = prop (SProxy :: SProxy "suffix")

_range :: Lens' JSONNumberData (Maybe (NumericRange Number))
_range = prop (SProxy :: SProxy "range")

data JSONSchemaType
  = JSONNumber JSONNumberData
  | JSONString
  | JSONArray
  | JSONInteger

derive instance eqJSONSchemaType :: Eq JSONSchemaType

instance showJsonSchema :: Show JSONSchemaType where
  show JSONString = "string"
  show JSONArray = "array"
  show JSONInteger = "int"
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
      "array" -> pure JSONArray
      _ -> Left (TypeMismatch $ "invalid \"type\" property: " <> type_)
