module App.RunProperty where

import Prelude

import App.JSONSchemaType (JSONSchemaType(..))
import Data.Argonaut (class DecodeJson, decodeJson, (.:), (.:?))
import Data.Lens (Lens')
import Data.Lens.Iso.Newtype (_Newtype)
import Data.Lens.Record (prop)
import Data.Maybe (Maybe(..))
import Data.Symbol (SProxy(..))
import Data.Newtype (class Newtype)

newtype RunProperty
  = RunProperty
  { description :: String
  , name :: String
  , suffix :: Maybe String
  , type_schema :: Maybe JSONSchemaType
  }

derive instance newtypeRunProperty :: Newtype RunProperty _

rpDescription :: RunProperty -> String
rpDescription (RunProperty p) = p.description

_description :: Lens' RunProperty String
_description = _Newtype <<< prop (SProxy :: SProxy "description")

_name :: Lens' RunProperty String
_name = _Newtype <<< prop (SProxy :: SProxy "name")


rpName :: RunProperty -> String
rpName (RunProperty p) = p.name

rpType :: RunProperty -> Maybe JSONSchemaType
rpType (RunProperty p) = p.type_schema

rpIsSortable :: RunProperty -> Boolean
rpIsSortable (RunProperty r) = case r.type_schema of
  Nothing -> false
  Just JSONArray -> false
  _ -> true

derive newtype instance eqRunProperty :: Eq RunProperty

instance runPropertyDecode :: DecodeJson RunProperty where
  decodeJson json = do
    obj <- decodeJson json
    description <- obj .: "description"
    name <- obj .: "name"
    suffix <- obj .: "suffix"
    type_schema <- obj .:? "type_schema"
    pure
      $ RunProperty
          { description, name, suffix, type_schema
          }

instance ordRunProperty :: Ord RunProperty where
  compare (RunProperty x) (RunProperty y) = comparing (_.name) x y
