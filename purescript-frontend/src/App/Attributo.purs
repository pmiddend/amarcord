module App.Attributo where

import Prelude

import App.JSONSchemaType (JSONSchemaType(..))
import Data.Argonaut (class DecodeJson, decodeJson, (.:))
import Data.Lens (Lens')
import Data.Lens.Iso.Newtype (_Newtype)
import Data.Lens.Record (prop)
import Data.Newtype (class Newtype)
import Data.Symbol (SProxy(..))

newtype Attributo
  = Attributo
  { description :: String
  , name :: String
  , type_schema :: JSONSchemaType
  }

derive instance newtypeAttributo :: Newtype Attributo _

instance showAttributo :: Show Attributo where
  show (Attributo x) = show x

_description :: Lens' Attributo String
_description = _Newtype <<< prop (SProxy :: SProxy "description")

_name :: Lens' Attributo String
_name = _Newtype <<< prop (SProxy :: SProxy "name")

_typeSchema :: Lens' Attributo JSONSchemaType
_typeSchema = _Newtype <<< prop (SProxy :: SProxy "type_schema")

rpIsSortable :: Attributo -> Boolean
rpIsSortable (Attributo r) = case r.type_schema of
  JSONArray -> false
  _ -> true

derive newtype instance eqAttributo :: Eq Attributo

instance runPropertyDecode :: DecodeJson Attributo where
  decodeJson json = do
    obj <- decodeJson json
    description <- obj .: "description"
    name <- obj .: "name"
    type_schema <- obj .: "type_schema"
    pure
      $ Attributo
          { description, name, type_schema
          }

instance ordAttributo :: Ord Attributo where
  compare (Attributo x) (Attributo y) = comparing (_.name) x y
