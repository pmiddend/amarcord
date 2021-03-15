module App.Attributo where

import Prelude

import App.JSONSchemaType (JSONSchemaType(..))
import App.NumericRange (inRange)
import Data.Argonaut (class DecodeJson, decodeJson, (.:), (.:?))
import Data.Lens (Lens')
import Data.Lens.Iso.Newtype (_Newtype)
import Data.Lens.Record (prop)
import Data.Maybe (Maybe(..))
import Data.Newtype (class Newtype)
import Data.Symbol (SProxy(..))

newtype Attributo
  = Attributo
  { description :: String
  , name :: String
  , type_schema :: Maybe JSONSchemaType
  }

derive instance newtypeAttributo :: Newtype Attributo _

validateNumeric :: Number -> Attributo -> Boolean
validateNumeric n (Attributo p) = case p.type_schema of
  Nothing -> true
  Just (JSONNumber { range: Just range }) -> inRange range n
  Just (JSONNumber { range: Nothing }) -> true
  _ -> false

rpDescription :: Attributo -> String
rpDescription (Attributo p) = p.description

_description :: Lens' Attributo String
_description = _Newtype <<< prop (SProxy :: SProxy "description")

_name :: Lens' Attributo String
_name = _Newtype <<< prop (SProxy :: SProxy "name")


rpName :: Attributo -> String
rpName (Attributo p) = p.name

rpType :: Attributo -> Maybe JSONSchemaType
rpType (Attributo p) = p.type_schema

rpIsSortable :: Attributo -> Boolean
rpIsSortable (Attributo r) = case r.type_schema of
  Nothing -> false
  Just JSONArray -> false
  _ -> true

derive newtype instance eqAttributo :: Eq Attributo

instance runPropertyDecode :: DecodeJson Attributo where
  decodeJson json = do
    obj <- decodeJson json
    description <- obj .: "description"
    name <- obj .: "name"
    type_schema <- obj .:? "type_schema"
    pure
      $ Attributo
          { description, name, type_schema
          }

instance ordAttributo :: Ord Attributo where
  compare (Attributo x) (Attributo y) = comparing (_.name) x y
