module App.Attributo where

import App.AssociatedTable (AssociatedTable)
import App.JSONSchemaType (JSONSchemaType, _JSONNumber, _suffix)
import App.QualifiedAttributoName (QualifiedAttributoName)
import Data.Function ((<<<))
import Data.Lens (Lens', Traversal', traversed)
import Data.Lens.Record (prop)
import Data.Maybe (Maybe(..))
import Data.Symbol (SProxy(..))
import Data.Tuple (Tuple(..))

type Attributo
  = { name :: String
    , description :: Maybe String
    , typeSchema :: JSONSchemaType
    , table :: AssociatedTable
    }

_typeSchema :: Lens' Attributo JSONSchemaType
_typeSchema = prop (SProxy :: SProxy "typeSchema")

attributoSuffix :: Traversal' Attributo String
attributoSuffix = _typeSchema <<< _JSONNumber <<< _suffix <<< traversed

qualifiedAttributoName :: Attributo -> QualifiedAttributoName
qualifiedAttributoName a = Tuple a.table a.name

descriptiveAttributoText :: Attributo -> String
descriptiveAttributoText a =
  case a.description of
    Nothing -> a.name
    Just "" -> a.name
    Just d -> d
