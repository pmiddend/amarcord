module App.QualifiedAttributoName where

import App.AssociatedTable (AssociatedTable, associatedTableFromString)
import Control.Applicative ((<*>))
import Control.Bind ((>>=))
import Data.Array ((!!))
import Data.Either (Either(..))
import Data.Functor ((<$>))
import Data.Maybe (Maybe, maybe)
import Data.Semigroup ((<>))
import Data.Show (show)
import Data.String (Pattern(..), split)
import Data.Tuple (Tuple(..))

type QualifiedAttributoName = Tuple AssociatedTable String

qanToString :: QualifiedAttributoName -> String
qanToString (Tuple table name) = show table <> "." <> name

qanFromString :: String -> Either String QualifiedAttributoName
qanFromString s =
  let parts = split (Pattern ".") s
      result :: Maybe QualifiedAttributoName
      result = Tuple <$> ((parts !! 0) >>= associatedTableFromString) <*> (parts !! 1)
  in maybe (Left "invalid number of dots in attributo name") Right result
