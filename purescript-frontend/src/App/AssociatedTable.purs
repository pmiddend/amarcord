module App.AssociatedTable where

import Control.Bind (bind)
import Data.Argonaut (class DecodeJson, decodeJson)
import Data.Argonaut.Decode (JsonDecodeError(..))
import Data.Either (note)
import Data.Eq (class Eq)
import Data.Show(class Show)
import Data.Maybe (Maybe(..))
import Data.Ord (class Ord)

data AssociatedTable = Run | Sample

derive instance eqAssociatedTable :: Eq AssociatedTable
derive instance ordAssociatedTable :: Ord AssociatedTable

instance showAssociatedTable :: Show AssociatedTable where
  show Run = "Run"
  show Sample = "Sample"

associatedTableFromString :: String -> Maybe AssociatedTable
associatedTableFromString "run" = Just Run
associatedTableFromString "sample" = Just Sample
associatedTableFromString _ = Nothing

instance associatedTableJsonDecode :: DecodeJson AssociatedTable where
  decodeJson json = do
    string <- decodeJson json
    note (TypeMismatch "AssociatedTable") (associatedTableFromString string)
