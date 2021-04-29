module App.AssociatedTable where

import Control.Bind (bind)
import Data.Argonaut (class DecodeJson, class EncodeJson, decodeJson, fromString)
import Data.Argonaut.Decode (JsonDecodeError(..))
import Data.Either (note)
import Data.Eq (class Eq)
import Data.Function ((<<<))
import Data.Maybe (Maybe(..))
import Data.Ord (class Ord)
import Data.Show (class Show, show)

data AssociatedTable = Run | Sample | Analysis

derive instance eqAssociatedTable :: Eq AssociatedTable
derive instance ordAssociatedTable :: Ord AssociatedTable

instance showAssociatedTable :: Show AssociatedTable where
  show Run = "run"
  show Sample = "sample"
  show Analysis = "analysis"

associatedTableFromString :: String -> Maybe AssociatedTable
associatedTableFromString "run" = Just Run
associatedTableFromString "sample" = Just Sample
associatedTableFromString "analysis" = Just Analysis
associatedTableFromString _ = Nothing

instance associatedTableJsonDecode :: DecodeJson AssociatedTable where
  decodeJson json = do
    string <- decodeJson json
    note (TypeMismatch "AssociatedTable") (associatedTableFromString string)

instance associatedTableJsonEncode :: EncodeJson AssociatedTable where
  encodeJson = fromString <<< show
