module App.Run where

import Prelude

import App.Comment (Comment)
import App.RunScalar (RunScalar(..), runScalarInt)
import App.RunValue (RunValue, _Comments, runValueScalar)
import Data.Argonaut (class DecodeJson, Json, JsonDecodeError, decodeJson)
import Data.Either (Either)
import Data.Lens (Traversal', traversed)
import Data.Lens.Index (ix)
import Data.Lens.Iso.Newtype (_Newtype)
import Data.List (toUnfoldable)
import Data.Map (Map, insert, lookup, values)
import Data.Maybe (Maybe, fromMaybe)
import Data.Newtype (class Newtype)
import Foreign.Object (foldM)


newtype Run
  = Run (Map String RunValue)

derive instance newtypeRun :: Newtype Run _

runLookup :: Run -> String -> Maybe RunValue
runLookup (Run r) s = lookup s r

runValues :: Run -> Array RunValue
runValues (Run r) = toUnfoldable (values r)

runComments :: Traversal' Run Comment
runComments = _Newtype <<< ix "comments" <<< _Comments <<< traversed

instance showRun :: Show Run where
  show (Run r) = show r

runId :: Run -> Int
runId (Run r) = fromMaybe 0 (lookup "id" r >>= runValueScalar >>= runScalarInt)

runScalarProperty :: String -> Run -> RunScalar
runScalarProperty prop run = fromMaybe (RunScalarNumber 0.0) (runLookup run prop >>= runValueScalar)

instance eqRun :: Eq Run where
  eq (Run a) (Run b) = a == b

instance ordRun :: Ord Run where
  compare = comparing runId

instance decodeRun :: DecodeJson Run where
  decodeJson json = do
    obj <- decodeJson json
    let
      folder :: Map String RunValue -> String -> Json -> Either JsonDecodeError (Map String RunValue)
      folder previousMap newKey newValue = (\decodedValue -> insert newKey decodedValue previousMap) <$> (decodeJson newValue)
    result <- foldM folder mempty obj
    pure (Run result)
