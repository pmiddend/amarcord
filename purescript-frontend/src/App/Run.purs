module App.Run where

-- import Prelude

-- import App.Comment (Comment)
-- import App.ScalarAttributo (ScalarAttributo(..), scalarAttributoInt)
-- import App.AttributoValue (AttributoValue, _Comments)
-- import Data.Argonaut (class DecodeJson, Json, JsonDecodeError, decodeJson)
-- import Data.Array (mapMaybe)
-- import Data.Either (Either)
-- import Data.Lens (Traversal', Lens', traversed)
-- import Data.Lens.Index (ix)
-- import Data.Lens.Iso.Newtype (_Newtype)
-- import Data.Lens.Record (prop)
-- import Data.Maybe (Maybe, fromMaybe)
-- import Data.Newtype (class Newtype)
-- import Data.Symbol (SProxy(..))
-- import Data.Traversable (traverse)
-- import Data.Tuple (Tuple(..))
-- import Foreign.Object (Object, foldM, foldMap, lookup, toUnfoldable)


-- type Run
--   = { id :: Int
--     , sample_id :: Maybe Int
--     , modified :: String
--     , comments :: Array Comment
--     , attributi :: Attributi
--     }

-- _attributi :: Lens' Run Attributi
-- _attributi = prop (SProxy :: SProxy "attributi")

-- type Source = String



-- derive instance newtypeRun :: Newtype Run _

-- runLookup :: Run -> String -> Maybe AttributoValue
-- runLookup (Run r) s = lookup s r

-- attributos :: Run -> Array AttributoValue
-- attributos (Run r) = toUnfoldable (values r)

-- runComments :: Traversal' Run Comment
-- runComments = _Newtype <<< ix "comments" <<< _Comments <<< traversed

-- instance showRun :: Show Run where
--   show (Run r) = show r

-- runId :: Run -> Int
-- runId (Run r) = fromMaybe 0 (lookup "id" r >>= attributoScalar >>= runScalarInt)

-- runScalarProperty :: String -> Run -> ScalarAttributo
-- runScalarProperty prop run = fromMaybe (ScalarAttributoNumber 0.0) (runLookup run prop >>= attributoScalar)

-- instance eqRun :: Eq Run where
--   eq (Run a) (Run b) = a == b

-- instance ordRun :: Ord Run where
--   compare = comparing runId

-- instance decodeRun :: DecodeJson Run where
--   decodeJson json = do
--     obj <- decodeJson json
--     let
--       folder :: Map String AttributoValue -> String -> Json -> Either JsonDecodeError (Map String AttributoValue)
--       folder previousMap newKey newValue = (\decodedValue -> insert newKey decodedValue previousMap) <$> (decodeJson newValue)
--     result <- foldM folder mempty obj
--     pure (Run result)
