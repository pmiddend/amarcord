module App.AttributoValue where

import Prelude
import App.Comment (Comment)
import Data.Argonaut (class DecodeJson, class EncodeJson, JsonDecodeError(..), decodeJson, encodeJson, isArray, isNumber, isString)
import Data.Array (intercalate)
import Data.Either (Either(..))
import Data.Int (fromNumber)
import Data.Lens.Prism (Prism', prism')
import Data.Maybe (Maybe(..), maybe)

data AttributoValue
  = Comments (Array Comment)
  | IntegralAttributo Int
  | NumericAttributo Number
  | StringAttributo String

derive instance eqAttributoValue :: Eq AttributoValue

derive instance ordAttributoValue :: Ord AttributoValue

instance showAttributoValue :: Show AttributoValue where
  show (Comments cs) = intercalate "," (show <$> cs)
  show (IntegralAttributo i) = show i
  show (NumericAttributo i) = show i
  show (StringAttributo i) = i

-- attributoScalar :: AttributoValue -> Maybe ScalarAttributo
-- attributoScalar (Scalar r) = Just r
-- attributoScalar _ = Nothing
attributoComments :: AttributoValue -> Maybe (Array Comment)
attributoComments (Comments a) = Just a

attributoComments _ = Nothing

_Comments :: Prism' AttributoValue (Array Comment)
_Comments = prism' Comments attributoComments

-- instance showAttributoValue :: Show AttributoValue where
--   show (Scalar x) = show x
--   show (Comments xs) = show xs
numberOrInt :: Number -> AttributoValue
numberOrInt x = maybe (NumericAttributo x) IntegralAttributo (fromNumber x)

instance attributoDecoder :: DecodeJson AttributoValue where
  decodeJson json = do
    if isArray json then
      Comments <$> decodeJson json
    else if isNumber json then
      numberOrInt <$> decodeJson json
    else if isString json then
      StringAttributo <$> decodeJson json
    else
      (Left (TypeMismatch "invalid type"))

instance attributoEncoder :: EncodeJson AttributoValue where
  encodeJson v = case v of
    (Comments _) ->
      let
        comments :: Array Comment
        comments = []
      in
        encodeJson comments
    IntegralAttributo v -> encodeJson v
    NumericAttributo v -> encodeJson v
    StringAttributo v -> encodeJson v
