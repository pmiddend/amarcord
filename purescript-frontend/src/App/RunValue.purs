module App.RunValue where

import Prelude

import App.Comment (Comment)
import App.RunScalar (RunScalar(..))
import Data.Argonaut (class DecodeJson, JsonDecodeError(..), decodeJson, isArray, isNumber, isString)
import Data.Either (Either(..))
import Data.Int (fromNumber)
import Data.Lens.Prism (Prism', prism')
import Data.Maybe (Maybe(..), maybe)

data RunValue
  = Comments (Array Comment)
  | Scalar RunScalar

derive instance eqRunValue :: Eq RunValue


runValueScalar :: RunValue -> Maybe RunScalar
runValueScalar (Scalar r) = Just r

runValueScalar _ = Nothing

runValueComments :: RunValue -> Maybe (Array Comment)
runValueComments (Comments a) = Just a
runValueComments _ = Nothing

_Comments :: Prism' RunValue (Array Comment)
_Comments = prism' Comments runValueComments

instance showRunValue :: Show RunValue where
  show (Scalar x) = show x
  show (Comments xs) = show xs

numberOrInt :: Number -> RunScalar
numberOrInt x = maybe (RunScalarNumber x) RunScalarInt (fromNumber x)

instance runValueDecoder :: DecodeJson RunValue where
  decodeJson json = do
    if isArray json then
      Comments <$> decodeJson json
    else if isNumber json then
      (Scalar <<< numberOrInt) <$> decodeJson json
    else if isString json then
      (Scalar <<< RunScalarString) <$> decodeJson json
    else
      (Left (TypeMismatch "invalid type"))


