module App.RunScalar where

import Prelude

import Data.Maybe (Maybe(..))

data RunScalar
  = RunScalarNumber Number
  | RunScalarInt Int
  | RunScalarString String

derive instance eqRunScalar :: Eq RunScalar

instance ordRunScalar :: Ord RunScalar where
  compare (RunScalarNumber a) (RunScalarNumber b) = compare a b
  compare (RunScalarInt a) (RunScalarInt b) = compare a b
  compare (RunScalarString a) (RunScalarString b) = compare a b
  compare _ _ = EQ

runScalarNumber :: RunScalar -> Maybe Number
runScalarNumber (RunScalarNumber r) = Just r

runScalarNumber _ = Nothing

runScalarInt :: RunScalar -> Maybe Int
runScalarInt (RunScalarInt r) = Just r

runScalarInt _ = Nothing

instance showRunScalar :: Show RunScalar where
  show (RunScalarNumber n) = show n
  show (RunScalarInt n) = show n
  show (RunScalarString n) = n

