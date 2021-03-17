module App.ScalarAttributo where

import Prelude

import Data.Maybe (Maybe(..))

data ScalarAttributo
  = ScalarAttributoNumber Number
  | ScalarAttributoInt Int
  | ScalarAttributoString String

derive instance eqScalarAttributo :: Eq ScalarAttributo
derive instance ordScalarAttributo :: Ord ScalarAttributo

scalarAttributoNumber :: ScalarAttributo -> Maybe Number
scalarAttributoNumber (ScalarAttributoNumber r) = Just r

scalarAttributoNumber _ = Nothing

scalarAttributoInt :: ScalarAttributo -> Maybe Int
scalarAttributoInt (ScalarAttributoInt r) = Just r

scalarAttributoInt _ = Nothing

instance showScalarAttributo :: Show ScalarAttributo where
  show (ScalarAttributoNumber n) = show n
  show (ScalarAttributoInt n) = show n
  show (ScalarAttributoString n) = n

