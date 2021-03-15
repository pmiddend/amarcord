module App.NumericRange where

import Data.Maybe (Maybe(..))
import Data.Ord ((<), (<=), (>), (>=))
import Data.Tuple (Tuple(..))
import Prelude (class Eq, class Ord, class Show, show, (&&), (<>))

data NumericBorder a
  = Open a
  | Closed a
  | Infinity

derive instance eqNumericBorder :: Eq a => Eq (NumericBorder a)

type NumericRange a
  = { left :: NumericBorder a
    , right :: NumericBorder a
    }

fromMaybes :: forall a. Maybe a -> Maybe a -> Maybe a -> Maybe a -> Maybe (NumericRange a)
fromMaybes Nothing Nothing Nothing Nothing = Nothing
fromMaybes minimum exclusiveMinimum maximum exclusiveMaximum =
  let
    fromMaybes' (Tuple (Just v) Nothing) = Just (Open v)

    fromMaybes' (Tuple Nothing (Just v)) = Just (Closed v)

    fromMaybes' (Tuple Nothing Nothing) = Just Infinity

    fromMaybes' _ = Nothing
  in
    case fromMaybes' (Tuple exclusiveMinimum minimum) of
      Nothing -> Nothing
      Just left -> case fromMaybes' (Tuple  exclusiveMaximum maximum) of
        Nothing -> Nothing
        Just right -> Just ({ left, right })

prettyPrintRange :: forall a. Show a => NumericRange a -> String
prettyPrintRange { left, right } = strLeft <> "," <> strRight
  where strLeft = case left of
          Open a -> "(" <> show a
          Closed a -> "[" <> show a
          Infinity -> "(∞"
        strRight = case right of
          Open a -> show a <> ")"
          Closed a -> show a <> "]"
          Infinity -> "∞)"

inRange :: forall a. Ord a => NumericRange a -> a -> Boolean
inRange { left, right } v = leftInRange left && rightInRange right
  where leftInRange Infinity = true
        leftInRange (Closed x) = v >= x
        leftInRange (Open x) = v > x
        rightInRange Infinity = true
        rightInRange (Closed x) = v <= x
        rightInRange (Open x) = v < x
