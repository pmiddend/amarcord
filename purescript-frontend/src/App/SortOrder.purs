module App.SortOrder where

import Prelude

import Data.Ordering (invert)

data SortOrder = Ascending | Descending

derive instance eqSortOrder :: Eq SortOrder

invertOrder :: SortOrder -> SortOrder
invertOrder Ascending = Descending
invertOrder _ = Ascending

comparing :: forall a b. Ord b => SortOrder -> (a -> b) -> a -> a -> Ordering
comparing Ascending getter f g = compare (getter f) (getter g)
comparing Descending getter f g = invert (compare (getter f) (getter g))
