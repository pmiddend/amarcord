module App.SortOrder where

import Prelude

import Data.Either (Either(..))
import Data.Generic.Rep (class Generic)
import Data.Ordering (invert)

data SortOrder = Ascending | Descending

derive instance eqSortOrder :: Eq SortOrder
derive instance genericSort :: Generic SortOrder _

invertOrder :: SortOrder -> SortOrder
invertOrder Ascending = Descending
invertOrder _ = Ascending

comparing :: forall a b. Ord b => SortOrder -> (a -> b) -> a -> a -> Ordering
comparing Ascending getter f g = compare (getter f) (getter g)
comparing Descending getter f g = invert (compare (getter f) (getter g))

sortToString :: SortOrder -> String
sortToString = case _ of
  Ascending -> "asc"
  Descending -> "desc"

sortFromString :: String -> Either String SortOrder
sortFromString = case _ of
  "asc" -> Right Ascending
  "desc" -> Right Descending
  val -> Left $ "Not a sort: " <> val
