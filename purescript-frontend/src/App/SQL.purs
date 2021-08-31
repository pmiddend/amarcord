module App.SQL where

import Data.Array (intercalate)
import Data.Functor ((<$>))
import Data.Maybe (Maybe(..))
import Data.Monoid ((<>))

type SqlComparison = {
    column :: String
  , value :: Maybe String
  }

type SqlCondition = Array SqlComparison

quote :: String -> String
quote x = "\"" <> x <> "\""

sqlConditionToString :: SqlCondition -> String
sqlConditionToString terms = intercalate " AND " (sqlComparisonToString <$> terms)

sqlComparisonToString :: SqlComparison -> String
sqlComparisonToString { column, value: Nothing } = column <> " IS NULL"
sqlComparisonToString { column, value: Just v } = column <> "=" <> quote v
