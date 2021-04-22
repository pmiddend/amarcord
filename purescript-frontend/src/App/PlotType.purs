module App.PlotType where

import Data.Either (Either(..))
import Data.Eq (class Eq)
import Data.Semigroup ((<>))
import Data.Show (class Show)

data PlotType
  = Line
  | Scatter

derive instance eqPlotType :: Eq PlotType

instance showPlotType :: Show PlotType where
  show Line = "line"
  show Scatter = "scatter"

plotTypeFromString :: String -> Either String PlotType
plotTypeFromString = case _ of
  "line" -> Right Line
  "scatter" -> Right Scatter
  val -> Left ("Not a plot type: " <> val)
