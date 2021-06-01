module App.Route where

import Prelude

import App.API (AnalysisColumn, analysisColumnToString, stringToAnalysisColumn)
import App.SortOrder (SortOrder, sortFromString, sortToString)
import Data.Generic.Rep (class Generic)
import Data.Maybe (Maybe)
import Effect (Effect)
import Routing.Duplex (RouteDuplex, RouteDuplex', as, optional, parse, print, root, string)
import Routing.Duplex.Generic as G
import Routing.Duplex.Generic.Syntax ((?))
import Routing.Hash (matchesWith)

data Route = Root | Beamline BeamlineRouteInput | Analysis AnalysisRouteInput

derive instance genericRoute :: Generic Route _

derive instance eqRoute :: Eq Route

sameRoute :: Route -> Route -> Boolean
sameRoute Root Root = true
sameRoute (Analysis _) (Analysis _) = true
sameRoute (Beamline _) (Beamline _) = true
sameRoute _ _ = false

type BeamlineRouteInput = {
   puckId :: Maybe String
  }

type AnalysisRouteInput = {
    sortColumn :: AnalysisColumn
  , sortOrder :: SortOrder
  }

sortOrder :: RouteDuplex String String -> RouteDuplex SortOrder SortOrder
sortOrder = as sortToString sortFromString

analysisColumn :: RouteDuplex String String -> RouteDuplex AnalysisColumn AnalysisColumn
analysisColumn = as analysisColumnToString stringToAnalysisColumn

routeCodec :: RouteDuplex' Route
routeCodec =
  root
    $ G.sum
        { "Root": G.noArgs
        , "Beamline": "beamline" ? { puckId: optional <<< string }
        , "Analysis": "analysis" ? { sortColumn: analysisColumn, sortOrder: sortOrder }
        }

matchRoute :: (Maybe Route -> Route -> Effect Unit) -> Effect (Effect Unit)
matchRoute = matchesWith (parse routeCodec)

createLink :: Route -> String
createLink x = "#" <> print routeCodec x
