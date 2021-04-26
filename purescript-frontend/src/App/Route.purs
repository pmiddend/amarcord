module App.Route where

import Prelude

import App.PlotType (PlotType, plotTypeFromString)
import App.QualifiedAttributoName (QualifiedAttributoName, qanFromString, qanToString)
import App.SortOrder (SortOrder, sortFromString, sortToString)
import Data.Generic.Rep (class Generic)
import Data.Maybe (Maybe)
import Effect (Effect)
import Routing.Duplex (RouteDuplex, RouteDuplex', as, optional, parse, path, print, root)
import Routing.Duplex.Generic as G
import Routing.Duplex.Generic.Syntax ((?))
import Routing.Hash (matchesWith)

type OverviewRouteInput
  = { sort :: QualifiedAttributoName, sortOrder :: SortOrder }

type GraphsRouteInput
  = { xAxis :: Maybe QualifiedAttributoName
    , yAxis :: Maybe QualifiedAttributoName
    , plotType :: PlotType
    }

data Route
  = Root
  | Overview OverviewRouteInput
  | Graphs GraphsRouteInput
  | Events

derive instance genericRoute :: Generic Route _

derive instance eqRoute :: Eq Route

sortOrder :: RouteDuplex String String -> RouteDuplex SortOrder SortOrder
sortOrder = as sortToString sortFromString

qualifiedAttributoName :: RouteDuplex String String -> RouteDuplex QualifiedAttributoName QualifiedAttributoName
qualifiedAttributoName = as qanToString qanFromString

plotType :: RouteDuplex String String -> RouteDuplex PlotType PlotType
plotType = as show plotTypeFromString

routeCodec :: RouteDuplex' Route
routeCodec =
  root
    $ G.sum
        { "Root": G.noArgs
        , "Events": path "events" G.noArgs
        , "Graphs":
            "graphs"
              ? { xAxis: optional <<< qualifiedAttributoName
                , yAxis: optional <<< qualifiedAttributoName
                , plotType: plotType
                }
        , "Overview":
            "overview"
              ? { sort: qualifiedAttributoName
                , sortOrder: sortOrder
                }
        }

matchRoute :: (Maybe Route -> Route -> Effect Unit) -> Effect (Effect Unit)
matchRoute = matchesWith (parse routeCodec)

createLink :: Route -> String
createLink x = "#" <> print routeCodec x
