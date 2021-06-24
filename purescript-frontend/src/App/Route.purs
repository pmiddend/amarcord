module App.Route where

import Prelude

import App.SortOrder (SortOrder, sortFromString, sortToString)
import Data.Generic.Rep (class Generic)
import Data.Maybe (Maybe)
import Effect (Effect)
import Routing.Duplex (RouteDuplex, RouteDuplex', as, optional, parse, path, print, root, string)
import Routing.Duplex.Generic as G
import Routing.Duplex.Generic.Syntax ((?))
import Routing.Hash (matchesWith)

data Route = Root | Beamline BeamlineRouteInput | Analysis AnalysisRouteInput | Sample

derive instance genericRoute :: Generic Route _

derive instance eqRoute :: Eq Route

sameRoute :: Route -> Route -> Boolean
sameRoute Root Root = true
sameRoute Sample Sample = true
sameRoute (Analysis _) (Analysis _) = true
sameRoute (Beamline _) (Beamline _) = true
sameRoute _ _ = false

type BeamlineRouteInput = {
   puckId :: Maybe String
  }

type AnalysisRouteInput = {
    sortColumn :: String
  , sortOrder :: SortOrder
  , filterQuery :: String
  }

sortOrder :: RouteDuplex String String -> RouteDuplex SortOrder SortOrder
sortOrder = as sortToString sortFromString

routeCodec :: RouteDuplex' Route
routeCodec =
  root
    $ G.sum
        { "Root": G.noArgs
        , "Sample": path "sample" G.noArgs
        , "Beamline": "beamline" ? { puckId: optional <<< string }
        , "Analysis": "analysis" ? { sortColumn: string, sortOrder: sortOrder, filterQuery: string }
        }

matchRoute :: (Maybe Route -> Route -> Effect Unit) -> Effect (Effect Unit)
matchRoute = matchesWith (parse routeCodec)

createLink :: Route -> String
createLink x = "#" <> print routeCodec x
