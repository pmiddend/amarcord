module App.Route where

import Prelude

import App.SortOrder (SortOrder, sortFromString, sortToString)
import Data.Generic.Rep (class Generic)
import Data.Maybe (Maybe)
import Effect (Effect)
import Routing.Duplex (RouteDuplex', as, parse, print, root, string)
import Routing.Duplex.Generic as G
import Routing.Duplex.Generic.Syntax ((?))
import Routing.Hash (matchesWith)

type RunsRouteInput = { sort :: String, sortOrder :: SortOrder }

data Route
  = Root
  | Runs RunsRouteInput

derive instance genericRoute :: Generic Route _

derive instance eqRoute :: Eq Route

sort = as sortToString sortFromString

routeCodec :: RouteDuplex' Route
routeCodec = root $ G.sum {
    "Root": G.noArgs
  , "Runs": "runs" ? { sort: string, sortOrder: sort }
  }

matchRoute :: (Maybe Route -> Route -> Effect Unit) -> Effect (Effect Unit)
matchRoute = matchesWith (parse routeCodec)

createLink :: Route -> String
createLink x = "#" <> print routeCodec x
