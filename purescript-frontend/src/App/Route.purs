module App.Route where

import Prelude

import App.SortOrder (SortOrder, sortFromString, sortToString)
import Data.Generic.Rep (class Generic)
import Data.Maybe (Maybe)
import Effect (Effect)
import Routing.Duplex (RouteDuplex, RouteDuplex', as, parse, print, root, string)
import Routing.Duplex.Generic as G
import Routing.Duplex.Generic.Syntax ((?))
import Routing.Hash (matchesWith)

type OverviewRouteInput = { sort :: String, sortOrder :: SortOrder }

data Route
  = Root
  | Overview OverviewRouteInput
--  | EditRun Int

derive instance genericRoute :: Generic Route _

derive instance eqRoute :: Eq Route

sort :: RouteDuplex String String -> RouteDuplex SortOrder SortOrder
sort = as sortToString sortFromString

routeCodec :: RouteDuplex' Route
routeCodec = root $ G.sum {
    "Root": G.noArgs
  , "Overview": "overview" ? { sort: string, sortOrder: sort }
--  , "EditRun": path "editRun" (int segment)
  }

matchRoute :: (Maybe Route -> Route -> Effect Unit) -> Effect (Effect Unit)
matchRoute = matchesWith (parse routeCodec)

createLink :: Route -> String
createLink x = "#" <> print routeCodec x
