module App.Route where

import Prelude

import App.QualifiedAttributoName (QualifiedAttributoName, qanFromString, qanToString)
import App.SortOrder (SortOrder, sortFromString, sortToString)
import Data.Generic.Rep (class Generic)
import Data.Maybe (Maybe)
import Effect (Effect)
import Routing.Duplex (RouteDuplex, RouteDuplex', as, parse, print, root)
import Routing.Duplex.Generic as G
import Routing.Duplex.Generic.Syntax ((?))
import Routing.Hash (matchesWith)

type OverviewRouteInput = { sort :: QualifiedAttributoName, sortOrder :: SortOrder }

data Route
  = Root
  | Overview OverviewRouteInput
--  | EditRun Int

derive instance genericRoute :: Generic Route _

derive instance eqRoute :: Eq Route

sortOrder :: RouteDuplex String String -> RouteDuplex SortOrder SortOrder
sortOrder = as sortToString sortFromString

qualifiedAttributoName :: RouteDuplex String String -> RouteDuplex QualifiedAttributoName QualifiedAttributoName
qualifiedAttributoName = as qanToString qanFromString
        

routeCodec :: RouteDuplex' Route
routeCodec = root $ G.sum {
    "Root": G.noArgs
  , "Overview": "overview" ? { sort: qualifiedAttributoName, sortOrder: sortOrder }
--  , "EditRun": path "editRun" (int segment)
  }

matchRoute :: (Maybe Route -> Route -> Effect Unit) -> Effect (Effect Unit)
matchRoute = matchesWith (parse routeCodec)

createLink :: Route -> String
createLink x = "#" <> print routeCodec x
