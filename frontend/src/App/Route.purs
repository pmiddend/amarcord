module App.Route
  ( Route(..)
  , createLink
  , matchRoute
  , routeCodec
  , sameRoute
  , sortOrder
  ) where

import App.SortOrder (SortOrder, sortFromString, sortToString)
import Data.Array (intercalate)
import Data.Either (Either(..))
import Data.Eq (class Eq)
import Data.Function (($), (<<<))
import Data.Functor (map, (<$>))
import Data.Generic.Rep (class Generic)
import Data.Maybe (Maybe(..))
import Data.Monoid (class Monoid, mempty, (<>))
import Data.Ord (class Ord)
import Data.Semigroup (class Semigroup)
import Data.String (Pattern(..), split)
import Data.Traversable (traverse)
import Data.Unit (Unit)
import Effect (Effect)
import Routing.Duplex (RouteDuplex, RouteDuplex', as, optional, parse, path, print, root, string, int)
import Routing.Duplex.Generic as G
import Routing.Duplex.Generic.Syntax ((?))
import Routing.Hash (matchesWith)

data Route = Root | Attributi

derive instance genericRoute :: Generic Route _

derive instance eqRoute :: Eq Route

sameRoute :: Route -> Route -> Boolean
sameRoute Root Root = true
sameRoute Attributi Attributi = true
sameRoute _ _ = false

sortOrder :: RouteDuplex String String -> RouteDuplex SortOrder SortOrder
sortOrder = as sortToString sortFromString

routeCodec :: RouteDuplex' Route
routeCodec =
  root
    $ G.sum
        { "Root": G.noArgs
        , "Attributi": path "attributi" G.noArgs
        }

matchRoute :: (Maybe Route -> Route -> Effect Unit) -> Effect (Effect Unit)
matchRoute = matchesWith (parse routeCodec)

createLink :: Route -> String
createLink x = "#" <> print routeCodec x
