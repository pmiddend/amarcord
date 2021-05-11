module App.Route where

import Prelude

import Data.Generic.Rep (class Generic)
import Data.Maybe (Maybe)
import Effect (Effect)
import Routing.Duplex (RouteDuplex', optional, parse, print, root, string)
import Routing.Duplex.Generic as G
import Routing.Duplex.Generic.Syntax ((?))
import Routing.Hash (matchesWith)

data Route = Root | Beamline BeamlineRouteInput

derive instance genericRoute :: Generic Route _

derive instance eqRoute :: Eq Route

sameRoute :: Route -> Route -> Boolean
sameRoute Root Root = true
sameRoute (Beamline _) (Beamline _) = true
sameRoute _ _ = false

type BeamlineRouteInput = {
   puckId :: Maybe String
  }

routeCodec :: RouteDuplex' Route
routeCodec =
  root
    $ G.sum
        { "Root": G.noArgs
        , "Beamline": "beamline" ? { puckId: optional <<< string }
        }

matchRoute :: (Maybe Route -> Route -> Effect Unit) -> Effect (Effect Unit)
matchRoute = matchesWith (parse routeCodec)

createLink :: Route -> String
createLink x = "#" <> print routeCodec x
