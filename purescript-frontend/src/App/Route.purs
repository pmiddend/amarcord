module App.Route where

import Prelude
import App.SortOrder (SortOrder, sortFromString, sortToString)
import Data.Generic.Rep (class Generic)
import Data.Maybe (Maybe)
import Effect (Effect)
import Routing.Duplex (RouteDuplex, RouteDuplex', as, optional, parse, path, print, root, string, int)
import Routing.Duplex.Generic as G
import Routing.Duplex.Generic.Syntax ((?))
import Routing.Hash (matchesWith)

data Route
  = Root
  | Beamline BeamlineRouteInput
  | Analysis AnalysisRouteInput
  | Sample
  | Tools ToolsRouteInput
  | Jobs
  | Job JobRouteInput
  | ToolsAdmin

derive instance genericRoute :: Generic Route _

derive instance eqRoute :: Eq Route

sameRoute :: Route -> Route -> Boolean
sameRoute Root Root = true

sameRoute Sample Sample = true
sameRoute Jobs Jobs = true
sameRoute ToolsAdmin ToolsAdmin = true

sameRoute (Analysis _) (Analysis _) = true

sameRoute (Beamline _) (Beamline _) = true

sameRoute (Tools _) (Tools _) = true

sameRoute _ _ = false

type BeamlineRouteInput
  = { puckId :: Maybe String
    }
    
type JobRouteInput
  = { jobId :: Int
    }

type AnalysisRouteInput
  = { sortColumn :: String
    , sortOrder :: SortOrder
    , filterQuery :: String
    }

type ToolsRouteInput = {}

sortOrder :: RouteDuplex String String -> RouteDuplex SortOrder SortOrder
sortOrder = as sortToString sortFromString

routeCodec :: RouteDuplex' Route
routeCodec =
  root
    $ G.sum
        { "Root": G.noArgs
        , "Sample": path "sample" G.noArgs
        , "Beamline": "beamline" ? { puckId: optional <<< string }
        , "Job": "job" ? { jobId: int }
        , "Analysis": "analysis" ? { sortColumn: string, sortOrder: sortOrder, filterQuery: string }
        , "Tools": "tools" ? {}
        , "Jobs": path "jobs" G.noArgs
        , "ToolsAdmin": path "toolsadmin" G.noArgs
        }

matchRoute :: (Maybe Route -> Route -> Effect Unit) -> Effect (Effect Unit)
matchRoute = matchesWith (parse routeCodec)

createLink :: Route -> String
createLink x = "#" <> print routeCodec x
