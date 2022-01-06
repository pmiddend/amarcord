module App.Route where

import App.QualifiedColumn (QualifiedColumn, qualifiedColumnFromString, qualifiedColumnToString)
import App.Since (Since, deserializeSince, serializeSince)
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

data Route
  = Root
  | Beamline BeamlineRouteInput
  | Analysis AnalysisRouteInput
  | Crystals
  | Pucks
  | Targets
  | Tools ToolsRouteInput
  | Jobs JobListInput
  | Job JobRouteInput
  | CompoundOverview
  | P11Ingest
  | ToolsAdmin

derive instance genericRoute :: Generic Route _

derive instance eqRoute :: Eq Route

sameRoute :: Route -> Route -> Boolean
sameRoute Root Root = true

sameRoute CompoundOverview CompoundOverview = true

sameRoute Crystals Crystals = true
sameRoute Pucks Pucks = true
sameRoute Targets Targets = true
sameRoute P11Ingest P11Ingest = true

sameRoute (Jobs _) (Jobs _) = true

sameRoute ToolsAdmin ToolsAdmin = true

sameRoute (Analysis _) (Analysis _) = true

sameRoute (Beamline _) (Beamline _) = true

sameRoute (Tools _) (Tools _) = true

sameRoute _ _ = false

type BeamlineRouteInput
  =
  { puckId :: Maybe String
  , crystalId :: Maybe String
  }

type JobRouteInput
  =
  { jobId :: Int
  }

newtype SelectedColumns
  = SelectedColumns (Array QualifiedColumn)

instance semigroupSelectedColumns :: Semigroup SelectedColumns where
  append (SelectedColumns xs) (SelectedColumns ys) = SelectedColumns (xs <> ys)

instance monoidSelectedColumns :: Monoid SelectedColumns where
  mempty = SelectedColumns mempty

extractSelectedColumns :: SelectedColumns -> Array QualifiedColumn
extractSelectedColumns (SelectedColumns x) = x

derive instance eqSelectedColumns :: Eq SelectedColumns

derive instance ordSelectedColumns :: Ord SelectedColumns

type AnalysisRouteInput
  =
  { sortColumn :: Maybe QualifiedColumn
  , sortOrder :: SortOrder
  , filterQuery :: String
  , limit :: Int
  , selectedColumns :: SelectedColumns
  }

type JobListInput
  = { limit :: Int, statusFilter :: Maybe String, tagFilter :: Maybe String, durationFilter :: Maybe Since }

type ToolsRouteInput
  = {}

sortOrder :: RouteDuplex String String -> RouteDuplex SortOrder SortOrder
sortOrder = as sortToString sortFromString

qcFromStringEither :: String -> Either String QualifiedColumn
qcFromStringEither qc = case qualifiedColumnFromString qc of
  Nothing -> Left (qc <> " not a qualified column")
  Just qc' -> Right qc'

sinceFromStringEither :: String -> Either String Since
sinceFromStringEither qc = case deserializeSince qc of
  Nothing -> Left (qc <> " not a since value")
  Just qc' -> Right qc'

qualifiedColumnCodec :: RouteDuplex String String -> RouteDuplex QualifiedColumn QualifiedColumn
qualifiedColumnCodec = as qualifiedColumnToString qcFromStringEither

sinceCodec :: RouteDuplex String String -> RouteDuplex Since Since
sinceCodec = as serializeSince sinceFromStringEither

selectedColumnsCodec :: RouteDuplex String String -> RouteDuplex SelectedColumns SelectedColumns
selectedColumnsCodec = as scToString scFromString
  where
  scToString :: SelectedColumns -> String
  scToString (SelectedColumns sc) = intercalate "," (qualifiedColumnToString <$> sc)

  scFromString :: String -> Either String SelectedColumns
  scFromString "" = Right (SelectedColumns [])

  scFromString v = (map SelectedColumns <<< traverse qcFromStringEither <<< split (Pattern ",")) v

routeCodec :: RouteDuplex' Route
routeCodec =
  root
    $ G.sum
      { "Root": G.noArgs
      , "Crystals": path "crystals" G.noArgs
      , "P11Ingest": path "p11ingest" G.noArgs
      , "Pucks": path "pucks" G.noArgs
      , "Targets": path "targets" G.noArgs
      , "Beamline": "beamline" ? { puckId: optional <<< string, crystalId: optional <<< string }
      , "Job": "job" ? { jobId: int }
      , "Analysis":
          "analysis"
            ?
              { sortColumn: optional <<< qualifiedColumnCodec
              , sortOrder: sortOrder
              , filterQuery: string
              , selectedColumns: selectedColumnsCodec
              , limit: int
              }
      , "Tools": "tools" ? {}
      , "Jobs":
          "jobs"
            ?
              { limit: int
              , statusFilter: optional <<< string
              , tagFilter: optional <<< string
              , durationFilter: optional <<< sinceCodec
              }
      , "CompoundOverview": path "compounds" G.noArgs
      , "ToolsAdmin": path "toolsadmin" G.noArgs
      }

matchRoute :: (Maybe Route -> Route -> Effect Unit) -> Effect (Effect Unit)
matchRoute = matchesWith (parse routeCodec)

createLink :: Route -> String
createLink x = "#" <> print routeCodec x
