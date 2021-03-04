module App.Components.EditRun where

import Prelude hiding (comparing)
import App.API (RunPropertiesResponse, RunResponse, RunsResponse, retrieveRun, retrieveRunProperties, retrieveRuns)
import App.AppMonad (AppMonad)
import App.Autocomplete as Autocomplete
import App.Comment (Comment)
import App.Components.ParentComponent (ParentError, ChildInput, parentComponent)
import App.HalogenUtils (scope)
import App.Route (Route(..), RunsRouteInput, createLink, routeCodec)
import App.Run (Run, runId, runLookup, runScalarProperty, runValues)
import App.RunProperty (RunProperty, rpDescription, rpIsSortable, rpName)
import App.RunScalar (RunScalar(..))
import App.RunValue (RunValue(..), runValueComments)
import App.SortOrder (SortOrder(..), comparing, invertOrder)
import DOM.HTML.Indexed.ScopeValue (ScopeValue(ScopeCol))
import Data.Array (head, mapMaybe, sortBy, (:))
import Data.Either (Either)
import Data.Maybe (Maybe(..), maybe)
import Data.Number.Format (precision, toStringWith)
import Data.Tuple (Tuple(..))
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData, fromEither)
import Routing.Duplex (print)

type State
  = { runId :: Int
    , run :: Run
    , manualProperties :: Array String
    }

data Action
  = Resort

initialState :: ChildInput Int RunResponse -> State
initialState { input: runId, remoteData: runResponse } =
  { runId: runId
  , run: runResponse.run
  , manualProperties: runResponse.manual_properties
  }

component :: forall output query. H.Component HH.HTML query RunsRouteInput output AppMonad
component = parentComponent fetchInitialData childComponent

childComponent :: forall q. H.Component HH.HTML q (ChildInput RunsRouteInput (Tuple RunsResponse RunPropertiesResponse)) ParentError AppMonad
childComponent =
  H.mkComponent
    { initialState
    , render
    , eval:
        H.mkEval
          H.defaultEval
            { handleAction = handleAction
            }
    }

fetchInitialData :: Int -> AppMonad (RemoteData String RunResponse)
fetchInitialData rid = fromEither <$> (retrieveRun rid)

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  Resort -> pure unit

commentsTable :: Array Comment -> HH.HTML w i
commentsTable comments =
  let
    makeRow comment = HH.tr_ [ HH.td_ [ comment.created ], HH.td_ [ comment.author ], HH.td_ [ comment.text ] ]
  in
    HH.table
      [ HP.classes [ HH.ClassName "table" ] ]
      [ HH.thead_
          [ HH.tr_ [ HH.th_ [ HH.text "Created" ], HH.th_ [ HH.text "Author" ], HH.th_ [ HH.text "Text" ] ]
          ]
      , HH.tbody_ (makeRow <$> comments)
      ]

render :: State -> H.ComponentHTML Action ( refinementComplete :: forall query. H.Slot query Autocomplete.Output Int ) AppMonad
render state =
  let
    makeHeader :: forall w. RunProperty -> HH.HTML w Action
    makeHeader t = HH.th [ scope ScopeCol ] [ HH.text (rpDescription t) ]

    makeRow comment = HH.tr_ [ HH.td_ [ comment.created ], HH.td_ [ comment.author ], HH.td_ [ comment.text ] ]

    comments :: Maybe (Array Comment)
    comments = head (mapMaybe runValueComments (runValues (state.run)))
  in
    HH.div [ HP.classes [ HH.ClassName "container-fluid" ] ]
      [ HH.h3_ [ HH.text "Comments" ]
      , HH.table
          [ HP.classes [ HH.ClassName "table" ] ]
          [ HH.thead_
              [ HH.tr_ [ HH.th_ [ HH.text "Created" ], HH.th_ [ HH.text "Author" ], HH.th_ [ HH.text "Text" ] ]
              ]
          , HH.tbody_ (makeRow <$> state.run.comments)
          ]
      ]
