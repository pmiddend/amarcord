module App.Components.Events where

import App.API (EventsResponse, retrieveEvents)
import App.AppMonad (AppMonad, log)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.Event (Event)
import App.HalogenUtils (classList, scope)
import App.Logging (LogLevel(..))
import Control.Applicative (pure)
import Data.Functor ((<$>))
import Data.Maybe (Maybe(..))
import Data.Unit (Unit, unit)
import Effect.Aff.Class (class MonadAff)
import Effect.Timer (clearTimeout, setTimeout)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Properties (ScopeValue(..))
import Halogen.Query.EventSource (EventSource, Finalizer(..), effectEventSource, emit)
import Network.RemoteData (RemoteData(..), fromEither)
import Prelude (discard, bind)

-- Event source for a simple timer
timerEventSource :: forall m. MonadAff m => EventSource m Action
timerEventSource =
  effectEventSource \emitter -> do
    timerId <- setTimeout 5000 (emit emitter RefreshTimeout)
    pure (Finalizer (clearTimeout timerId))

type State
  = { events :: Array Event
    }

render :: forall slots. State -> H.ComponentHTML Action slots AppMonad
render state =
  let
    makeRow event =
      HH.tr_
        [ HH.td_ [ HH.text event.created ]
        , HH.td_ [ HH.text event.source ]
        , HH.td_ [ HH.text event.level ]
        , HH.td_ [ HH.text event.text ]
        ]
  in
    HH.div_
      [ HH.h1_ [ HH.text "Events" ]
      , HH.table [ classList [ "table", "table-striped" ] ]
          [ HH.thead_
              [ HH.tr_
                  [ HH.th [ scope ScopeCol ] [ HH.text "Created" ]
                  , HH.th [ scope ScopeCol ] [ HH.text "Source" ]
                  , HH.th [ scope ScopeCol ] [ HH.text "Level" ]
                  , HH.th [ scope ScopeCol ] [ HH.text "Text" ]
                  ]
              ]
          , HH.tbody_ (makeRow <$> state.events)
          ]
      ]

childComponent :: forall q. H.Component HH.HTML q (ChildInput Unit ParentOutput) ParentError AppMonad
childComponent =
  H.mkComponent
    { initialState
    , render
    , eval:
        H.mkEval
          H.defaultEval
            { initialize = Just Initialize
            , handleAction = handleAction
            }
    }

data Action
  = RefreshTimeout
  | Initialize

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  Initialize -> do
    _ <- H.subscribe timerEventSource
    pure unit
  RefreshTimeout -> do
    H.lift (log Info "Refreshing...")
    s <- H.get
    remoteData <- H.lift (fetchData unit)
    case remoteData of
      Success newEvents ->
        H.modify_ \state ->
          state
            { events = newEvents.events
            }
      _ -> pure unit
    _ <- H.subscribe timerEventSource
    pure unit

initialState :: ChildInput Unit EventsResponse -> State
initialState { remoteData: eventsResponse } = { events: eventsResponse.events }

type ParentOutput
  = EventsResponse

fetchData :: Unit -> AppMonad (RemoteData String ParentOutput)
fetchData _ = fromEither <$> retrieveEvents

component ::
  forall query output.
  H.Component HH.HTML query
    Unit
    output
    AppMonad
component = parentComponent fetchData childComponent
