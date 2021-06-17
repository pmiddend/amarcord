module App.Timer where

import Control.Applicative (pure)
import Control.Monad (bind)
import Effect.Aff.Class (class MonadAff)
import Effect.Timer (clearTimeout, setTimeout)
import Halogen.Query.EventSource (EventSource, Finalizer(..), effectEventSource, emit)

timerEventSource :: forall m a. MonadAff m => a -> EventSource m a
timerEventSource a =
  effectEventSource \emitter -> do
    timerId <- setTimeout 5000 (emit emitter a)
    pure (Finalizer (clearTimeout timerId))
