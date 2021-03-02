module App.Logging where

import Prelude

import Effect.Class (class MonadEffect, liftEffect)
import Effect.Console as Console
import Effect.Now (nowDateTime)

data LogLevel = Info | Error

instance showLogLevel :: Show LogLevel where
  show Info = "INFO"
  show Error = "ERROR"

logRaw :: forall m. MonadEffect m => LogLevel -> String -> m Unit
logRaw level m = do
  dt <- liftEffect nowDateTime
  liftEffect $ Console.log $ "[" <> show level <> "] " <> (show dt) <> ": " <> m

