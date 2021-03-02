module App.AppMonad where

import Prelude

import App.Env (Env)
import App.Logging (LogLevel, logRaw)
import Control.Monad.Reader (class MonadAsk, ReaderT, asks, runReaderT)
import Effect.Aff (Aff)
import Effect.Aff.Class (class MonadAff)
import Effect.Class (class MonadEffect)
import Type.Equality as TE

newtype AppMonad a = AppMonad (ReaderT Env Aff a)

derive newtype instance functorAppM :: Functor AppMonad
derive newtype instance applyAppM :: Apply AppMonad
derive newtype instance applicativeAppM :: Applicative AppMonad
derive newtype instance bindAppM :: Bind AppMonad
derive newtype instance monadAppM :: Monad AppMonad
derive newtype instance monadEffectAppM :: MonadEffect AppMonad
derive newtype instance monadAffAppM :: MonadAff AppMonad
                        
instance monadAskAppM :: TE.TypeEquals e Env => MonadAsk e AppMonad where
  ask = AppMonad $ asks TE.from


runAppMonad :: Env -> AppMonad ~> Aff
runAppMonad e (AppMonad m) = runReaderT m e

log :: LogLevel -> String -> AppMonad Unit
log = logRaw
