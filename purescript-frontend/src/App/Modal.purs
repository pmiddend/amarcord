module App.Modal where

import Data.Unit (Unit)
import Effect (Effect)

foreign import hide :: String -> Effect Unit

foreign import show :: String -> Effect Unit
