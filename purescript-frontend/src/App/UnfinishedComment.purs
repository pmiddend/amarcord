module App.UnfinishedComment where

import Data.Lens.Record (prop)
import Data.Symbol (SProxy(..))

type UnfinishedComment
  = { author :: String
    , text :: String
    }

_author = prop (SProxy :: SProxy "author")

_text = prop (SProxy :: SProxy "text")

emptyUnfinishedComment = { author: "", text: "" }
