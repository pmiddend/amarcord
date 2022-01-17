module App.RequestError
  ( RequestError
  )
  where

import Data.Maybe (Maybe)

type RequestError = {
    code :: Maybe Int
  , title :: String
  , description :: String
  }
