module App.FontAwesome
  ( Icon
  , IconSize(..)
  , icon
  , simpleIcon
  , spinner
  , mkSimpleIcon
  )
  where

import App.HalogenUtils (classList)
import Control.Applicative (pure)
import Control.Category ((<<<))
import Data.Maybe (Maybe(..), maybe)
import Data.Semigroup ((<>))
import Data.Show (class Show, show)
import Halogen.HTML as HH

data IconSize = Xs | Sm | Lg | Twice | Thrice | Fifth | Seventh | Tenth

instance showIconSize :: Show IconSize where
  show Xs = "fa-xs"
  show Sm = "fa-sm"
  show Lg = "fa-lg"
  show Twice = "fa-2x"
  show Thrice = "fa-3x"
  show Fifth = "fa-5x"
  show Seventh = "fa-7x"
  show Tenth = "fa-10x"

type Icon = {
    name :: String
  , size :: Maybe IconSize
  , spin :: Boolean
  }

mkSimpleIcon :: String -> Icon
mkSimpleIcon name = { name, size: Nothing, spin: false }

icon :: forall w i. Icon -> HH.HTML w i
icon { name, size, spin } =
  let classes = [ "fas", "fa-" <> name ] <> (maybe [] (pure <<< show) size) <> (if spin then ["fa-spin"] else [])
  in HH.i [ classList classes ] []

simpleIcon :: forall w i.String -> HH.HTML w i
simpleIcon name = icon {name, size: Nothing, spin: false}

spinner :: forall w i. Maybe IconSize -> HH.HTML w i
spinner size = icon { name: "spinner", size, spin: true }
