module Main where

import Prelude
import App.Config(baseUrl)
import App.AppMonad (runAppMonad)
import App.Router (routeCodec)
import App.Router as Router
import Data.Maybe (Maybe(..))
import Effect (Effect)
import Effect.Aff (launchAff_)
import Effect.Class (liftEffect)
import Halogen.Aff as HA
import Halogen.Component (hoist)
import Halogen.VDom.Driver (runUI)
import Routing.Duplex (parse)
import Routing.Hash (matchesWith)
import Halogen as H

main :: Effect Unit
main = HA.runHalogenAff do
  body <- HA.awaitBody
  let appMComponent = hoist (runAppMonad { baseUrl }) Router.component
  halogenIO <- runUI appMComponent unit body

  -- Listen to the route changes.
  void $ liftEffect $ matchesWith ( parse routeCodec ) \mOld new ->
    when ( mOld /= Just new ) do
      launchAff_ $ halogenIO.query $ H.tell $ Router.Navigate new
  pure unit
