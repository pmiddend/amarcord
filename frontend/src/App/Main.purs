module Main where

import Prelude
import App.Route (routeCodec)
import App.Components.Router as Router
import Data.Maybe (Maybe(..))
import Effect (Effect)
import Effect.Aff (launchAff_)
import Effect.Class (liftEffect)
import Halogen.Aff as HA
import Halogen.VDom.Driver (runUI)
import Routing.Duplex (parse)
import Routing.Hash (matchesWith)
import Halogen as H

main :: Effect Unit
main =
  HA.runHalogenAff do
    body <- HA.awaitBody
    let
      appMComponent = Router.component
    halogenIO <- runUI appMComponent unit body
    -- Listen to the route changes.
    -- See https://github.com/thomashoneyman/purescript-halogen-realworld/blob/main/src/Main.purs
    void $ liftEffect
      $ matchesWith (parse routeCodec) \mOld new ->
          when (mOld /= Just new) do
            launchAff_ $ halogenIO.query $ H.mkTell $ Router.Navigate new
    pure unit
