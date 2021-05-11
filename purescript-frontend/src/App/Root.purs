module App.Root where

import Halogen as H
import Halogen.HTML as HH
import App.AppMonad (AppMonad)

render :: forall a m slots. {} -> H.ComponentHTML a slots m
render state =
  HH.div_
    [ HH.h1_ [ HH.text "Welcome to amarcord" ]
    , HH.p_ [ HH.text "Select one of the tools on the top to continue." ]
    ]

component :: forall q i o. H.Component HH.HTML q i o AppMonad
component =
  H.mkComponent
    { initialState: \_ -> {}
    , render
    , eval: H.mkEval H.defaultEval
    }
