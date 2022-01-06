module App.Root where

import App.HalogenUtils (singleClass)
import Halogen as H
import Halogen.HTML as HH

render :: forall a m slots. {} -> H.ComponentHTML a slots m
render _state =
  HH.div [ singleClass "container text-center" ]
    [ HH.h1_ [ HH.text "Welcome to AMARCORD" ]
    , HH.p [singleClass "lead"] [ HH.text "Select one of the tools on the top to continue." ]
    ]

component :: forall q i o m. H.Component q i o m
component =
  H.mkComponent
    { initialState: \_ -> {}
    , render
    , eval: H.mkEval H.defaultEval
    }
