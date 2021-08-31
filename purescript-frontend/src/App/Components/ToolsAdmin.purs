module App.Components.ToolsAdmin where

import App.Components.ToolsCrud as ToolsCrud
import App.AppMonad (AppMonad)
import App.Bootstrap (container)
import App.Halogen.FontAwesome (icon)
import Data.Maybe (Maybe(..))
import Data.Symbol (SProxy(..))
import Data.Void (Void, absurd)
import Halogen as H
import Halogen.HTML as HH

data Action

type State = {}
type ToolsData = {}

component :: forall t26 t43 t46. H.Component HH.HTML t46 t43 t26 AppMonad
component =
  H.mkComponent
    { initialState: \_ -> {}
    , render
    , eval: H.mkEval H.defaultEval
    }

_jobList = SProxy :: SProxy "jobList"

type Slots
  = ( toolsCrud :: forall query. H.Slot query Void Int )

_toolsCrud = SProxy :: SProxy "toolsCrud"


render :: State -> H.ComponentHTML Action Slots AppMonad
render state =
  container
    [ HH.h2_
        [ icon { name: "tools", size: Nothing, spin: false }, HH.text " Available Tools" ]
    , HH.slot _toolsCrud 1 ToolsCrud.component { } absurd
    ]
