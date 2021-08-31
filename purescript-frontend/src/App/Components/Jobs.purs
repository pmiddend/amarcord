module App.Components.Jobs where

import App.AppMonad (AppMonad)
import App.Bootstrap (container)
import App.Components.JobList as JobList
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
  = ( jobList :: forall query. H.Slot query Void Int )


render :: State -> H.ComponentHTML Action Slots AppMonad
render state =
  container
    [ HH.h2_
        [ icon { name: "clipboard", size: Nothing, spin: false }, HH.text " Jobs" ]
    , HH.slot _jobList 1 JobList.component { limit: 10, statusFilter: Nothing, durationFilter: Nothing } absurd
    ]
