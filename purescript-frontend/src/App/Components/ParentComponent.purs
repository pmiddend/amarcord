module App.Components.ParentComponent where

import App.AppMonad (AppMonad)
import App.Halogen.FontAwesome (IconSize(..), spinner)
import App.HalogenUtils (classList)
import Control.Bind (bind, discard)
import Control.Semigroupoid ((<<<))
import Data.Maybe (Maybe(..))
import Data.Ord (class Ord)
import Data.Symbol (SProxy(..))
import Data.Tuple (Tuple(..))
import Data.Unit (Unit)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData(..))

type ParentError
  = String

type ParentState ci a
  = { remoteData :: RemoteData String a
    , childInput :: ci
    }

data ParentAction a
  = Initialize
  | ChildFailed ParentError

type ParentSlots co
  = ( child :: forall query. H.Slot query co Int )

type ChildInput ci a
  = { remoteData :: a
    , input :: ci
    }

parentRender childComponent state =
  let
    loading = HH.div [ HP.classes [] ] [ HH.p_ [ HH.text ("Loading, please wait...") ], HH.p_ [ spinner (Just Twice) ] ]
  in
    case state.remoteData of
      NotAsked -> loading
      Loading -> loading
      Failure e -> HH.div [ HP.classes [] ] [ HH.p_ [ HH.text "Error loading table:" ], HH.pre_ [ HH.text e ], HH.p_ [ HH.button [ classList [ "btn", "btn-primary" ], HE.onClick \_ -> Just Initialize ] [ HH.text "Retry" ] ] ]
      Success state' -> HH.slot (SProxy :: SProxy "child") state' childComponent ({ remoteData: state', input: state.childInput }) (Just <<< ChildFailed)

parentHandleAction :: forall ci a slots output. AppMonad (RemoteData String a) -> ParentAction a -> H.HalogenM (ParentState ci a) (ParentAction a) slots output AppMonad Unit
parentHandleAction fetchState a = case a of
  Initialize -> do
    H.modify_ \state -> state { remoteData = Loading }
    result <- H.lift fetchState
    H.modify_ \state -> state { remoteData = result }
  ChildFailed e -> do
    H.modify_ \state -> state { remoteData = Failure e }

parentComponent :: forall i q o cq cs. Ord cs => AppMonad (RemoteData String cs) -> H.Component HH.HTML cq (ChildInput i cs) ParentError AppMonad -> H.Component HH.HTML q i o AppMonad
parentComponent fetchState childComponent =
  H.mkComponent
    { initialState: \ci -> { remoteData: NotAsked, childInput: ci }
    , render: parentRender childComponent
    , eval:
        H.mkEval
          H.defaultEval
            { handleAction = parentHandleAction fetchState
            , initialize = Just Initialize
            }
    }
