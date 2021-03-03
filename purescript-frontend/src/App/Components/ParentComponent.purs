module App.Components.ParentComponent where

import App.AppMonad (AppMonad)
import App.Halogen.FontAwesome (IconSize(..), spinner)
import App.HalogenUtils (classList)
import Control.Bind (bind, discard)
import Control.Semigroupoid ((<<<))
import Data.Maybe (Maybe(..))
import Data.Ord (class Ord)
import Data.Symbol (SProxy(..))
import Data.Unit (Unit)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData(..))

type ParentError
  = String

type ParentState a
  = RemoteData String a

data ParentAction a
  = Initialize
  | ChildFailed ParentError

type ParentSlots co
  = ( child :: forall query. H.Slot query co Int )

parentRender childComponent state =
  let
    loading = HH.div [ HP.classes [] ] [ HH.p_ [ HH.text ("Loading, please wait...") ], HH.p_ [ spinner (Just Twice) ] ]
  in
    case state of
      NotAsked -> loading
      Loading -> loading
      Failure e -> HH.div [ HP.classes [] ] [ HH.p_ [ HH.text "Error loading table:" ], HH.pre_ [ HH.text e ], HH.p_ [ HH.button [ classList [ "btn", "btn-primary" ], HE.onClick \_ -> Just Initialize ] [ HH.text "Retry" ] ] ]
      Success state' -> HH.slot (SProxy :: SProxy "child") state' childComponent state' (Just <<< ChildFailed)

parentHandleAction :: forall a slots output. AppMonad (ParentState a) -> ParentAction a -> H.HalogenM (ParentState a) (ParentAction a) slots output AppMonad Unit
parentHandleAction fetchState a = case a of
  Initialize -> do
    H.put Loading
    result <- H.lift fetchState
    H.put result
  ChildFailed e -> H.put (Failure e)

parentComponent :: forall i q o cq cs. Ord cs => AppMonad (RemoteData String cs) -> H.Component HH.HTML cq cs ParentError AppMonad -> H.Component HH.HTML q i o AppMonad
parentComponent fetchState childComponent =
  H.mkComponent
    { initialState: \_ -> NotAsked
    , render: parentRender childComponent
    , eval:
        H.mkEval
          H.defaultEval
            { handleAction = parentHandleAction fetchState
            , initialize = Just Initialize
            }
    }
