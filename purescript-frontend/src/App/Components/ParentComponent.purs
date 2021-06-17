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

type ParentState input tofetch
  = { remoteData :: RemoteData String tofetch
    , childInput :: input
    }

data ParentAction input tofetch
  = Initialize
  | ChildFailed ParentError
  | InputUpdated input

type ParentSlots co
  = ( child :: forall query. H.Slot query co Int )

type ChildInput ci a
  = { remoteData :: a
    , input :: ci
    }

parentRender ::
  forall tofetch m query input.
  Ord tofetch =>
  H.Component HH.HTML query (ChildInput input tofetch) String m ->
  ParentState input tofetch ->
  H.ComponentHTML (ParentAction input tofetch) ( child :: H.Slot query String tofetch ) m
parentRender childComponent state =
  let
    loading = HH.div [ HP.classes [] ] [ HH.p_ [ HH.text ("Loading, please wait...") ], HH.p_ [ spinner (Just Twice) ] ]
  in
    case state.remoteData of
      NotAsked -> loading
      Loading -> loading
      Failure e ->
        HH.div
          [ HP.classes []
          ]
          [ HH.p_ [ HH.text "Error loading:" ]
          , HH.pre_ [ HH.text e ]
          , HH.p_ [ HH.button [ classList [ "btn", "btn-primary" ], HE.onClick \_ -> Just Initialize ] [ HH.text "Retry" ] ]
          ]
      Success state' -> HH.slot (SProxy :: SProxy "child") state' childComponent ({ remoteData: state', input: state.childInput }) (Just <<< ChildFailed)

type Fetcher input tofetch
  = input -> AppMonad (RemoteData String tofetch)

parentHandleAction ::
  forall input tofetch slots output.
  Fetcher input tofetch ->
  ParentAction input tofetch ->
  H.HalogenM (ParentState input tofetch) (ParentAction input tofetch) slots output AppMonad Unit
parentHandleAction fetchState a = case a of
  Initialize -> do
    H.modify_ \state -> state { remoteData = Loading }
    childInput <- H.gets _.childInput
    result <- H.lift (fetchState childInput)
    H.modify_ \state -> state { remoteData = result }
  ChildFailed e -> do
    H.modify_ \state -> state { remoteData = Failure e }
  InputUpdated e -> H.modify_ \state -> state { childInput = e }

parentComponent ::
  forall input query output cq tofetch.
  Ord tofetch =>
  Fetcher input tofetch ->
  H.Component HH.HTML cq (ChildInput input tofetch) ParentError AppMonad ->
  H.Component HH.HTML query input output AppMonad
parentComponent fetchState childComponent =
  H.mkComponent
    { initialState: \input -> { remoteData: NotAsked, childInput: input }
    , render: parentRender childComponent
    , eval:
        H.mkEval
          H.defaultEval
            { handleAction = parentHandleAction fetchState
            , initialize = Just Initialize
            , receive = Just <<< InputUpdated
            }
    }
