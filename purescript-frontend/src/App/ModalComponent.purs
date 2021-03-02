module App.ModalComponent where

import App.AppMonad (AppMonad)
import App.HalogenUtils (classList, dismiss)
import App.Modal (hide, show)
import Control.Alternative (when)
import Control.Bind (bind, discard)
import Control.Category ((<<<))
import Data.Eq ((/=))
import Data.Maybe (Maybe(..))
import Data.Ring (negate)
import Data.Unit (Unit, unit)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties (ButtonType(..))
import Halogen.HTML.Properties as HP
import Halogen.HTML.Properties.ARIA as HPA

type ModalInput
  = { modalId :: String
    , confirmText :: String
    , open :: Boolean
    , renderBody :: forall a s m. Unit -> H.ComponentHTML a s m
    }

render :: forall s m. ModalInput -> H.ComponentHTML Action s m
render state =
  HH.div
    [ classList [ "modal", "fade" ]
    , HP.id_ (state.modalId)
    , HP.tabIndex (-1)
    , HPA.role "dialog"
    , HPA.labelledBy "delete-modal-title"
    , HPA.hidden "true"
    ]
    [ HH.div [ classList [ "modal-dialog" ], HPA.role "document" ]
        [ HH.div [ classList [ "modal-content" ] ]
            [ HH.div [ classList [ "modal-header" ] ]
                [ HH.h5 [ classList [ "modal-title" ], HP.id_ "delete-modal-title" ] [ HH.text "Really delete?" ]
                , HH.button [ HP.type_ ButtonButton, classList [ "close" ], dismiss "modal", HPA.label "Close" ]
                    [ HH.span [ HPA.hidden "true" ] [ HH.text "×" ]
                    ]
                ]
            , HH.div [ classList [ "modal-body" ] ]
                [ state.renderBody unit
                ]
            , HH.div [ classList [ "modal-footer" ] ]
                [ HH.button [ HP.type_ ButtonButton, classList [ "btn btn-danger" ], HE.onClick \_ -> Just Confirm ] [ HH.text state.confirmText ]
                , HH.button [ HP.type_ ButtonButton, classList [ "btn btn-secondary" ], HP.attr (HH.AttrName "data-dismiss") "modal" ] [ HH.text "Cancel" ]
                ]
            ]
        ]
    ]

data Action
  = Confirm
  | Update ModalInput

data Output
  = Confirmed

handleAction :: forall slots. Action -> H.HalogenM ModalInput Action slots Output AppMonad Unit
handleAction = case _ of
  Confirm -> H.raise Confirmed
  Update newInput -> do
    previousOpen <- H.gets _.open
    H.put newInput
    when (newInput.open /= previousOpen) do
      modalId <- H.gets _.modalId
      if newInput.open then
        H.liftEffect (show modalId)
      else
        H.liftEffect (hide modalId)

modal :: forall q. H.Component HH.HTML q ModalInput Output AppMonad
modal =
  H.mkComponent
    { initialState: \input -> { modalId: input.modalId, confirmText: input.confirmText, renderBody: input.renderBody, open: input.open }
    , render
    , eval:
        H.mkEval
          H.defaultEval
            { handleAction = handleAction
            , receive = Just <<< Update
            }
    }
