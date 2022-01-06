module App.HalogenUtils where

import Prelude

import App.SortOrder (SortOrder(..))
import DOM.HTML.Indexed.ScopeValue (ScopeValue)
import Effect.Class (class MonadEffect)
import Halogen as H
import Halogen.HTML (IProp)
import Halogen.HTML as HH
import Halogen.HTML.Properties as HP
import Halogen.HTML.Properties.ARIA as HPA
import Network.RemoteData (RemoteData(..))
import Web.HTML (window)
import Web.HTML.Window (confirm)

classList ::
  forall t144 t145.
  Array String ->
  HH.IProp
    ( class :: String
    | t145
    )
    t144
classList = HP.classes <<< map HH.ClassName

singleClass ::
  forall t53 t54.
  String ->
  IProp
    ( class :: String
    | t53
    )
    t54
singleClass s = classList [ s ]

plainTh :: forall w i. String -> HH.HTML w i
plainTh x = HH.th_ [ HH.text x ]

plainTd :: forall w i. String -> HH.HTML w i
plainTd x = HH.td_ [ HH.text x ]

scope :: forall r i. ScopeValue -> HH.IProp ( scope :: ScopeValue | r ) i
scope = HH.prop (HH.PropName "scope")

dismiss :: forall t5 t6. String -> HH.IProp t6 t5
dismiss = HP.attr (HH.AttrName "data-dismiss")

data AlertType
  = AlertPrimary
  | AlertSecondary
  | AlertSuccess
  | AlertDanger
  | AlertWarning
  | AlertInfo
  | AlertLight
  | AlertDark

instance showAlertType :: Show AlertType where
  show AlertPrimary = "alert-primary"
  show AlertSecondary = "alert-secondary"
  show AlertSuccess = "alert-success"
  show AlertDanger = "alert-danger"
  show AlertWarning = "alert-warning"
  show AlertInfo = "alert-info"
  show AlertLight = "alert-light"
  show AlertDark = "alert-dark"

makeAlertFromRemoteData :: forall w i. RemoteData String String -> HH.HTML w i
makeAlertFromRemoteData (Success e) = makeAlert AlertSuccess e
makeAlertFromRemoteData (Failure e) = makeAlert AlertDanger e
makeAlertFromRemoteData _ = HH.text ""

makeAlertHtml :: forall w i. AlertType -> Array (HH.HTML w i) -> HH.HTML w i
makeAlertHtml alertType content =
  HH.div
    [ HP.classes [ HH.ClassName ("alert " <> show alertType) ], HPA.role "alert" ]
    content

makeAlert :: forall w i. AlertType -> String -> HH.HTML w i
makeAlert alertType content = makeAlertHtml alertType [HH.text content]

makeRequestResult :: forall w i. (RemoteData String String) -> HH.HTML w i
makeRequestResult (Failure e) = makeAlert AlertDanger e

makeRequestResult (Success e) = makeAlert AlertPrimary e

makeRequestResult _ = HH.text ""

whenConfirmed :: forall t4. Bind t4 => MonadEffect t4 => String -> t4 Unit -> t4 Unit
whenConfirmed message f = do
  w <- H.liftEffect window
  confirmResult <- H.liftEffect (confirm message w)
  when confirmResult f


-- messageFromResult :: forall e r. String -> Either (GraphQLError e) (Maybe { errorMessage :: Maybe String | r }) -> RemoteData String String
-- messageFromResult opName result =
--   let
--     successMessage = Success (opName <> " successful!")
--   in
--     case result of
--       Left e -> Failure $ "HTTP Error: " <> printGraphQLError e
--       Right Nothing -> successMessage
--       Right (Just { errorMessage: Just errorMessage' }) -> Failure errorMessage'
--       Right (Just { errorMessage: Nothing }) -> successMessage
faIcon :: forall w i. String -> HH.HTML w i
faIcon name = HH.i [ HP.classes [ HH.ClassName "fa", HH.ClassName ("fa-" <> name) ] ] []

faIconRotate :: forall w i. String -> HH.HTML w i
faIconRotate name =HH.i [ HP.classes [ HH.ClassName "fa", HH.ClassName ("fa-" <> name) ] ] []

errorText :: forall w i. String -> HH.HTML w i
errorText text = makeAlert AlertDanger text

-- Convert a sort order to an icon
orderingToIcon :: forall w i. SortOrder -> HH.HTML w i
orderingToIcon Ascending = faIcon "sort-up"

orderingToIcon Descending = faIcon "sort-down"
