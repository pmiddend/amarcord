module App.HalogenUtils where

import Prelude

import DOM.HTML.Indexed.ScopeValue (ScopeValue)
import Data.Either (Either(..))
import Data.Maybe (Maybe(..))
import Halogen.HTML as HH
import Halogen.HTML.Properties as HP
import Halogen.HTML.Properties.ARIA as HPA
import Network.RemoteData (RemoteData(..))


classList ::
  forall t144 t145.
  Array String ->
  HH.IProp
    ( class :: String
    | t145
    )
    t144
classList = HP.classes <<< map HH.ClassName


scope :: forall r i. ScopeValue -> HH.IProp ( scope :: ScopeValue | r ) i
scope = HH.prop (HH.PropName "scope")

dismiss :: forall t5 t6. String -> HH.IProp t6 t5
dismiss = HP.attr (HH.AttrName "data-dismiss")

makeAlert :: forall w i. String -> String -> HH.HTML w i
makeAlert alertType content =
  HH.div
    [ HP.classes [ HH.ClassName ("alert " <> alertType) ], HPA.role "alert" ]
    [ HH.text content ]

makeRequestResult :: forall w i. (RemoteData String String) -> HH.HTML w i
makeRequestResult (Failure e) = makeAlert "alert-danger" e

makeRequestResult (Success e) = makeAlert "alert-primary" e

makeRequestResult _ = HH.text ""

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
