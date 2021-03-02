module App.Autocomplete where

import App.HalogenUtils (classList)
import Control.Applicative (pure, when)
import Control.Bind (bind, discard)
import Control.Promise (Promise, fromAff)
import Data.Eq ((/=))
import Data.Function (($))
import Data.Lens (Lens', use, (.=))
import Data.Lens.Record (prop)
import Data.Maybe (Maybe(..))
import Data.Monoid (mempty)
import Data.Semigroup ((<>))
import Data.Show (show)
import Data.Symbol (SProxy(..))
import Data.Unit (Unit, unit)
import Effect (Effect)
import Effect.Aff (Aff)
import Effect.Aff.Class (class MonadAff)
import Effect.Console as Console
import Halogen (liftEffect)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Properties (InputType(..))
import Halogen.HTML.Properties as HP
import Halogen.Query.EventSource (EventSource, effectEventSource, emit)

type Result r = {
    value :: String
  , text :: String
  | r
  }

type FormatResult = {
    id :: String
  , text :: String
  , html :: String
  }

type SearchCallback r = String -> Aff (Array (Result r))

type SearchCallbackEff r = String -> Effect (Promise (Array (Result r)))

type FormatCallback r = Result r -> FormatResult

type ChangeCallback r = String -> Effect Unit

type ClearCallback = Unit -> Effect Unit

foreign import doAutocomplete :: forall r. String -> SearchCallbackEff r -> FormatCallback r -> Effect Unit
foreign import autocompleteOnChange :: forall r. String -> ClearCallback -> ChangeCallback r -> Effect Unit
foreign import autocompleteSet :: String -> String -> Effect Unit
foreign import autocompleteClear :: String -> Effect Unit

data Action r
  = Initialize
  | Finalize
  | ValueChange (Maybe String)
  | Receive (Input r)

type Input r = {
    elementId :: String
  , placeholder :: String
  , currentValue :: Maybe String
  , searchCallback :: SearchCallback r
  , formatCallback :: FormatCallback r
  }

type State r = Input r

data Output = Change (Maybe String)

_elementId :: forall r. Lens' (State r) String
_elementId = prop (SProxy :: SProxy "elementId")

_searchCallback :: forall r. Lens' (State r) (SearchCallback r)
_searchCallback = prop (SProxy :: SProxy "searchCallback")

_formatCallback :: forall r. Lens' (State r) (FormatCallback r)
_formatCallback = prop (SProxy :: SProxy "formatCallback")

_currentValue :: forall r. Lens' (State r) (Maybe String)
_currentValue = prop (SProxy :: SProxy "currentValue")

initialState :: forall r. Input r -> State r
initialState input = input

render :: forall r w i. State r -> HH.HTML w i
render state = 
  HH.input
    [ classList [ "form-control", "basicAutoSelect" ]
    , HP.attr (HH.AttrName "placeholder") (state.placeholder)
    , HP.type_ InputText
    , HP.id_ (state.elementId)
    ]

changeSource :: forall r m. MonadAff m => String -> EventSource m (Action r)
changeSource autocompleteId = effectEventSource \emitter -> do
  autocompleteOnChange autocompleteId (\_ -> emit emitter (ValueChange Nothing)) \newValue -> (emit emitter (ValueChange (Just newValue)))
  pure mempty

handleAction :: forall r slots m. MonadAff m => Action r -> H.HalogenM (State r) (Action r) slots Output m Unit
handleAction = case _ of
  Finalize -> do
    liftEffect $ Console.log "finalizing component"
  Initialize -> do
    liftEffect $ Console.log "initializing component"
    eid <- use _elementId
    searchCallback' <- use _searchCallback
    formatCallback' <- use _formatCallback
    liftEffect $ doAutocomplete eid (\qry -> fromAff (searchCallback' qry)) formatCallback'
    _ <- H.subscribe (changeSource eid)
    pure unit
  ValueChange newValue -> do
    _currentValue .= newValue
    liftEffect $ Console.log "new value in change!"
    H.raise (Change newValue)
  Receive newState -> do
    currentValue <- use _currentValue
    let newValue = newState.currentValue
    liftEffect $ Console.log $ "in receive, current: " <> show currentValue <> ", new " <> show newValue <> "!"
    eid <- use _elementId
    when (newValue /= currentValue) $ do
    --when ((_.value <$> newValue) /= (_.value <$> currentValue)) $ do
      liftEffect $ Console.log "new value in receive!"
      _currentValue .= newState.currentValue
      liftEffect $ case newValue of
        Nothing -> autocompleteClear eid
        Just newValue' -> autocompleteSet eid newValue'

component :: forall q m r. MonadAff m => H.Component HH.HTML q (Input r) Output m
component =
  H.mkComponent
    { initialState
    , render
    , eval: H.mkEval $ H.defaultEval {
        handleAction = handleAction
      , initialize = Just Initialize
      , finalize = Just Finalize
--      , receive = Just <<< Receive
      }
    }
