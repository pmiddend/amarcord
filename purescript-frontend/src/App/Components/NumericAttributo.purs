module App.Components.NumericAttributo where

-- import App.API (changeRunAttributo)
-- import App.AppMonad (AppMonad)
-- import App.AttributoUtil (SourcedValue, _source, _value)
-- import App.HalogenUtils (classList, singleClass)
-- import App.JSONSchemaType (JSONNumberData, _range, _suffix)
-- import App.NumericRange (NumericRange, inRange, prettyPrintRange)
-- import Control.Applicative (pure, when)
-- import Control.Bind (bind, discard)
-- import Control.Category ((<<<))
-- import Data.Function (const, ($))
-- import Data.Functor ((<$>))
-- import Data.Lens (Lens', to, traversed, (.=), (^.), (^?))
-- import Data.Lens.Record (prop)
-- import Data.Maybe (Maybe(..))
-- import Data.Number (fromString)
-- import Data.Semigroup ((<>))
-- import Data.Show (class Show, show)
-- import Data.Symbol (SProxy(..))
-- import Data.Tuple (Tuple(..))
-- import Data.Unit (Unit, unit)
-- import Halogen as H
-- import Halogen.HTML as HH
-- import Halogen.HTML.Events as HE
-- import Halogen.HTML.Properties (InputType(..))
-- import Halogen.HTML.Properties as HP
-- import Network.RemoteData (RemoteData(..))

-- type Input
--   = { metadata :: JSONNumberData
--     , attributoName :: String
--     , content :: Maybe (SourcedValue Number)
--     , runId :: Int
--     }

-- type State
--   = { metadata :: JSONNumberData
--     , attributoName :: String
--     , content :: Maybe (SourcedValue Number)
--     , runRequest :: RemoteData String String
--     , runId :: Int
--     }

-- _runId :: Lens' State Int
-- _runId = prop (SProxy :: SProxy "runId")

-- _metadata :: Lens' State JSONNumberData
-- _metadata = prop (SProxy :: SProxy "metadata")

-- _attributoName :: Lens' State String
-- _attributoName = prop (SProxy :: SProxy "attributoName")

-- _content :: Lens' State (Maybe (SourcedValue Number))
-- _content = prop (SProxy :: SProxy "content")

-- _runRequest :: Lens' State (RemoteData String String)
-- _runRequest = prop (SProxy :: SProxy "runRequest")

-- data Action
--   = ValueChange Number
--   | Blur

-- numericAttributoComponent :: forall query output. H.Component HH.HTML query Input output AppMonad
-- numericAttributoComponent =
--   H.mkComponent
--     { initialState: \input -> { metadata: input.metadata, attributoName: input.attributoName, content: input.content, runRequest: NotAsked }
--     , render
--     , eval: H.mkEval H.defaultEval { handleAction = handleAction }
--     }

-- handleAction :: forall slots output. Action -> H.HalogenM State Action slots output AppMonad Unit
-- handleAction = case _ of
--   Blur -> do
--     s <- H.get
--     when (valueValid s)
--       $ do
--           _runRequest .= Loading
--           runResponse <- changeRunAttributo (s ^. _runId) (s ^. _attributoName) (s ^. (_content <<< traversed <<< _value))
--           case runResponse of
--             Left e -> _runRequest .= Failure e
--             Right _ -> H.modify_ (set _commentRequest (Success "Value changed!") >>> set _newComment emptyUnfinishedComment)
--   ValueChange newValue -> _content .= Just ({ source: "manual", value: newValue })

-- formatRange :: forall a w i. Show a => Maybe (NumericRange a) -> Array (HH.HTML w i)
-- formatRange Nothing = []

-- formatRange (Just r) = [ HH.small [ classList [ "ml-3", "form-text", "text-muted" ] ] [ HH.text ("value ∈ " <> prettyPrintRange r) ] ]

-- validValue state = case Tuple (state ^? (_content <<< traversed <<< _value)) (state ^. (_metadata <<< _range)) of
--   Tuple (Just number) (Just range) ->
--     if inRange range number
--        then Just number
--        else Nothing
--   _ -> 
-- valueValid state = case Tuple (state ^? (_content <<< traversed <<< _value)) (state ^. (_metadata <<< _range)) of
--   Tuple (Just number) (Just range) -> inRange range number
--   _ -> true

-- render :: forall slots. State -> H.ComponentHTML Action slots AppMonad
-- render state =
--   let
--     numericInput classes =
--       HH.input
--         [ HP.type_ InputNumber
--         , HP.id_ (state ^. _attributoName)
--         , classList classes
--         , HE.onValueInput (\newInput -> (ValueChange <$> fromString newInput))
--         , HP.value (state ^. (_content <<< traversed <<< _value <<< to show))
--         , HE.onBlur (const (Just Blur))
--         ]

--     sourceToClass = case state ^? (_content <<< traversed <<< _source) of
--       Just "manual" -> [ "manual-edit" ]
--       _ -> []

--     invalidToClass = if valueValid state then [ "is-valid" ] else [ "is-invalid" ]

--     formattedRange = formatRange (state ^. (_metadata <<< _range))
--   in
--     HH.div_
--       $ case state ^. (_metadata <<< _suffix) of
--           Just suffix ->
--             [ numericInput ([ "form-control" ] <> sourceToClass <> invalidToClass)
--             , HH.div
--                 [ classList [ "input-group-append" ] ]
--                 [ HH.div [ singleClass "input-group-text" ] [ HH.text suffix ] ]
--             ]
--               <> formattedRange
--           Nothing -> [ numericInput [ "form-control" ] ] <> formattedRange
