module App.Components.Runs where

-- import Prelude hiding (comparing)
-- import App.API (RunPropertiesResponse, RunsResponse, retrieveRunProperties, retrieveRuns)
-- import App.AppMonad (AppMonad)
-- import App.Autocomplete as Autocomplete
-- import App.Bootstrap (TableFlag(..), fluidContainer, plainH1_, plainTh_, table)
-- import App.Comment (Comment)
-- import App.Components.ParentComponent (ParentError, ChildInput, parentComponent)
-- import App.HalogenUtils (classList, faIcon, scope, singleClass)
-- import App.Route (Route(..), RunsRouteInput, createLink)
-- import App.Run (Run, runId, runLookup, runScalarProperty)
-- import App.RunProperty (RunProperty, _description, _name, rpDescription, rpIsSortable, rpName)
-- import App.RunScalar (RunScalar(..))
-- import App.RunValue (RunValue(..))
-- import App.SortOrder (SortOrder(..), comparing, invertOrder)
-- import DOM.HTML.Indexed.ScopeValue (ScopeValue(ScopeCol))
-- import Data.Array (filter, sortBy, (:))
-- import Data.Either (Either)
-- import Data.Foldable (foldMap)
-- import Data.Lens (over, set, view, (^.))
-- import Data.Lens.At (at)
-- import Data.Lens.Record (prop)
-- import Data.Maybe (Maybe(..), maybe)
-- import Data.Number.Format (precision, toStringWith)
-- import Data.Set (Set, delete, insert, member, singleton)
-- import Data.Symbol (SProxy(..))
-- import Data.Tuple (Tuple(..))
import Halogen as H
import Halogen.HTML as HH
-- import Halogen.HTML.Events as HE
-- import Halogen.HTML.Properties as HP
-- import Network.RemoteData (RemoteData, fromEither)

-- type State
--   = { runs :: Array Run
--     , runProperties :: Array RunProperty
--     , selectedRunProperties :: Set String
--     , runSorting :: RunsRouteInput
--     }

-- _selectedRunProperties = prop (SProxy :: SProxy "selectedRunProperties")

-- data Action
--   = Resort RunsRouteInput
--   | ToggleProperty String

-- initialState :: ChildInput RunsRouteInput (Tuple RunsResponse RunPropertiesResponse) -> State
-- initialState { input: runSorting, remoteData: Tuple runsResponse runPropertiesResponse } =
--   { runProperties: runPropertiesResponse.metadata
--   , selectedRunProperties: (foldMap (view _name >>> singleton) runPropertiesResponse.metadata)
--   , runSorting
--   , runs: resort runSorting runsResponse.runs
--   }

-- component :: forall output query. H.Component HH.HTML query RunsRouteInput output AppMonad
-- component = parentComponent fetchInitialData childComponent
component = H.mkComponent { initialState: \_ -> 0, render: \_ -> HH.text "todo", eval: H.mkEval H.defaultEval }

-- childComponent :: forall q. H.Component HH.HTML q (ChildInput RunsRouteInput (Tuple RunsResponse RunPropertiesResponse)) ParentError AppMonad
-- childComponent =
--   H.mkComponent
--     { initialState
--     , render
--     , eval:
--         H.mkEval
--           H.defaultEval
--             { handleAction = handleAction
--             }
--     }

-- combineEithers :: forall a b c. Either a b -> Either a c -> Either a (Tuple b c)
-- combineEithers x' y' = Tuple <$> x' <*> y'

-- fetchInitialData :: RunsRouteInput -> AppMonad (RemoteData String (Tuple RunsResponse RunPropertiesResponse))
-- fetchInitialData _ = fromEither <$> (combineEithers <$> retrieveRuns <*> retrieveRunProperties)

-- resort :: RunsRouteInput -> Array Run -> Array Run
-- resort by = sortBy (comparing (by.sortOrder) (runScalarProperty by.sort))

-- toggleSetElement :: forall a. Ord a => a -> Set a -> Set a
-- toggleSetElement v x
--   | member v x = delete v x
--   | otherwise = insert v x

-- handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
-- handleAction = case _ of
--   ToggleProperty p -> H.modify_ (over _selectedRunProperties (toggleSetElement p))
--   Resort by ->
--     H.modify_ \state ->
--       state
--         { runSorting = by
--         , runs = resort by (state.runs)
--         }

-- orderingToIcon :: forall w i. SortOrder -> HH.HTML w i
-- orderingToIcon Ascending = faIcon "sort-up"

-- orderingToIcon Descending = faIcon "sort-down"

-- resortParams :: String -> RunsRouteInput -> RunsRouteInput
-- resortParams x { sort, sortOrder }
--   | x == sort = { sort, sortOrder: invertOrder sortOrder }
--   | otherwise = { sort: x, sortOrder: Ascending }

-- selectedColumnChooser :: forall w. State -> HH.HTML w Action
-- selectedColumnChooser state =
--   let
--     makeRow :: RunProperty -> HH.HTML w Action
--     makeRow property =
--       HH.button
--         [ HP.type_ HP.ButtonButton
--         , classList ([ "list-group-item", "list-group-flush", "list-group-item-action" ] <> (if member (property ^. _name) state.selectedRunProperties then [ "active" ] else []))
--         , HE.onClick \_ -> Just (ToggleProperty (property ^. _name))
--         ]
--         [ HH.text (property ^. _description) ]
--   in
--     HH.div_
--       [ HH.p_
--           [ HH.button
--               [ classList [ "btn", "btn-secondary" ]
--               , HP.type_ HP.ButtonButton
--               , HP.attr (HH.AttrName "data-toggle") "collapse"
--               , HP.attr (HH.AttrName "data-target") "#columnChooser"
--               ]
--               [ HH.text "Choose columns" ]
--           ]
--       , HH.div [ singleClass "collapse", HP.id_ "columnChooser" ]
--           [ HH.div [ singleClass "list-group " ]
--               ( makeRow <$> state.runProperties
--               )
--           ]
--       ]

-- render :: State -> H.ComponentHTML Action ( refinementComplete :: forall query. H.Slot query Autocomplete.Output Int ) AppMonad
-- render state =
--   let
--     makeHeader :: forall w. RunProperty -> HH.HTML w Action
--     makeHeader t =
--       let
--         isSortProp = rpName t == state.runSorting.sort

--         afterSort = resortParams (rpName t) (state.runSorting)
--       in
--         HH.th [ scope ScopeCol ]
--           ( if rpIsSortable t then
--               [ HH.a
--                   [ HP.href (createLink (Runs afterSort)), HE.onClick \_ -> Just (Resort afterSort) ]
--                   ([ HH.text (rpDescription t) ] <> (if isSortProp then [ orderingToIcon state.runSorting.sortOrder ] else []))
--               ]
--             else
--               [ HH.text (rpDescription t) ]
--           )

--     makeComment :: forall w. Comment -> HH.HTML w Action
--     makeComment c = HH.li_ [ HH.text (c.author <> ": " <> c.text) ]

--     makeProperty :: forall w. RunProperty -> Maybe RunValue -> HH.HTML w Action
--     makeProperty rp value = case value of
--       Nothing -> HH.td_ []
--       Just (Comments cs) -> HH.td_ [ HH.ul_ (makeComment <$> cs) ]
--       Just (Scalar (RunScalarNumber n)) -> HH.td_ [ HH.text (toStringWith (precision 2) n) ]
--       _ -> HH.td_ [ HH.text (maybe "" show value) ]

--     wholeSelectedProps = filter (\x -> member (x ^. _name) state.selectedRunProperties) state.runProperties

--     makeRow run = HH.tr_ (HH.td_ [ HH.a [ HP.href (createLink (EditRun (runId run))) ] [ faIcon "edit" ] ] : ((\rp -> makeProperty rp (runLookup run (rpName rp))) <$> wholeSelectedProps))
--   in
--     fluidContainer
--       [ plainH1_ "Runs"
--       , selectedColumnChooser state
--       , table
--           [ TableStriped ]
--           (plainTh_ "Actions" : (makeHeader <$> wholeSelectedProps))
--           (makeRow <$> state.runs)
--       ]
