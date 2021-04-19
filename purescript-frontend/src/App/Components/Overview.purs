module App.Components.Overview where

import App.API (AttributiResponse, Attributo, OverviewCell, OverviewResponse, OverviewRow, attributoSuffix, descriptiveAttributoText, qualifiedAttributoName, retrieveAttributi, retrieveOverview)
import App.AppMonad (AppMonad)
import App.AssociatedTable (AssociatedTable)
import App.Bootstrap (TableFlag(..), fluidContainer, plainH1_, table)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.HalogenUtils (classList, faIcon, scope, singleClass)
import App.JSONSchemaType (JSONSchemaType(..))
import App.QualifiedAttributoName (QualifiedAttributoName)
import App.Route (Route(..), OverviewRouteInput, createLink)
import App.SortOrder (SortOrder(..), comparing, invertOrder)
import App.Utils (fanoutApplicative, toggleSetElement)
import Control.Apply ((<*>))
import Data.Argonaut (caseJson)
import Data.Array (filter, head, mapMaybe, sortBy)
import Data.Eq ((==))
import Data.Foldable (foldMap, null)
import Data.Function (const, (<<<))
import Data.Functor (map, (<$>))
import Data.HeytingAlgebra ((&&))
import Data.Int (round)
import Data.Lens (to, toArrayOf)
import Data.Maybe (Maybe(..), maybe)
import Data.Number.Format (fixed, toStringWith)
import Data.Semigroup ((<>))
import Data.Set (Set, member, singleton)
import Data.Show (show)
import Data.Traversable (find)
import Data.Tuple (Tuple(..))
import Data.Unit (Unit)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties (ScopeValue(..))
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData, fromEither)

type State
  = { overviewRows :: Array OverviewRow
    , attributi :: Array Attributo
    , selectedAttributi :: Set QualifiedAttributoName
    , overviewSorting :: OverviewRouteInput
    }

data Action
  = Resort OverviewRouteInput
  | ToggleAttributo QualifiedAttributoName

initialState :: ChildInput OverviewRouteInput (Tuple OverviewResponse AttributiResponse) -> State
initialState { input: overviewSorting, remoteData: Tuple overviewResponse attributiResponse } =
  { overviewRows: resort overviewSorting overviewResponse.overviewRows
  , selectedAttributi: foldMap (singleton <<< qualifiedAttributoName) attributiResponse.attributi
  , attributi: attributiResponse.attributi
  , overviewSorting
  }

component ::
  forall query output.
  H.Component HH.HTML query
    { sort :: Tuple AssociatedTable String
    , sortOrder :: SortOrder
    }
    output
    AppMonad
component = parentComponent fetchInitialData childComponent

-- component = H.mkComponent { initialState: \_ -> 0, render: \_ -> HH.text "todo", eval: H.mkEval H.defaultEval }
childComponent :: forall q. H.Component HH.HTML q (ChildInput OverviewRouteInput (Tuple OverviewResponse AttributiResponse)) ParentError AppMonad
childComponent =
  H.mkComponent
    { initialState
    , render
    , eval:
        H.mkEval
          H.defaultEval
            { handleAction = handleAction
            }
    }

resort :: OverviewRouteInput -> Array OverviewRow -> Array OverviewRow
resort by = sortBy (comparing (by.sortOrder) ((map _.value) <<< findCellInRow by.sort))

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  Resort newInput -> H.modify_ \state -> state { overviewSorting = newInput, overviewRows = resort newInput state.overviewRows }
  ToggleAttributo x -> do
    H.modify_ \state -> state { selectedAttributi = toggleSetElement x state.selectedAttributi }

fetchInitialData :: OverviewRouteInput -> AppMonad (RemoteData String (Tuple OverviewResponse AttributiResponse))
fetchInitialData _ = fromEither <$> (fanoutApplicative <$> retrieveOverview <*> retrieveAttributi)

schemaTypeSortable :: JSONSchemaType -> Boolean
schemaTypeSortable (JSONNumber _) = true

schemaTypeSortable JSONInteger = true

schemaTypeSortable _ = false

attributoSortable :: Attributo -> Boolean
attributoSortable a = schemaTypeSortable a.typeSchema

createUpdatedSortInput :: Boolean -> Attributo -> OverviewRouteInput -> OverviewRouteInput
createUpdatedSortInput doInvertOrder a { sort, sortOrder } =
  let
    qualifiedA = qualifiedAttributoName a
  in
    if qualifiedA == sort then
      { sortOrder: if doInvertOrder then invertOrder sortOrder else sortOrder, sort }
    else
      { sort: qualifiedA, sortOrder: Ascending }

isSortedBy :: State -> Attributo -> Boolean
isSortedBy state a = state.overviewSorting.sort == qualifiedAttributoName a

-- Given a list of cells with source, select the "best" source
selectProperSource :: Array OverviewCell -> Maybe OverviewCell
selectProperSource [] = Nothing

selectProperSource xs =
  let
    sourceOrder :: Array String
    sourceOrder = [ "manual", "offline", "online" ]

    sources :: Array OverviewCell
    sources = mapMaybe (\source -> find (\x -> x.source == source) xs) sourceOrder
  in
    head (if null sources then xs else sources)

-- Given a row in the overview and a certain attributo, find the correct attributo cell (proper source and stuff)
findCellInRow :: QualifiedAttributoName -> OverviewRow -> Maybe OverviewCell
findCellInRow (Tuple table name) cells =
  let
    foundCells = filter (\cell -> cell.table == table && cell.name == name) cells
  in
    selectProperSource foundCells

render :: forall cs. State -> H.ComponentHTML Action cs AppMonad
render state =
  let
    makeHeader :: forall w. Attributo -> HH.HTML w Action
    makeHeader t =
      let
        maybeSuffix :: Array (HH.HTML w Action)
        maybeSuffix = toArrayOf (attributoSuffix <<< to (\x -> HH.text (" [" <> x <> "]"))) t

        maybeOrderIcon = if isSortedBy state t then [ orderingToIcon state.overviewSorting.sortOrder, HH.text " " ] else []

        headerElements :: Array (HH.HTML w Action)
        headerElements = ([ HH.span [ singleClass "text-muted" ] [ HH.text (show t.table <> ".") ], HH.text (descriptiveAttributoText t) ] <> maybeSuffix)

        updatedSortInput doInvertOrder = createUpdatedSortInput doInvertOrder t (state.overviewSorting)

        cellContent =
          if attributoSortable t then
            maybeOrderIcon <> [ HH.a [ HP.href (createLink (Overview (updatedSortInput false))), HE.onClick \_ -> Just (Resort (updatedSortInput true)) ] headerElements ]
          else
            headerElements
      in
        HH.th
          [ scope ScopeCol, singleClass "text-nowrap" ]
          cellContent

    -- let --   isSortProp = rpName t == state.runSorting.sort --   afterSort = resortParams (rpName t) (state.runSorting) -- in HH.th [ scope ScopeCol ] (if null t.description then t.description else t.name)
    -- ( if rpIsSortable t then
    --     [ HH.a
    --         [ HP.href (createLink (Runs afterSort)), HE.onClick \_ -> Just (Resort afterSort) ]
    --         ([ HH.text (rpDescription t) ] <> (if isSortProp then [ orderingToIcon state.runSorting.sortOrder ] else []))
    --     ]
    --   else
    --     [ HH.text (rpDescription t) ]
    -- )
    -- makeComment :: forall w. Comment -> HH.HTML w Action
    -- makeComment c = HH.li_ [ HH.text (c.author <> ": " <> c.text) ]
    -- makeProperty :: forall w. RunProperty -> Maybe RunValue -> HH.HTML w Action
    -- makeProperty rp value = case value of
    --   Nothing -> HH.td_ []
    --   Just (Comments cs) -> HH.td_ [ HH.ul_ (makeComment <$> cs) ]
    --   Just (Scalar (RunScalarNumber n)) -> HH.td_ [ HH.text (toStringWith (precision 2) n) ]
    --   _ -> HH.td_ [ HH.text (maybe "" show value) ]
    wholeSelectedProps = filter (\a -> qualifiedAttributoName a `member` state.selectedAttributi) state.attributi

    numberToHtml :: forall w. Attributo -> Number -> HH.HTML w Action
    numberToHtml attributo n = case attributo.typeSchema of
      JSONInteger -> HH.text (show (round n))
      JSONNumber nd -> HH.text (toStringWith (fixed 2) n)
      _ -> HH.text "invalid"

    cellToHtml :: forall w. Attributo -> OverviewCell -> HH.HTML w Action
    cellToHtml attributo cell =
      HH.td_
        [ caseJson
            (const (HH.text ""))
            (HH.text <<< show)
            (numberToHtml attributo)
            HH.text
            (const (HH.text "array"))
            (const (HH.text "object"))
            cell.value
        ]

    makeCell :: forall w. OverviewRow -> Attributo -> HH.HTML w Action
    makeCell overviewRow attributo = maybe (HH.th_ []) (cellToHtml attributo) (findCellInRow (qualifiedAttributoName attributo) overviewRow)

    makeRow overviewRow = HH.tr_ (makeCell overviewRow <$> wholeSelectedProps)
  in
    fluidContainer
      [ plainH1_ "Experiment Overview"
      , selectedColumnChooser state
      , table
          [ TableStriped ]
          (makeHeader <$> wholeSelectedProps)
          (makeRow <$> state.overviewRows)
      ]

orderingToIcon :: forall w i. SortOrder -> HH.HTML w i
orderingToIcon Ascending = faIcon "sort-up"

orderingToIcon Descending = faIcon "sort-down"

selectedColumnChooser :: forall w. State -> HH.HTML w Action
selectedColumnChooser state =
  let
    makeRow :: Attributo -> HH.HTML w Action
    makeRow a =
      let
        qan = qualifiedAttributoName a
      in
        HH.button
          [ HP.type_ HP.ButtonButton
          , classList ([ "list-group-item", "list-group-flush", "list-group-item-action" ] <> (if qan `member` state.selectedAttributi then [ "active" ] else []))
          , HE.onClick \_ -> Just (ToggleAttributo qan)
          ]
          [ HH.text (show a.table <> "." <> descriptiveAttributoText a) ]
  in
    HH.div_
      [ HH.p_
          [ HH.button
              [ classList [ "btn", "btn-secondary" ]
              , HP.type_ HP.ButtonButton
              , HP.attr (HH.AttrName "data-toggle") "collapse"
              , HP.attr (HH.AttrName "data-target") "#columnChooser"
              ]
              [ faIcon "columns", HH.text " Choose columns" ]
          ]
      , HH.div [ singleClass "collapse", HP.id_ "columnChooser" ]
          [ HH.div [ singleClass "list-group " ]
              ( makeRow <$> state.attributi
              )
          ]
      ]
