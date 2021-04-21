module App.Components.Overview where

import App.API (AttributiResponse, OverviewCell, OverviewResponse, OverviewRow, retrieveAttributi, retrieveOverview)
import App.AppMonad (AppMonad, log)
import App.AssociatedTable (AssociatedTable(..))
import App.Attributo (Attributo, attributoSuffix, descriptiveAttributoText, qualifiedAttributoName)
import App.Bootstrap (TableFlag(..), fluidContainer, plainH1_, table)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.HalogenUtils (classList, errorText, faIcon, scope, singleClass)
import App.JSONSchemaType (JSONSchemaType(..))
import App.Logging (LogLevel(..))
import App.QualifiedAttributoName (QualifiedAttributoName)
import App.Route (Route(..), OverviewRouteInput, createLink)
import App.SortOrder (SortOrder(..), comparing, invertOrder)
import App.Utils (fanoutApplicative, toggleSetElement)
import Control.Applicative (pure)
import Control.Apply ((<*>))
import Control.Bind (bind, discard)
import Data.Argonaut (Json, JsonDecodeError, caseJson, decodeJson, printJsonDecodeError)
import Data.Array (filter, head, length, mapMaybe, singleton, sortBy)
import Data.Either (Either(..))
import Data.Eq ((/=), (==))
import Data.Foldable (foldMap, intercalate, null)
import Data.Function (const, identity, (<<<), (>>>))
import Data.Functor (map, (<$>))
import Data.HeytingAlgebra ((&&))
import Data.Int (round)
import Data.Lens (to, toArrayOf)
import Data.Maybe (Maybe(..), maybe)
import Data.Number.Format (fixed, toStringWith)
import Data.Semigroup ((<>))
import Data.Set as Set
import Data.Show (show)
import Data.Traversable (find, traverse)
import Data.Tuple (Tuple(..), fst)
import Data.Unit (Unit, unit)
import Effect.Aff.Class (class MonadAff)
import Effect.Timer (clearTimeout, setTimeout)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties (ScopeValue(..))
import Halogen.HTML.Properties as HP
import Halogen.Query.EventSource (EventSource, Finalizer(..), effectEventSource, emit)
import Network.RemoteData (RemoteData(..), fromEither)

type State
  = { overviewRows :: Array OverviewRow
    , attributi :: Array Attributo
    , selectedAttributi :: Set.Set QualifiedAttributoName
    , overviewSorting :: OverviewRouteInput
    }

data Action
  = Initialize
  | Resort OverviewRouteInput
  | ToggleAttributo QualifiedAttributoName
  | ToggleTable AssociatedTable
  | RefreshTimeout

initialState :: ChildInput OverviewRouteInput (Tuple OverviewResponse AttributiResponse) -> State
initialState { input: overviewSorting, remoteData: Tuple overviewResponse attributiResponse } =
  { overviewRows: resort overviewSorting overviewResponse.overviewRows
  , selectedAttributi: foldMap (Set.singleton <<< qualifiedAttributoName) attributiResponse.attributi
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
component = parentComponent fetchData childComponent

childComponent :: forall q. H.Component HH.HTML q (ChildInput OverviewRouteInput (Tuple OverviewResponse AttributiResponse)) ParentError AppMonad
childComponent =
  H.mkComponent
    { initialState
    , render
    , eval:
        H.mkEval
          H.defaultEval
            { handleAction = handleAction
            , initialize = Just Initialize
            }
    }

resort :: OverviewRouteInput -> Array OverviewRow -> Array OverviewRow
resort by = sortBy (comparing (by.sortOrder) ((map _.value) <<< findCellInRow by.sort))

timerEventSource :: forall m. MonadAff m => EventSource m Action
timerEventSource =
  effectEventSource \emitter -> do
    timerId <- setTimeout 5000 (emit emitter RefreshTimeout)
    pure (Finalizer (clearTimeout timerId))

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  Initialize -> do
    _ <- H.subscribe timerEventSource
    pure unit
  RefreshTimeout -> do
    H.lift (log Info "Refreshing...")
    s <- H.get
    remoteData <- H.lift (fetchData s.overviewSorting)
    case remoteData of
      Success (Tuple newOverviewRows newAttributi) ->
        H.modify_ \state ->
          state
            { overviewRows = resort s.overviewSorting newOverviewRows.overviewRows
            , attributi = newAttributi.attributi
            }
      _ -> pure unit
    _ <- H.subscribe timerEventSource
    pure unit
  Resort newInput ->
    H.modify_ \state ->
      state
        { overviewSorting = newInput
        , overviewRows = resort newInput state.overviewRows
        }
  ToggleAttributo x -> do
    H.modify_ \state -> state { selectedAttributi = toggleSetElement x state.selectedAttributi }
  ToggleTable t -> do
    H.modify_ \state -> state { selectedAttributi = Set.filter (\x -> fst x /= t) state.selectedAttributi }

fetchData :: OverviewRouteInput -> AppMonad (RemoteData String (Tuple OverviewResponse AttributiResponse))
fetchData _ = fromEither <$> (fanoutApplicative <$> retrieveOverview <*> retrieveAttributi)

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

-- Check if the current state is sorted by the given attributo
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

numberToString :: Number -> String
numberToString = toStringWith (fixed 2)

numberToHtml :: forall w. JSONSchemaType -> Number -> HH.HTML w Action
numberToHtml typeSchema n = case typeSchema of
  JSONInteger -> HH.text (show (round n))
  JSONNumber nd -> HH.text (numberToString n)
  _ -> HH.text "invalid"

type Comment
  = { author :: String
    , created :: String
    , text :: String
    , id :: Int
    }

commentsToHtml :: forall w. Array Json -> HH.HTML w Action
commentsToHtml comments =
  let
    decodedComments' :: Either JsonDecodeError (Array Comment)
    decodedComments' = traverse decodeJson comments
  in
    case decodedComments' of
      Left e -> errorText ("invalid comment: " <> printJsonDecodeError e)
      Right decodedComments ->
        let
          sorted = sortBy (comparing Descending _.created) decodedComments

          firstComment = head sorted

          numberOfComments = length sorted
        in
          case firstComment of
            Nothing -> HH.text ""
            Just c ->
              let
                prefix = [ HH.em_ [ HH.text c.author ], HH.text (": " <> c.text) ]
              in
                if numberOfComments == 1 then
                  HH.p_ prefix
                else
                  HH.p_ (prefix <> [ HH.span [ singleClass "text-muted" ] [ HH.text (" [+" <> show numberOfComments <> " more]") ] ])

listToHtml :: forall w. Attributo -> Array Json -> HH.HTML w Action
listToHtml attributo list = case attributo.typeSchema of
  JSONComments -> commentsToHtml list
  JSONArray items ->
    let
      subItemToHtml :: Json -> HH.HTML w Action
      subItemToHtml i =
        HH.text
          ( caseJson
              (const "")
              show
              numberToString
              identity
              (const "nested lists not supported")
              (const "objects not supported")
              i
          )
    in
      HH.div_ (intercalate [ HH.text "," ] ((subItemToHtml >>> singleton) <$> list))
  _ -> errorText "list which is not an array"

jsonToHtml :: forall w. Attributo -> Json -> HH.HTML w Action
jsonToHtml attributo =
  caseJson
    (const (HH.text ""))
    (HH.text <<< show)
    (numberToHtml attributo.typeSchema)
    HH.text
    (listToHtml attributo)
    (const (HH.text "object"))

cellToHtml :: forall w. Attributo -> OverviewCell -> HH.HTML w Action
cellToHtml attributo cell = HH.td [ singleClass "text-nowrap" ] [ jsonToHtml attributo cell.value ]

makeCell :: forall w. OverviewRow -> Attributo -> HH.HTML w Action
makeCell overviewRow attributo =
  maybe
    (HH.td_ [])
    (cellToHtml attributo)
    (findCellInRow (qualifiedAttributoName attributo) overviewRow)

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

    -- makeComment :: forall w. Comment -> HH.HTML w Action
    -- makeComment c = HH.li_ [ HH.text (c.author <> ": " <> c.text) ]
    -- makeProperty :: forall w. RunProperty -> Maybe RunValue -> HH.HTML w Action
    -- makeProperty rp value = case value of
    --   Nothing -> HH.td_ []
    --   Just (Comments cs) -> HH.td_ [ HH.ul_ (makeComment <$> cs) ]
    --   Just (Scalar (RunScalarNumber n)) -> HH.td_ [ HH.text (toStringWith (precision 2) n) ]
    --   _ -> HH.td_ [ HH.text (maybe "" show value) ]
    wholeSelectedProps = filter (\a -> qualifiedAttributoName a `Set.member` state.selectedAttributi) state.attributi

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
          , classList ([ "list-group-item", "list-group-flush", "list-group-item-action" ] <> (if qan `Set.member` state.selectedAttributi then [ "active" ] else []))
          , HE.onClick \_ -> Just (ToggleAttributo qan)
          ]
          [ HH.text (show a.table <> "." <> descriptiveAttributoText a) ]

    disableRow t =
      HH.p_
        [ HH.button
            [ classList [ "btn", "btn-secondary" ]
            , HE.onClick \_ -> Just (ToggleTable t)
            ]
            [ HH.text "Disable all" ]
        ]
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
          [ HH.div [ singleClass "row" ]
              [ HH.div [ singleClass "col" ]
                  [ HH.h3_ [ HH.text "Sample" ]
                  , disableRow Sample
                  , HH.div [ singleClass "list-group " ]
                      (makeRow <$> (filter (\a -> a.table == Sample) state.attributi))
                  ]
              , HH.div [ singleClass "col" ]
                  [ HH.h3_ [ HH.text "Run" ]
                  , disableRow Run
                  , HH.div [ singleClass "list-group " ]
                      (makeRow <$> (filter (\a -> a.table == Run) state.attributi))
                  ]
              ]
          ]
      ]
