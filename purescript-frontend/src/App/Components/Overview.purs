module App.Components.Overview where

import App.API
  ( AttributiResponse
  , MiniSamplesResponse
  , OverviewResponse
  , changeRunSample
  , retrieveAttributi
  , retrieveMiniSamples
  , retrieveOverview
  )
import App.AppMonad (AppMonad, log)
import App.AssociatedTable (AssociatedTable(..))
import App.Attributo
  ( Attributo
  , attributoSuffix
  , descriptiveAttributoText
  , qualifiedAttributoName
  )
import App.Bootstrap (TableFlag(..), fluidContainer, plainH1_, table)
import App.Comment (Comment)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.Formatting
  ( floatNumberToString
  , intToString
  , parseExternalDate
  , prettyPrintExternalDate
  )
import App.HalogenUtils
  ( AlertType(..)
  , classList
  , errorText
  , faIcon
  , makeAlert
  , scope
  , singleClass
  )
import App.JSONSchemaType (JSONSchemaStringFormat(..), JSONSchemaType(..))
import App.Logging (LogLevel(..))
import App.MiniSample (MiniSample)
import App.Overview (OverviewRow, OverviewCell, findCellInRow)
import App.QualifiedAttributoName (QualifiedAttributoName)
import App.Route (Route(..), OverviewRouteInput, createLink)
import App.SortOrder (SortOrder(..), comparing, invertOrder)
import App.Utils (toggleSetElement)
import Control.Applicative (pure)
import Control.Apply ((<*>))
import Control.Bind (bind, discard, (>>=))
import Data.Argonaut
  ( Json
  , JsonDecodeError
  , caseJson
  , decodeJson
  , printJsonDecodeError
  , toNumber
  )
import Data.Array (filter, find, head, length, singleton, sortBy)
import Data.Either (Either(..))
import Data.Eq ((/=), (==), class Eq)
import Data.Foldable (foldMap, intercalate)
import Data.Function (const, identity, (<<<), (>>>))
import Data.Functor (map, (<$>))
import Data.HeytingAlgebra ((&&), (||))
import Data.Int (fromString, round)
import Data.Lens (to, toArrayOf)
import Data.Maybe (Maybe(..), fromMaybe, maybe)
import Data.Ord (class Ord)
import Data.Semigroup ((<>))
import Data.Set as Set
import Data.Show (show)
import Data.Traversable (traverse)
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
    , miniSamples :: Array MiniSample
    , selectedAttributi :: Set.Set QualifiedAttributoName
    , overviewSorting :: OverviewRouteInput
    }

data OverviewData
  = OverviewData OverviewResponse AttributiResponse MiniSamplesResponse

derive instance eqAssociatedTable :: Eq OverviewData

derive instance ordAssociatedTable :: Ord OverviewData

data Action
  = Initialize
  | Resort OverviewRouteInput
  | ToggleAttributo QualifiedAttributoName
  | ToggleTable AssociatedTable
  | RefreshTimeout
  | SampleChange Int Int

initialState :: ChildInput OverviewRouteInput OverviewData -> State
initialState { input: overviewSorting, remoteData: OverviewData overviewResponse attributiResponse miniSampleResponse } =
  { overviewRows: resort overviewSorting overviewResponse.overviewRows
  , selectedAttributi: foldMap (Set.singleton <<< qualifiedAttributoName) attributiResponse.attributi
  , attributi: attributiResponse.attributi
  , miniSamples: miniSampleResponse.samples
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

childComponent :: forall q. H.Component HH.HTML q (ChildInput OverviewRouteInput OverviewData) ParentError AppMonad
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

-- Given a sorting input and an array, resort it
resort :: OverviewRouteInput -> Array OverviewRow -> Array OverviewRow
resort by = sortBy (comparing (by.sortOrder) ((map _.value) <<< findCellInRow by.sort))

-- Event source for a simple timer
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
  SampleChange runId newSampleId -> do
    result <- H.lift (changeRunSample runId newSampleId)
    pure unit
  RefreshTimeout -> do
    H.lift (log Info "Refreshing...")
    s <- H.get
    remoteData <- H.lift (fetchData s.overviewSorting)
    case remoteData of
      Success (OverviewData newOverviewRows newAttributi newMiniSamples) ->
        H.modify_ \state ->
          state
            { overviewRows = resort s.overviewSorting newOverviewRows.overviewRows
            , attributi = newAttributi.attributi
            , miniSamples = newMiniSamples.samples
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

-- Fetch runs and attributi
fetchData :: OverviewRouteInput -> AppMonad (RemoteData String OverviewData)
fetchData _ = do
  overview <- retrieveOverview
  attributi <- retrieveAttributi
  samples <- retrieveMiniSamples
  let
    result :: Either String OverviewData
    result = OverviewData <$> overview <*> attributi <*> samples
  pure (fromEither result)

-- Determine if we have a sortable schema type
schemaTypeSortable :: JSONSchemaType -> Boolean
schemaTypeSortable (JSONNumber _) = true

schemaTypeSortable (JSONInteger _) = true

schemaTypeSortable _ = false

-- Determine if the attributo is sortable
attributoSortable :: Attributo -> Boolean
attributoSortable a = schemaTypeSortable a.typeSchema

-- When a column is clicked, create new route
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

sampleSelect :: forall w. Array MiniSample -> Int -> Int -> HH.HTML w Action
sampleSelect samples runId sampleSelectedId =
  let
    makeOption sample =
      HH.option
        [ HP.value (show sample.id), HP.selected (sample.id == sampleSelectedId) ]
        [ HH.text sample.name ]
  in
    HH.select
      [ classList [ "form-select", "form-control" ], HE.onValueChange (fromString >>> map (SampleChange runId)) ]
      (makeOption <$> samples)

type TableRowContext
  = { samples :: Array MiniSample
    , runId :: Int
    }

-- Convert a number (int and string) to HTML
numberToHtml :: forall w. TableRowContext -> JSONSchemaType -> Number -> HH.HTML w Action
numberToHtml trc typeSchema n = case typeSchema of
  JSONInteger id -> case id.format of
    Just "sample-id" -> sampleSelect trc.samples trc.runId (round n)
    _ -> HH.text (intToString n)
  JSONNumber nd -> HH.text (floatNumberToString n)
  _ -> errorText "Cannot convert from non-number type to HTML"

-- Convert a list of comments (as JSON) to HTML
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

-- Convert a list type (comments, list of int, ...) to HTML
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
              floatNumberToString
              identity
              (const "nested lists not supported")
              (const "objects not supported")
              i
          )
    in
      HH.div_ (intercalate [ HH.text "," ] ((subItemToHtml >>> singleton) <$> list))
  _ -> errorText "list which is not an array"

stringAttributoToHtml :: forall w. Attributo -> String -> HH.HTML w Action
stringAttributoToHtml a s = case a.typeSchema of
  JSONString (Just JSONStringDateTime) -> case parseExternalDate s of
    Left e -> makeAlert AlertDanger ("invalid date time \"" <> s <> "\", not the right format: " <> e)
    Right v -> HH.text (prettyPrintExternalDate v)
  JSONString Nothing -> HH.text s
  _ -> HH.text s

-- Convert a general attributo cell as JSON (number, comments, ...) to HTML
jsonToHtml :: forall w. TableRowContext -> Attributo -> Json -> HH.HTML w Action
jsonToHtml trc attributo =
  caseJson
    (const (HH.text ""))
    (HH.text <<< show)
    (numberToHtml trc attributo.typeSchema)
    (stringAttributoToHtml attributo)
    (listToHtml attributo)
    (const (HH.text "object"))

-- Convert an attributo cell to a HTML table cell
cellToHtml :: forall w. TableRowContext -> Attributo -> OverviewCell -> HH.HTML w Action
cellToHtml trc attributo cell = HH.td [ singleClass "text-nowrap" ] [ jsonToHtml trc attributo cell.value ]

runIdFromOverview :: OverviewRow -> Int
runIdFromOverview or =
  let
    valueAsInt :: Json -> Maybe Int
    valueAsInt x = round <$> toNumber x
  in
    fromMaybe 1 (find (\r -> r.table == Run && r.name == "id") or >>= ((\x -> x.value) >>> valueAsInt))

-- Convert an attributo inside a whole row to HTML
makeCell :: forall w. Array MiniSample -> OverviewRow -> Attributo -> HH.HTML w Action
makeCell samples overviewRow attributo =
  maybe
    (HH.td_ [])
    (cellToHtml { samples, runId: runIdFromOverview overviewRow } attributo)
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
        headerElements = ([ HH.span [ singleClass "text-muted" ] [ HH.text (show t.table <> ".") ], HH.text (descriptiveAttributoText t) ]) -- <> maybeSuffix)

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

    attributoSortFunction attributo = Tuple (attributo.name /= "id" || attributo.table /= Run) (Tuple attributo.table attributo.name)

    wholeSelectedProps = sortBy (comparing Ascending attributoSortFunction) (filter (\a -> qualifiedAttributoName a `Set.member` state.selectedAttributi) state.attributi)

    makeRow overviewRow = HH.tr_ (makeCell state.miniSamples overviewRow <$> wholeSelectedProps)
  in
    fluidContainer
      [ plainH1_ "Experiment Overview"
      , selectedColumnChooser state
      , table
          [ TableStriped ]
          (makeHeader <$> wholeSelectedProps)
          (makeRow <$> state.overviewRows)
      ]

-- Convert a sort order to an icon
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
