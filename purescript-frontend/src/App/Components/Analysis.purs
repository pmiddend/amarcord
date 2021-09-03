module App.Components.Analysis where

import App.API (AnalysisResponse, AnalysisRow, DiffractionList, retrieveAnalysis)
import App.AppMonad (AppMonad)
import App.Bootstrap (TableFlag(..), fluidContainer, table)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.Halogen.FontAwesome (icon)
import App.HalogenUtils (AlertType(..), classList, faIcon, makeAlert, orderingToIcon, singleClass)
import App.Route (AnalysisRouteInput, Route(..), createLink)
import App.SortOrder (SortOrder(..), invertOrder)
import Control.Applicative (pure, (<*>))
import Control.Bind (bind, (>>=), discard)
import Data.Argonaut (Json, caseJson, fromObject, jsonNull, stringify)
import Data.Argonaut as Argonaut
import Data.Array (cons, elem, filter, find, findIndex, groupBy, index, length, mapMaybe, nub, sort)
import Data.Array.NonEmpty (NonEmptyArray, head, toUnfoldable)
import Data.Eq (class Eq, eq, (/=), (==))
import Data.Foldable (class Foldable, indexl)
import Data.FoldableWithIndex (class FoldableWithIndex, foldrWithIndex)
import Data.Function (const, identity, on, (<<<), (>>>))
import Data.Functor (class Functor, (<$>))
import Data.Int (fromNumber)
import Data.Map as Map
import Data.Maybe (Maybe(..), fromMaybe, maybe)
import Data.Monoid (mempty)
import Data.Ord (class Ord, (>))
import Data.Semigroup ((<>))
import Data.Show (show, class Show)
import Data.String (codePointFromChar, drop, dropWhile, takeWhile)
import Data.Traversable (foldMap)
import Data.Tuple (Tuple(..), fst, snd)
import Data.Unit (Unit, unit)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties (InputType(..))
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData(..), fromEither)
import Routing.Hash (setHash)

type State
  = { rows :: Array AnalysisRow
    , totalRows :: Int
    , totalDiffractions :: Int
    , columns :: Array String
    , displayRows :: Array AnalysisRow
    , sorting :: AnalysisRouteInput
    , errorMessage :: Maybe String
    , selectedColumns :: Array String
    }

data AnalysisData
  = AnalysisData AnalysisResponse

derive instance eqAssociatedTable :: Eq AnalysisData

derive instance ordAssociatedTable :: Ord AnalysisData

data Action
  = Initialize
  | Resort AnalysisRouteInput
  | DeselectAll
  | QuerySubmit
  | ToggleColumn String
  | QueryChange String

initialState :: ChildInput AnalysisRouteInput AnalysisData -> State
initialState { input: sorting, remoteData: AnalysisData analysisResponse } =
  let
    selectedColumns = [ "crystals_crystal_id", "crystals_created", "diff_run_id", "diff_diffraction", "dr_data_reduction_id" ]
  in
    { rows: analysisResponse.analysis
    , totalRows: analysisResponse.totalRows
    , totalDiffractions: analysisResponse.totalDiffractions
    , columns: analysisResponse.analysisColumns
    , displayRows: nub (rowsWithSelected analysisResponse.analysisColumns analysisResponse.analysis selectedColumns)
    , sorting
    , errorMessage: Nothing
    , selectedColumns
    }

component :: forall query output. H.Component HH.HTML query AnalysisRouteInput output AppMonad
component = parentComponent fetchData childComponent

childComponent :: forall q. H.Component HH.HTML q (ChildInput AnalysisRouteInput AnalysisData) ParentError AppMonad
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

updateHash :: forall slots action. H.HalogenM State action slots ParentError AppMonad Unit
updateHash = do
  s <- H.get
  H.liftEffect (setHash (createLink (Analysis s.sorting)))

refresh :: forall slots. Array String -> AnalysisRouteInput -> H.HalogenM State Action slots ParentError AppMonad Unit
refresh selectedColumns routeInput = do
  result <- H.lift (fetchData routeInput)
  updateHash
  case result of
    Success (AnalysisData { analysis, sqlError: Just sqlError }) ->
      H.modify_ \state ->
        state
          { errorMessage = Just ("SQL error: " <> sqlError)
          }
    Success (AnalysisData { analysisColumns, totalRows, totalDiffractions, analysis, sqlError: Nothing }) ->
      H.modify_ \state ->
        state
          { errorMessage = Nothing
          , rows = analysis
          , totalRows = totalRows
          , totalDiffractions = totalDiffractions
          , displayRows = nub (rowsWithSelected analysisColumns analysis selectedColumns)
          , sorting = routeInput
          }
    Failure e -> H.modify_ \state -> state { errorMessage = Just e }
    _ -> pure unit

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  Initialize -> pure unit
  QueryChange query -> H.modify_ \state -> state { sorting = state.sorting { filterQuery = query } }
  QuerySubmit -> do
    state <- H.get
    refresh state.selectedColumns state.sorting
  DeselectAll -> do
    H.modify_ \state -> state { selectedColumns = [] }
  ToggleColumn a -> do
    s <- H.get
    let
      newSelectedColumns = if a `elem` s.selectedColumns then filter (_ /= a) s.selectedColumns else sort (cons a s.selectedColumns)
    H.modify_ \state ->
      state
        { selectedColumns = newSelectedColumns
        , displayRows = nub (rowsWithSelected state.columns state.rows newSelectedColumns)
        }
  Resort routeInput -> do
    state <- H.get
    refresh state.selectedColumns routeInput

fetchData :: AnalysisRouteInput -> AppMonad (RemoteData String AnalysisData)
fetchData { filterQuery, sortColumn, sortOrder } = do
  analysis <- retrieveAnalysis filterQuery sortColumn sortOrder
  pure (fromEither (AnalysisData <$> analysis))

createUpdatedSortInput :: Boolean -> String -> AnalysisRouteInput -> AnalysisRouteInput
createUpdatedSortInput doInvertOrder newColumn { sortColumn, sortOrder, filterQuery } =
  if newColumn == sortColumn then
    { sortOrder: if doInvertOrder then invertOrder sortOrder else sortOrder, sortColumn: newColumn, filterQuery }
  else
    { sortColumn: newColumn, sortOrder: Ascending, filterQuery }

showCellContent :: Json -> String
showCellContent = caseJson (const "") show showNumber identity (const "array") (stringify <<< fromObject)
  where
  showNumber n = fromMaybe (show n) (show <$> (fromNumber n))

data ColumnGroup
  = Crystals
  | Diffractions
  | Reductions
  | Refinements
  | ReductionJobs
  | RefinementJobs
  | ToolsGroup
  | Other

derive instance eqColumnGroup :: Eq ColumnGroup

derive instance ordColumnGroup :: Ord ColumnGroup

instance showColumnGroup :: Show ColumnGroup where
  show Crystals = "Crystals"
  show Diffractions = "Diffractions"
  show Reductions = "Data_Reduction"
  show Refinements = "Refinement"
  show ReductionJobs = "Reduction_Jobs"
  show RefinementJobs = "Refinement_Jobs"
  show ToolsGroup = "Tools"
  show Other = "Other"

groupPrefixes :: Array (Tuple String ColumnGroup)
groupPrefixes =
  [ Tuple "diff" Diffractions
  , Tuple "dr" Reductions
  , Tuple "crystals" Crystals
  , Tuple "redjobs" ReductionJobs
  , Tuple "rfjobs" RefinementJobs
  , Tuple "tools" ToolsGroup
  , Tuple "ref" Refinements
  ]

toColumnGroup :: String -> ColumnGroup
toColumnGroup s = fromMaybe Other (snd <$> find (fst >>> (_ == s)) groupPrefixes)

makeGroupedEntry :: String -> GroupedEntry
makeGroupedEntry s =
  let
    prefix = takeWhile (_ /= codePointFromChar '_') s

    group = toColumnGroup prefix

    rest = case group of
      Other -> s
      _ -> drop 1 (dropWhile (_ /= codePointFromChar '_') s)
  in
    { group
    , rest
    , original: s
    }

postprocessColumnName :: forall w i. String -> Array (HH.HTML w i)
postprocessColumnName s =
  let
    { group, rest } = makeGroupedEntry s
  in
    [ HH.span [ singleClass "text-muted" ] [ HH.text (show group <> ".") ]
    , HH.text rest
    ]

type GroupedEntry
  = { group :: ColumnGroup
    , original :: String
    , rest :: String
    }

renderColumnChooser :: forall w. State -> HH.HTML w Action
renderColumnChooser state =
  let
    columnGroups :: Array (NonEmptyArray GroupedEntry)
    columnGroups = groupBy ((==) `on` (\x -> x.group)) (makeGroupedEntry <$> state.columns)

    makeColumn :: NonEmptyArray GroupedEntry -> HH.HTML w Action
    makeColumn col =
      HH.div [ singleClass "col" ]
        [ HH.h3_ [ HH.text (show (head col).group) ]
        , HH.div [ singleClass "list-group " ]
            (makeRow <$> toUnfoldable col)
        ]

    makeRow :: GroupedEntry -> HH.HTML w Action
    makeRow a =
      HH.button
        [ HP.type_ HP.ButtonButton
        , classList ([ "list-group-item", "list-group-flush", "list-group-item-action" ] <> (if a.original `elem` state.selectedColumns then [ "active" ] else []))
        , HE.onClick \_ -> Just (ToggleColumn a.original)
        ]
        [ HH.text a.rest ]

    disableAll =
      HH.p_
        [ HH.button
            [ classList [ "btn", "btn-secondary" ]
            , HE.onClick \_ -> Just DeselectAll
            ]
            [ HH.text "Disable all" ]
        ]
  in
    HH.div_
      [ HH.div [ singleClass "row" ]
          [ HH.div [ singleClass "col" ]
              [ HH.p_
                  [ HH.button
                      [ classList [ "btn", "btn-secondary" ]
                      , HP.type_ HP.ButtonButton
                      , HP.attr (HH.AttrName "data-bs-toggle") "collapse"
                      , HP.attr (HH.AttrName "data-bs-target") "#columnChooser"
                      ]
                      [ faIcon "columns", HH.text " Choose columns" ]
                  ]
              ]
          , HH.div [ singleClass "col" ]
              [ HH.div [ classList [ "input-group", "mb-3" ] ]
                  [ HH.input
                      [ HP.type_ InputSearch
                      , HE.onValueChange (Just <<< QueryChange)
                      , HP.placeholder "Filter query"
                      , singleClass "form-control"
                      , HP.value state.sorting.filterQuery
                      ]
                  , HH.button
                      [ classList [ "btn", "btn-secondary" ]
                      , HE.onClick (const (Just QuerySubmit))
                      ]
                      [ HH.text "Refresh" ]
                  , HH.button
                      [ classList [ "btn", "btn-info" ]
                      , HP.type_ HP.ButtonButton
                      , HP.attr (HH.AttrName "data-bs-toggle") "collapse"
                      , HP.attr (HH.AttrName "data-bs-target") "#helpDisplayer"
                      ]
                      [ HH.text "Help" ]
                  ]
              ]
          ]
      , HH.div [ singleClass "collapse bg-light shadow p-3", HP.id_ "helpDisplayer" ]
          [ HH.h3_ [ HH.text "Example queries" ]
          , HH.ul_
              [ HH.li_ [ HH.text "Column greater than: ", HH.pre_ [ HH.text "Diffractions.angle_step > 0.1" ] ]
              , HH.li_ [ HH.text "Column equal to: ", HH.pre_ [ HH.text "Diffractions.beamline = \"p11\"" ] ]
              , HH.li_ [ HH.text "Text contains: ", HH.pre_ [ HH.text "Crystals.crystal_id LIKE '%cryst%'" ] ]
              , HH.li_ [ HH.text "Crystals without diffractions: ", HH.pre_ [ HH.text "Diffractions.run_id IS NULL" ] ]
              , HH.li_ [ HH.text "Crystals without reductions: ", HH.pre_ [ HH.text "Data_Reduction.data_reduction_id IS NULL" ] ]
              , HH.li_ [ HH.text "Comparison with a date/time column (just date): ", HH.pre_ [ HH.text "Crystals.created > '2021-05-01'" ] ]
              , HH.li_ [ HH.text "Comparison with a date/time column (date and time): ", HH.pre_ [ HH.text "Crystals.created > '2021-05-01 15:00:00'" ] ]
              , HH.li_ [ HH.text "Combining expressions with AND: ", HH.pre_ [ HH.text "Diffractions.beamline = \"p11\" AND Diffractions.angle_step > 0.1" ] ]
              ]
          , HH.h3_ [ HH.text "Troubleshooting" ]
          , HH.h4_ [ HH.text "Searches involving the crystal ID" ]
          , HH.p_ [ HH.text "When you want to reference the crystal ID, you have to use ", HH.span [ singleClass "font-monospace" ] [ HH.text "Crystals.crystal_id" ], HH.text " as the column name." ]
          ]
      , HH.div [ singleClass "collapse", HP.id_ "columnChooser" ]
          [ HH.div [ singleClass "row p-1 bg-light shadow" ] (makeColumn <$> columnGroups) ]
      ]

analysisColumnIsSortable :: forall t457. t457 -> Boolean
analysisColumnIsSortable = const true

rowsWithSelected :: forall t33 t43 t50. FoldableWithIndex Int t33 => Functor t43 => Foldable t50 => t33 String -> t43 (t50 Json) -> Array String -> t43 (Array Json)
rowsWithSelected columns rows selected =
  let
    columnsWithIndices :: Map.Map String Int
    columnsWithIndices = foldrWithIndex (\i columnName prior -> Map.insert columnName i prior) mempty columns

    selectedColumnIndices :: Array Int
    selectedColumnIndices = mapMaybe (\columnName -> Map.lookup columnName columnsWithIndices) selected
  in
    (\row -> (\columnIndex -> fromMaybe jsonNull (indexl columnIndex row)) <$> selectedColumnIndices) <$> rows

render :: forall slots. State -> H.ComponentHTML Action slots AppMonad
render state =
  let
    headers = (\x -> Tuple x (postprocessColumnName x)) <$> state.selectedColumns

    makeHeader (Tuple col title) =
      let
        updatedSortInput doInvertOrder = createUpdatedSortInput doInvertOrder col state.sorting

        maybeOrderIcon = if state.sorting.sortColumn == col then [ orderingToIcon state.sorting.sortOrder, HH.text " " ] else []

        cellContent =
          if analysisColumnIsSortable col then
            maybeOrderIcon
              <> [ HH.a [ singleClass "text-decoration-none", HP.href (createLink (Analysis (updatedSortInput false))), HE.onClick \_ -> Just (Resort (updatedSortInput true)) ]
                    title
                ]
          else
            title
      in
        HH.th [ singleClass "text-nowrap text-center" ] cellContent

    makeRow :: forall w. AnalysisRow -> HH.HTML w Action
    makeRow row = HH.tr_ ((\cell -> HH.td [ singleClass "text-center" ] [ HH.text (showCellContent cell) ]) <$> row)

    diffractionList :: DiffractionList
    diffractionList =
      let
        crystalIdColumn' = findIndex (eq "diff_crystal_id") state.columns

        runIdColumn' = findIndex (eq "diff_run_id") state.columns

        colIdxs' :: Maybe (Tuple Int Int)
        colIdxs' = Tuple <$> crystalIdColumn' <*> runIdColumn'

        unindexCols :: Tuple Int Int -> AnalysisRow -> Maybe (Tuple Json Json)
        unindexCols (Tuple c r) row = Tuple <$> index row c <*> index row r

        decomposeCols :: Tuple Json Json -> Maybe (Tuple String Int)
        decomposeCols (Tuple c r) = Tuple <$> (Argonaut.toString c) <*> (Argonaut.toNumber r >>= fromNumber)

        processRow :: Tuple Int Int -> AnalysisRow -> Maybe (Tuple String Int)
        processRow colIdxs row = unindexCols colIdxs row >>= decomposeCols
      in
        nub (foldMap (\colIdxs -> mapMaybe (processRow colIdxs) state.rows) colIdxs')
  in
    fluidContainer
      [ HH.h2_ [ icon { name: "table", size: Nothing, spin: false }, HH.text " Analysis Results" ]
      , HH.hr_
      , renderColumnChooser state
      , HH.hr_
      , maybe (HH.text "") (makeAlert AlertDanger) state.errorMessage
      , (if state.totalRows > length state.rows then makeAlert AlertInfo "Results have been capped" else HH.text "")
      , HH.h5_ [ HH.text ((show state.totalRows) <> " result" <> (if state.totalRows > 1 then "s" else "")), HH.br_, HH.em [ singleClass "text-muted" ] [  HH.text "including attached reductions/refinements" ] ]
      , table
          "analysis-table"
          [ TableStriped, TableSmall, TableBordered ]
          (makeHeader <$> headers)
          (makeRow <$> state.displayRows)
      ]
