module App.Components.Analysis where

import App.API (AnalysisColumn(..), AnalysisResponse, AnalysisRow, allAnalysisColumns, retrieveAnalysis)
import App.AppMonad (AppMonad)
import App.Bootstrap (TableFlag(..), fluidContainer, table)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.Halogen.FontAwesome (icon)
import App.HalogenUtils (AlertType(..), classList, faIcon, makeAlert, singleClass)
import App.Route (AnalysisRouteInput, Route(..), createLink)
import App.SortOrder (SortOrder(..), invertOrder)
import Control.Applicative (pure)
import Control.Bind (bind)
import Control.Monad ((>>=))
import Data.Eq (class Eq, (==))
import Data.Function ((<<<))
import Data.Functor ((<$>))
import Data.Maybe (Maybe(..), fromMaybe, maybe)
import Data.Ord (class Ord)
import Data.Semigroup ((<>))
import Data.Set (Set, delete, empty, fromFoldable, insert, member, toUnfoldable)
import Data.Show (show)
import Data.Tuple (Tuple(..))
import Data.Unit (Unit, unit)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties (InputType(..))
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData(..), fromEither)

type DewarEdit
  = { editDewarPosition :: Int
    , editPuckId :: String
    }

type State
  = { rows :: Array AnalysisRow
    , sorting :: AnalysisRouteInput
    , errorMessage :: Maybe String
    , selectedColumns :: Set AnalysisColumn
    }

data AnalysisData
  = AnalysisData AnalysisResponse

derive instance eqAssociatedTable :: Eq AnalysisData

derive instance ordAssociatedTable :: Ord AnalysisData

data Action
  = Initialize
  | Resort AnalysisRouteInput
  | DeselectAll
  | ToggleColumn AnalysisColumn

-- Convert a sort order to an icon
orderingToIcon :: forall w i. SortOrder -> HH.HTML w i
orderingToIcon Ascending = faIcon "sort-up"

orderingToIcon Descending = faIcon "sort-down"

initialState :: ChildInput AnalysisRouteInput AnalysisData -> State
initialState { input: sorting, remoteData: AnalysisData analysisResponse } =
  { rows: analysisResponse.analysis
  , sorting
  , errorMessage: Nothing
  , selectedColumns: fromFoldable [ CrystalID, AnalysisTime, RunID, Comment, DataReductionID, ResolutionCC, ResolutionIsigI ]
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

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  Initialize -> pure unit
  DeselectAll -> do
    let
      emptyColumns :: Set AnalysisColumn
      emptyColumns = empty
    H.modify_ \state -> state { selectedColumns = emptyColumns }
  ToggleColumn a -> do
    s <- H.get
    H.modify_ \state ->
      state
        { selectedColumns = if a `member` s.selectedColumns then delete a s.selectedColumns else insert a s.selectedColumns
        }
  Resort routeInput -> do
    result <- H.lift (fetchData routeInput)
    case result of
      Success (AnalysisData { analysis }) ->
        H.modify_ \state ->
          state
            { errorMessage = Nothing
            , rows = analysis
            , sorting = routeInput
            }
      Failure e -> H.modify_ \state -> state { errorMessage = Just e }
      _ -> pure unit

fetchData :: AnalysisRouteInput -> AppMonad (RemoteData String AnalysisData)
fetchData { sortColumn, sortOrder } = do
  analysis <- retrieveAnalysis sortColumn sortOrder
  pure (fromEither (AnalysisData <$> analysis))

createUpdatedSortInput :: Boolean -> AnalysisColumn -> AnalysisRouteInput -> AnalysisRouteInput
createUpdatedSortInput doInvertOrder newColumn { sortColumn, sortOrder } =
  if newColumn == sortColumn then
    { sortOrder: if doInvertOrder then invertOrder sortOrder else sortOrder, sortColumn: newColumn }
  else
    { sortColumn: newColumn, sortOrder: Ascending }

analysisColumnIsSortable :: AnalysisColumn -> Boolean
analysisColumnIsSortable CrystalID = true

analysisColumnIsSortable AnalysisTime = true

analysisColumnIsSortable DataReductionID = true

analysisColumnIsSortable ResolutionCC = true

analysisColumnIsSortable ResolutionIsigI = true

analysisColumnIsSortable _ = false

analysisColumnToHumanString :: AnalysisColumn -> String
analysisColumnToHumanString CrystalID = "Crystal ID"

analysisColumnToHumanString RunID = "Run"

analysisColumnToHumanString Comment = "Comment"

analysisColumnToHumanString AnalysisTime = "Analysis time"

analysisColumnToHumanString DataReductionID = "Red. ID"

analysisColumnToHumanString ResolutionCC = "Res. CC"

analysisColumnToHumanString ResolutionIsigI = "Res. I/σ(I)"

analysisColumnToHumanString CellA = "a"

analysisColumnToHumanString CellB = "b"

analysisColumnToHumanString CellC = "c"

analysisColumnToHumanString CellAlpha = "α"

analysisColumnToHumanString CellBeta = "β"

analysisColumnToHumanString CellGamma = "γ"

extractFromRow :: forall w i. AnalysisRow -> AnalysisColumn -> HH.HTML w i
extractFromRow row CrystalID = HH.text row.crystalId

extractFromRow row RunID = HH.text (maybe "" show (_.runId <$> row.diffraction))

extractFromRow row Comment = HH.text (fromMaybe "" (_.comment <$> row.diffraction))

extractFromRow row AnalysisTime = HH.text (fromMaybe "" (_.analysisTime <$> row.dataReduction))

extractFromRow row DataReductionID = HH.text (maybe "" show (_.dataReductionId <$> row.dataReduction))

extractFromRow row ResolutionCC = HH.text (fromMaybe "" (row.dataReduction >>= _.resolutionCc >>= (pure <<< show)))

extractFromRow row ResolutionIsigI = HH.text (fromMaybe "" (row.dataReduction >>= _.resolutionIsigma >>= (pure <<< show)))

extractFromRow row CellA = HH.text (maybe "" show (_.a <$> row.dataReduction))

extractFromRow row CellB = HH.text (maybe "" show (_.b <$> row.dataReduction))

extractFromRow row CellC = HH.text (maybe "" show (_.c <$> row.dataReduction))

extractFromRow row CellAlpha = HH.text (maybe "" show (_.alpha <$> row.dataReduction))

extractFromRow row CellBeta = HH.text (maybe "" show (_.beta <$> row.dataReduction))

extractFromRow row CellGamma = HH.text (maybe "" show (_.gamma <$> row.dataReduction))

renderColumnChooser :: forall w. State -> HH.HTML w Action
renderColumnChooser state =
  let
    makeRow :: AnalysisColumn -> HH.HTML w Action
    makeRow a =
      HH.button
        [ HP.type_ HP.ButtonButton
        , classList ([ "list-group-item", "list-group-flush", "list-group-item-action" ] <> (if a `member` state.selectedColumns then [ "active" ] else []))
        , HE.onClick \_ -> Just (ToggleColumn a)
        ]
        [ HH.text (analysisColumnToHumanString a) ]

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
                      [ HP.type_ InputText
                      --                 , HE.onValueChange (Just <<< QueryChange)
                      , HP.placeholder "Filter query"
                      , singleClass "form-control"
                      ]
                  , HH.div [ classList [ "input-group-append" ] ]
                      [ HH.button
                          [ classList [ "btn" ]
                          --, HE.onClick (const (Just QuerySubmit))
                          ]
                          [ HH.text "Apply" ]
                      ]
                  ]
              ]
          ]
      , HH.div [ singleClass "collapse", HP.id_ "columnChooser" ]
          [ HH.div [ singleClass "row" ]
              [ HH.div [ singleClass "col" ]
                  [ disableAll
                  , HH.div [ singleClass "list-group " ]
                      (makeRow <$> allAnalysisColumns)
                  ]
              ]
          ]
      ]

render :: forall cs. State -> H.ComponentHTML Action cs AppMonad
render state =
  let
    headers :: Array (Tuple AnalysisColumn String)
    headers = (\x -> Tuple x (analysisColumnToHumanString x)) <$> toUnfoldable state.selectedColumns

    makeHeader (Tuple col title) =
      let
        updatedSortInput doInvertOrder = createUpdatedSortInput doInvertOrder col state.sorting

        maybeOrderIcon = if state.sorting.sortColumn == col then [ orderingToIcon state.sorting.sortOrder, HH.text " " ] else []

        cellContent = if analysisColumnIsSortable col then maybeOrderIcon <> [ HH.a [ HP.href (createLink (Analysis (updatedSortInput false))), HE.onClick \_ -> Just (Resort (updatedSortInput true)) ] [ HH.text title ] ] else [ HH.text title ]
      in
        HH.th [ singleClass "text-nowrap" ] cellContent

    makeRow :: forall w. AnalysisRow -> HH.HTML w Action
    makeRow row = HH.tr_ (((\x -> HH.td_ [ x ]) <<< extractFromRow row) <$> toUnfoldable state.selectedColumns)
  in
    fluidContainer
      [ HH.h2_ [ icon { name: "table", size: Nothing, spin: false }, HH.text " Analysis Results" ]
      , HH.hr_
      , renderColumnChooser state
      , maybe (HH.text "") (makeAlert AlertDanger) state.errorMessage
      , table
          "analysis-table"
          [ TableStriped, TableSmall ]
          (makeHeader <$> headers)
          (makeRow <$> state.rows)
      ]
