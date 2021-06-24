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
import Data.Function (const, (<<<))
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
  | QuerySubmit
  | ToggleColumn AnalysisColumn
  | QueryChange String

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

refresh :: forall slots. AnalysisRouteInput -> H.HalogenM State Action slots ParentError AppMonad Unit
refresh routeInput = do
  result <- H.lift (fetchData routeInput)
  case result of
    Success (AnalysisData { analysis, sqlError: Just sqlError }) ->
      H.modify_ \state ->
        state
          { errorMessage = Just ("SQL error: " <> sqlError)
          }
    Success (AnalysisData { analysis, sqlError: Nothing }) ->
      H.modify_ \state ->
        state
          { errorMessage = Nothing
          , rows = analysis
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
    refresh state.sorting
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
  Resort routeInput -> refresh routeInput

fetchData :: AnalysisRouteInput -> AppMonad (RemoteData String AnalysisData)
fetchData { filterQuery, sortColumn, sortOrder } = do
  analysis <- retrieveAnalysis filterQuery sortColumn sortOrder
  pure (fromEither (AnalysisData <$> analysis))

createUpdatedSortInput :: Boolean -> AnalysisColumn -> AnalysisRouteInput -> AnalysisRouteInput
createUpdatedSortInput doInvertOrder newColumn { sortColumn, sortOrder, filterQuery } =
  if newColumn == sortColumn then
    { sortOrder: if doInvertOrder then invertOrder sortOrder else sortOrder, sortColumn: newColumn, filterQuery }
  else
    { sortColumn: newColumn, sortOrder: Ascending, filterQuery }

analysisColumnIsSortable :: AnalysisColumn -> Boolean
analysisColumnIsSortable CrystalID = true

analysisColumnIsSortable AnalysisTime = true

analysisColumnIsSortable DataReductionID = true

analysisColumnIsSortable ResolutionCC = true

analysisColumnIsSortable BeamIntensity = true

analysisColumnIsSortable Pinhole = true

analysisColumnIsSortable Frames = true

analysisColumnIsSortable AngleStep = true

analysisColumnIsSortable ExposureTime = true

analysisColumnIsSortable XrayWavelength = true

analysisColumnIsSortable XrayEnergy = true

analysisColumnIsSortable DetectorDistance = true

analysisColumnIsSortable ApertureRadius = true

analysisColumnIsSortable RingCurrent = true

analysisColumnIsSortable ApertureHorizontal = true

analysisColumnIsSortable Isigi = true

analysisColumnIsSortable Rmeas = true

analysisColumnIsSortable Rfactor = true

analysisColumnIsSortable Cchalf = true

analysisColumnIsSortable Wilsonb = true

analysisColumnIsSortable ApertureVertical = true

analysisColumnIsSortable ResolutionIsigI = true

analysisColumnIsSortable _ = false

analysisColumnToHumanString :: AnalysisColumn -> String
analysisColumnToHumanString CrystalID = "Crystal ID"

analysisColumnToHumanString Pinhole = "Pinhole"

analysisColumnToHumanString Frames = "Frames"

analysisColumnToHumanString AngleStep = "Angle step"

analysisColumnToHumanString ExposureTime = "Exposure Time"

analysisColumnToHumanString XrayEnergy = "Energy"

analysisColumnToHumanString XrayWavelength = "Wavelength"

analysisColumnToHumanString DetectorDistance = "Detector Distance"

analysisColumnToHumanString ApertureRadius = "Aperture Radius"

analysisColumnToHumanString FilterTransmission = "Filter transmission"

analysisColumnToHumanString RingCurrent = "Ring current"

analysisColumnToHumanString ApertureHorizontal = "Aperture horizontal"

analysisColumnToHumanString ApertureVertical = "Aperture vertical"

analysisColumnToHumanString RunID = "Run"

analysisColumnToHumanString BeamIntensity = "Beam Intensity"

analysisColumnToHumanString Comment = "Comment"

analysisColumnToHumanString AnalysisTime = "Analysis time"

analysisColumnToHumanString DataReductionID = "Red. ID"

analysisColumnToHumanString ResolutionCC = "Res. CC"

analysisColumnToHumanString ResolutionIsigI = "Res. I/Σ(I)"

analysisColumnToHumanString Isigi = "I/Σ(I)"

analysisColumnToHumanString Rmeas = "Rmeas"

analysisColumnToHumanString Cchalf = "CC/2"

analysisColumnToHumanString Rfactor = "R factor"

analysisColumnToHumanString Wilsonb = "Wilson B"

analysisColumnToHumanString CellA = "a"

analysisColumnToHumanString CellB = "b"

analysisColumnToHumanString CellC = "c"

analysisColumnToHumanString CellAlpha = "α"

analysisColumnToHumanString CellBeta = "β"

analysisColumnToHumanString CellGamma = "γ"

extractFromRow :: forall w i. AnalysisRow -> AnalysisColumn -> HH.HTML w i
extractFromRow row CrystalID = HH.text row.crystalId

extractFromRow row RunID = HH.text (maybe "" show (_.runId <$> row.diffraction))

extractFromRow row BeamIntensity = HH.text (fromMaybe "" (row.diffraction >>= _.beamIntensity))

extractFromRow row Pinhole = HH.text (fromMaybe "" (row.diffraction >>= _.pinhole))

extractFromRow row Frames = HH.text (fromMaybe "" (row.diffraction >>= _.frames >>= (pure <<< show)))

extractFromRow row AngleStep = HH.text (fromMaybe "" (row.diffraction >>= _.angleStep >>= (pure <<< show)))

extractFromRow row ExposureTime = HH.text (fromMaybe "" (row.diffraction >>= _.exposureTime >>= (pure <<< show)))

extractFromRow row XrayEnergy = HH.text (fromMaybe "" (row.diffraction >>= _.xrayEnergy >>= (pure <<< show)))

extractFromRow row XrayWavelength = HH.text (fromMaybe "" (row.diffraction >>= _.xrayWavelength >>= (pure <<< show)))

extractFromRow row DetectorDistance = HH.text (fromMaybe "" (row.diffraction >>= _.detectorDistance >>= (pure <<< show)))

extractFromRow row ApertureRadius = HH.text (fromMaybe "" (row.diffraction >>= _.apertureRadius >>= (pure <<< show)))

extractFromRow row FilterTransmission = HH.text (fromMaybe "" (row.diffraction >>= _.filterTransmission >>= (pure <<< show)))

extractFromRow row RingCurrent = HH.text (fromMaybe "" (row.diffraction >>= _.ringCurrent >>= (pure <<< show)))

extractFromRow row ApertureHorizontal = HH.text (fromMaybe "" (row.diffraction >>= _.apertureHorizontal >>= (pure <<< show)))

extractFromRow row ApertureVertical = HH.text (fromMaybe "" (row.diffraction >>= _.apertureVertical >>= (pure <<< show)))

extractFromRow row Comment = HH.text (fromMaybe "" (_.comment <$> row.diffraction))

extractFromRow row AnalysisTime = HH.text (fromMaybe "" (_.analysisTime <$> row.dataReduction))

extractFromRow row DataReductionID = HH.text (maybe "" show (_.dataReductionId <$> row.dataReduction))

extractFromRow row ResolutionCC = HH.text (fromMaybe "" (row.dataReduction >>= _.resolutionCc >>= (pure <<< show)))

extractFromRow row ResolutionIsigI = HH.text (fromMaybe "" (row.dataReduction >>= _.resolutionIsigma >>= (pure <<< show)))

extractFromRow row Isigi = HH.text (fromMaybe "" (row.dataReduction >>= _.isigi >>= (pure <<< show)))

extractFromRow row Rmeas = HH.text (fromMaybe "" (row.dataReduction >>= _.rmeas >>= (pure <<< show)))

extractFromRow row Cchalf = HH.text (fromMaybe "" (row.dataReduction >>= _.cchalf >>= (pure <<< show)))

extractFromRow row Rfactor = HH.text (fromMaybe "" (row.dataReduction >>= _.rfactor >>= (pure <<< show)))

extractFromRow row Wilsonb = HH.text (fromMaybe "" (row.dataReduction >>= _.wilsonb >>= (pure <<< show)))

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
                      , HE.onValueChange (Just <<< QueryChange)
                      , HP.placeholder "Filter query"
                      , singleClass "form-control"
                      , HP.value state.sorting.filterQuery
                      ]
                  , HH.button
                      [ classList [ "btn", "btn-secondary" ]
                      , HE.onClick (const (Just QuerySubmit))
                      ]
                      [ HH.text "Apply" ]
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
