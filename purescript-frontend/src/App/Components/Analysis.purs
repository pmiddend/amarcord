module App.Components.Analysis where

import App.API (AnalysisResponse, AnalysisRow, retrieveAnalysis)
import App.AppMonad (AppMonad)
import App.Bootstrap (TableFlag(..), fluidContainer, table)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.Halogen.FontAwesome (icon)
import App.HalogenUtils (AlertType(..), classList, faIcon, makeAlert, singleClass)
import App.Route (AnalysisRouteInput, Route(..), createLink)
import App.SortOrder (SortOrder(..), invertOrder)
import Control.Applicative (pure)
import Control.Bind (bind)
import Data.Argonaut (Json, caseJson)
import Data.Array (find)
import Data.Eq (class Eq, (==))
import Data.Function (const, identity, (<<<))
import Data.Functor ((<$>))
import Data.Maybe (Maybe(..), fromMaybe, maybe)
import Data.Ord (class Ord)
import Data.Semigroup ((<>))
import Data.Set (Set, delete, empty, fromFoldable, insert, member, toUnfoldable)
import Data.Show (show)
import Data.String (Pattern(..), Replacement(..), replace)
import Data.Tuple (Tuple(..), snd)
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
    , columns :: Array String
    , sorting :: AnalysisRouteInput
    , errorMessage :: Maybe String
    , selectedColumns :: Set String
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

-- Convert a sort order to an icon
orderingToIcon :: forall w i. SortOrder -> HH.HTML w i
orderingToIcon Ascending = faIcon "sort-up"

orderingToIcon Descending = faIcon "sort-down"

initialState :: ChildInput AnalysisRouteInput AnalysisData -> State
initialState { input: sorting, remoteData: AnalysisData analysisResponse } =
  { rows: analysisResponse.analysis
  , columns: analysisResponse.analysisColumns
  , sorting
  , errorMessage: Nothing
  , selectedColumns: fromFoldable [ "crystal_id", "diff_run_id" ]
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
      emptyColumns :: Set String
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

createUpdatedSortInput :: Boolean -> String -> AnalysisRouteInput -> AnalysisRouteInput
createUpdatedSortInput doInvertOrder newColumn { sortColumn, sortOrder, filterQuery } =
  if newColumn == sortColumn then
    { sortOrder: if doInvertOrder then invertOrder sortOrder else sortOrder, sortColumn: newColumn, filterQuery }
  else
    { sortColumn: newColumn, sortOrder: Ascending, filterQuery }

showCellContent :: Json -> String
showCellContent = caseJson (const "") show show identity (const "array") (const "object")

extractFromRow row col = HH.text (fromMaybe "" ((showCellContent <<< snd) <$> find (\(Tuple col' v) -> col' == col) row))

postprocessColumnName :: String -> String
postprocessColumnName = replace (Pattern "diff_") (Replacement "Diffractions.") <<< replace (Pattern "dr_") (Replacement "Data_Reduction.")

renderColumnChooser :: forall w. State -> HH.HTML w Action
renderColumnChooser state =
  let
    makeRow :: String -> HH.HTML w Action
    makeRow a =
      HH.button
        [ HP.type_ HP.ButtonButton
        , classList ([ "list-group-item", "list-group-flush", "list-group-item-action" ] <> (if a `member` state.selectedColumns then [ "active" ] else []))
        , HE.onClick \_ -> Just (ToggleColumn a)
        ]
        [ HH.text (postprocessColumnName a) ]

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
      , HH.div [ singleClass "collapse", HP.id_ "helpDisplayer" ]
          [ HH.h3_ [ HH.text "Example queries" ]
          , HH.ul_
              [ HH.li_ [ HH.text "Column greater than: ", HH.pre_ [ HH.text "Diffractions.angle_step > 0.1" ] ]
              , HH.li_ [ HH.text "Column equal to: ", HH.pre_ [ HH.text "Diffractions.beamline = \"p11\"" ] ]
              , HH.li_ [ HH.text "Text contains: ", HH.pre_ [ HH.text "Crystals.crystal_id LIKE '%cryst%'" ] ]
              , HH.li_ [ HH.text "Combining expressions with AND: ", HH.pre_ [ HH.text "Diffractions.beamline = \"p11\" AND Diffractions.angle_step > 0.1" ] ]
              ]
          , HH.h3_ [ HH.text "Troubleshooting" ]
          , HH.h4_ [ HH.text "Searches involving the crystal ID" ]
          , HH.p_ [ HH.text "When you want to reference the crystal ID, you have to use ", HH.span [ singleClass "font-monospace" ] [ HH.text "Crystals.crystal_id" ], HH.text " as the column name." ]
          ]
      , HH.div [ singleClass "collapse", HP.id_ "columnChooser" ]
          [ HH.div [ singleClass "row" ]
              [ HH.div [ singleClass "col" ]
                  [ disableAll
                  , HH.div [ singleClass "list-group " ]
                      (makeRow <$> state.columns)
                  ]
              ]
          ]
      ]

analysisColumnIsSortable = const true

render :: forall cs. State -> H.ComponentHTML Action cs AppMonad
render state =
  let
    headers = (\x -> Tuple x (postprocessColumnName x)) <$> toUnfoldable state.selectedColumns

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
      , HH.hr_
      , maybe (HH.text "") (makeAlert AlertDanger) state.errorMessage
      , table
          "analysis-table"
          [ TableStriped, TableSmall ]
          (makeHeader <$> headers)
          (makeRow <$> state.rows)
      ]
