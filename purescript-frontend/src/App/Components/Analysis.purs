module App.Components.Analysis where

import App.API (AnalysisColumn(..), AnalysisResponse, AnalysisRow, retrieveAnalysis)
import App.AppMonad (AppMonad)
import App.Bootstrap (TableFlag(..), fluidContainer, plainH1_, table)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.Halogen.FontAwesome (icon)
import App.HalogenUtils (AlertType(..), faIcon, makeAlert, singleClass)
import App.Route (AnalysisRouteInput, Route(..), createLink)
import App.SortOrder (SortOrder(..), invertOrder)
import Control.Applicative (pure, (<*>))
import Control.Bind (bind)
import Data.Eq (class Eq, (==))
import Data.Functor ((<$>))
import Data.Maybe (Maybe(..), fromMaybe, maybe)
import Data.Ord (class Ord)
import Data.Semigroup ((<>))
import Data.Show (show)
import Data.Tuple (Tuple(..))
import Data.Unit (Unit, unit)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
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
    }

data AnalysisData
  = AnalysisData AnalysisResponse

derive instance eqAssociatedTable :: Eq AnalysisData

derive instance ordAssociatedTable :: Ord AnalysisData

data Action
  = Initialize
  | Resort AnalysisRouteInput

-- Convert a sort order to an icon
orderingToIcon :: forall w i. SortOrder -> HH.HTML w i
orderingToIcon Ascending = faIcon "sort-up"

orderingToIcon Descending = faIcon "sort-down"

initialState :: ChildInput AnalysisRouteInput AnalysisData -> State
initialState { input: sorting, remoteData: AnalysisData analysisResponse } =
  { rows: analysisResponse.analysis
  , sorting
  , errorMessage: Nothing
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

render :: forall cs. State -> H.ComponentHTML Action cs AppMonad
render state =
  let
    headers = [ Tuple CrystalID "Crystal ID", Tuple AnalysisTime "Analysis time", Tuple Puck "Puck", Tuple RunID "Run", Tuple DataReductionID "Red. ID", Tuple ResolutionCC "Res. CC", Tuple ResolutionIsigI "Res. I/σ(I)", Tuple CellA "a", Tuple CellB "b", Tuple CellC "c", Tuple CellAlpha "α", Tuple CellBeta "β", Tuple CellGamma "γ", Tuple Comment "Comment" ]

    makeHeader (Tuple col title) =
      let
        updatedSortInput doInvertOrder = createUpdatedSortInput doInvertOrder col state.sorting

        maybeOrderIcon = if state.sorting.sortColumn == col then [ orderingToIcon state.sorting.sortOrder, HH.text " " ] else []

        cellContent = if analysisColumnIsSortable col then maybeOrderIcon <> [ HH.a [ HP.href (createLink (Analysis (updatedSortInput false))), HE.onClick \_ -> Just (Resort (updatedSortInput true)) ] [ HH.text title ] ] else [ HH.text title ]
      in
        HH.th [ singleClass "text-nowrap" ] cellContent

    makeRow :: forall w. AnalysisRow -> HH.HTML w Action
    makeRow { crystalId
    , analysisTime
    , puckId
    , puckPositionId
    , runId
    , comment
    , dataReductionId
    , resolutionCc
    , resolutionIsigma
    , a
    , b
    , c
    , alpha
    , beta
    , gamma
    } =
      HH.tr_
        [ HH.td_ [ HH.text crystalId ]
        , HH.td_ [ HH.text analysisTime ]
        , HH.td_ [ HH.text (fromMaybe "-" ((\pid ppid -> pid <> " " <> ppid) <$> puckId <*> (show <$> puckPositionId))) ]
        , HH.td_ [ HH.text (show runId) ]
        , HH.td_ [ HH.text (show dataReductionId) ]
        , HH.td_ [ HH.text (maybe "" show resolutionCc) ]
        , HH.td_ [ HH.text (maybe "" show resolutionIsigma) ]
        , HH.td_ [ HH.text (show a) ]
        , HH.td_ [ HH.text (show b) ]
        , HH.td_ [ HH.text (show c) ]
        , HH.td_ [ HH.text (show alpha) ]
        , HH.td_ [ HH.text (show beta) ]
        , HH.td_ [ HH.text (show gamma) ]
        , HH.td_ [ HH.text (fromMaybe "" comment) ]
        ]
  in
    fluidContainer
      [ HH.h2_ [ icon { name: "table", size: Nothing, spin: false }, HH.text " Analysis Results" ]
      , HH.hr_
      , maybe (HH.text "") (makeAlert AlertDanger) state.errorMessage
      , table
          [ TableStriped, TableSmall ]
          (makeHeader <$> headers)
          (makeRow <$> state.rows)
      ]
