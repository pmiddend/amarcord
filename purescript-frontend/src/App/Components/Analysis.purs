module App.Components.Analysis where

import App.API (AnalysisResponse, AnalysisRow, retrieveAnalysis)
import App.AppMonad (AppMonad)
import App.Bootstrap (TableFlag(..), fluidContainer, plainH1_, table)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.HalogenUtils (singleClass)
import Control.Applicative (pure, (<*>))
import Control.Bind (bind)
import Data.Eq (class Eq)
import Data.Functor ((<$>))
import Data.Maybe (Maybe(..), fromMaybe, maybe)
import Data.Ord (class Ord)
import Data.Semigroup ((<>))
import Data.Show (show)
import Data.Tuple (Tuple(..))
import Data.Unit (Unit, unit)
import Halogen as H
import Halogen.HTML as HH
import Network.RemoteData (RemoteData, fromEither)

type AnalysisInput
  = Unit

type DewarEdit
  = { editDewarPosition :: Int
    , editPuckId :: String
    }

type State
  = { rows :: Array AnalysisRow
    }

data AnalysisData
  = AnalysisData AnalysisResponse

derive instance eqAssociatedTable :: Eq AnalysisData

derive instance ordAssociatedTable :: Ord AnalysisData

data Action
  = Initialize

initialState :: ChildInput AnalysisInput AnalysisData -> State
initialState { input: _, remoteData: AnalysisData analysisResponse } =
  { rows: analysisResponse.analysis
  }

component :: forall query output. H.Component HH.HTML query AnalysisInput output AppMonad
component = parentComponent fetchData childComponent

childComponent :: forall q. H.Component HH.HTML q (ChildInput AnalysisInput AnalysisData) ParentError AppMonad
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

fetchData :: AnalysisInput -> AppMonad (RemoteData String AnalysisData)
fetchData _ = do
  analysis <- retrieveAnalysis
  pure (fromEither (AnalysisData <$> analysis))

render :: forall cs. State -> H.ComponentHTML Action cs AppMonad
render state =
  let
    headers =
      [ "Crystal ID"
      , "Analysis time"
      , "Puck"
      , "Run"
      , "Red. ID"
      , "Res. CC"
      , "Res. I/σ(I)"
      , "a"
      , "b"
      , "c"
      , "α"
      , "β"
      , "γ"
      , "Comment"
      ]

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
      [ plainH1_ "Analysis Results"
      , HH.hr_
      , table
          [ TableStriped, TableSmall ]
          ((\x -> HH.th [ singleClass "text-nowrap" ] [ HH.text x ]) <$> headers)
          (makeRow <$> state.rows)
      ]
