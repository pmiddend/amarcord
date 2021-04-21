module App.Components.Graphs where

import App.API (AttributiResponse, OverviewResponse, retrieveAttributi, retrieveOverview)
import App.AppMonad (AppMonad, log)
import App.Attributo (Attributo, qualifiedAttributoName)
import App.Components.Echarts (echartsComponent)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.HalogenUtils (classList, singleClass)
import App.JSONSchemaType (JSONSchemaType(..))
import App.Logging (LogLevel(..))
import App.Overview (OverviewRow, OverviewCell, findCellInRow)
import App.Utils (fanoutApplicative)
import Control.Applicative (pure, (<*>))
import Control.Bind ((>>=))
import Data.Argonaut (toNumber)
import Data.Array (filter, mapMaybe)
import Data.Eq ((==))
import Data.Foldable (find)
import Data.Functor ((<$>))
import Data.Maybe (Maybe(..))
import Data.Semigroup ((<>))
import Data.Symbol (SProxy(..))
import Data.Tuple (Tuple(..))
import Data.Unit (Unit, unit)
import Data.Void (Void, absurd)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData, fromEither)
import Prelude (discard, bind)

type State
  = { xAxis :: Maybe Attributo
    , yAxis :: Maybe Attributo
    , overviewRows :: Array OverviewRow
    , attributi :: Array Attributo
    }

type OpaqueSlot slot
  = forall query. H.Slot query Void slot

type ChildSlots
  = ( graphSlot :: OpaqueSlot Unit )

extractNumberFromCell :: OverviewCell -> Maybe Number
extractNumberFromCell cell = toNumber cell.value

extractPairFromRow :: Attributo -> Attributo -> OverviewRow -> Maybe (Array Number)
extractPairFromRow xaxis yaxis overviewRow = do
  xcell <- findCellInRow (qualifiedAttributoName xaxis) overviewRow
  ycell <- findCellInRow (qualifiedAttributoName yaxis) overviewRow
  x <- toNumber xcell.value
  y <- toNumber ycell.value
  pure [ x, y ]

render :: State -> H.ComponentHTML Action ChildSlots AppMonad
render state =
  let
    isPlottable attributo = case attributo.typeSchema of
      JSONNumber _ -> true
      JSONInteger -> true
      _ -> false

    plottableAttributi = filter isPlottable state.attributi

    makeAttributoOption :: forall w. Attributo -> HH.HTML w Action
    makeAttributoOption a = HH.option [ HP.value a.name ] [ HH.text a.description ]

    generateXAxisChange attributoName = XAxisChange <$> find (\a -> a.name == attributoName) state.attributi

    generateYAxisChange attributoName = YAxisChange <$> find (\a -> a.name == attributoName) state.attributi

    mockData =
      { width: 600.0
      , height: 400.0
      , options:
          { title: { text: "My chart" }
          , xAxis: {}
          , yAxis: {}
          , series:
              [ { name: "MySeries"
                , "type": "line"
                , "data": [ [ 0.0, 5.0 ], [ 10.0, 20.0 ], [ 100.0, 36.0 ], [ 101.0, 10.0 ], [ 102.0, 10.0 ], [ 1000.0, 20.0 ] ]
                }
              ]
          }
      }

    chartData = case state.xAxis of
      Nothing -> mockData
      Just xaxis -> case state.yAxis of
        Nothing -> mockData
        Just yaxis ->
          let
            dataPoints :: Array (Array Number)
            dataPoints = mapMaybe (extractPairFromRow xaxis yaxis) state.overviewRows
          in
            { width: 600.0
            , height: 400.0
            , options:
                { title: { text: xaxis.description <> " vs. " <> yaxis.description }
                , xAxis: {}
                , yAxis: {}
                , series:
                    [ { name: xaxis.description <> " vs. " <> yaxis.description
                      , "type": "line"
                      , "data": dataPoints
                      }
                    ]
                }
            }
  in
    HH.div_
      [ HH.h1_ [ HH.text "Graph Dashboard" ]
      , HH.p_ [ HH.text "Select the axes to plot!" ]
      , HH.form_
          [ HH.div [ singleClass "mb-3" ]
              [ HH.label [ HP.for "x-axis", singleClass "form-label" ] [ HH.text "X Axis" ]
              , HH.select
                  [ classList [ "form-select", "form-control" ]
                  , HP.id_ "x-axis"
                  , HE.onValueChange generateXAxisChange
                  ]
                  (makeAttributoOption <$> plottableAttributi)
              ]
          , HH.div
              [ singleClass "mb-3" ]
              [ HH.label [ HP.for "y-axis", singleClass "form-label" ] [ HH.text "Y Axis" ]
              , HH.select
                  [ classList [ "form-select", "form-control" ]
                  , HP.id_ "y-axis"
                  , HE.onValueChange generateYAxisChange
                  ]
                  (makeAttributoOption <$> plottableAttributi)
              ]
          ]
      , HH.slot
          (SProxy :: _ "graphSlot")
          unit
          (echartsComponent chartData)
          chartData
          absurd
      ]

childComponent :: forall q. H.Component HH.HTML q (ChildInput RoutingInput ParentOutput) ParentError AppMonad
childComponent =
  H.mkComponent
    { initialState
    , render
    , eval: H.mkEval H.defaultEval { handleAction = handleAction }
    }

data Action
  = XAxisChange Attributo
  | YAxisChange Attributo

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  XAxisChange a -> do
    H.lift (log Info "x axis changed!")
    H.modify_ \state -> state { xAxis = Just a }
  YAxisChange a -> do
    H.lift (log Info "y axis changed!")
    H.modify_ \state -> state { yAxis = Just a }

initialState :: ChildInput RoutingInput (Tuple OverviewResponse AttributiResponse) -> State
initialState { input: _, remoteData: Tuple overviewResponse attributiResponse } =
  { overviewRows: overviewResponse.overviewRows
  , attributi: attributiResponse.attributi
  , xAxis: Nothing
  , yAxis: Nothing
  }

type RoutingInput
  = {}

type ParentOutput
  = Tuple OverviewResponse AttributiResponse

-- Fetch runs and attributi
fetchData :: RoutingInput -> AppMonad (RemoteData String ParentOutput)
fetchData _ = fromEither <$> (fanoutApplicative <$> retrieveOverview <*> retrieveAttributi)

component ::
  forall query output.
  H.Component HH.HTML query
    RoutingInput
    output
    AppMonad
component = parentComponent fetchData childComponent
