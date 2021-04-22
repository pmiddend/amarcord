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
import Data.Argonaut (toNumber)
import Data.Array (filter, mapMaybe)
import Data.Eq (class Eq, (==))
import Data.Foldable (find)
import Data.Function (const, (<<<))
import Data.Functor ((<$>))
import Data.Maybe (Maybe(..), maybe)
import Data.Semigroup ((<>))
import Data.Show (class Show, show)
import Data.Symbol (SProxy(..))
import Data.Tuple (Tuple(..))
import Data.Unit (Unit, unit)
import Data.Void (Void, absurd)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties (InputType(..))
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData, fromEither)
import Prelude (discard, bind)

type State
  = { xAxis :: Maybe Attributo
    , yAxis :: Maybe Attributo
    , overviewRows :: Array OverviewRow
    , attributi :: Array Attributo
    , plotType :: PlotType
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

    chartData = do
      xaxis <- state.xAxis
      yaxis <- state.yAxis
      let
        dataPoints :: Array (Array Number)
        dataPoints = mapMaybe (extractPairFromRow xaxis yaxis) state.overviewRows
      pure
        { width: 600.0
        , height: 400.0
        , options:
            { title: { text: xaxis.description <> " vs. " <> yaxis.description }
            , xAxis: {}
            , yAxis: {}
            , series:
                [ { name: xaxis.description <> " vs. " <> yaxis.description
                  , "type": show state.plotType
                  , "data": dataPoints
                  }
                ]
            }
        }

    disabledAttribute axis =
      [ HH.option
          [ HP.value "", HP.disabled true, HP.selected true ]
          [ HH.text ("Select " <> axis <> " axis") ]
      ]
  in
    HH.div_
      [ HH.h1_ [ HH.text "Graph Dashboard" ]
      , HH.form_
          [ HH.div [ singleClass "mb-3" ]
              [ HH.label [ HP.for "x-axis", singleClass "form-label" ] [ HH.text "X Axis" ]
              , HH.select
                  [ classList [ "form-select", "form-control" ]
                  , HP.id_ "x-axis"
                  , HE.onValueChange generateXAxisChange
                  ]
                  (disabledAttribute "X" <> (makeAttributoOption <$> plottableAttributi))
              ]
          , HH.div
              [ singleClass "mb-3" ]
              [ HH.label [ HP.for "y-axis", singleClass "form-label" ] [ HH.text "Y Axis" ]
              , HH.select
                  [ classList [ "form-select", "form-control" ]
                  , HP.id_ "y-axis"
                  , HE.onValueChange generateYAxisChange
                  ]
                  (disabledAttribute "Y" <> (makeAttributoOption <$> plottableAttributi))
              ]
          , HH.div
              [ singleClass "mb-3" ]
              [ HH.label [ HP.for "plot-type", singleClass "form-label" ] [ HH.text "Plot Type: " ]
              , HH.select
                  [ classList [ "form-select", "form-control" ]
                  , HP.id_ "plot-type"
                  , HE.onValueChange (\x -> Just (if x == "line" then PlotTypeChange Line else PlotTypeChange Scatter))
                  ]
                  [ HH.option [ HP.value (show Line) ] [ HH.text "Line" ]
                  , HH.option [ HP.value (show Scatter) ] [ HH.text "Scatter" ]
                  ]
              ]
          -- This is the much nicer radio button, but it doesn't work for some reason
          -- , HH.div [ classList [ "btn-group", "btn-group-toggle" ], HP.attr (HH.AttrName "data-toggle") "buttons" ]
          --     [ HH.label [ classList [ "btn", "btn-secondary", "active" ] ]
          --         [ HH.input
          --             [ HP.type_ InputRadio
          --             , HP.name "plot-type"
          --             , HP.id_ "line-plot"
          --             , HP.checked (state.plotType == Line)
          --             , HP.value "line"
          --             , HE.onChange (const (Just (PlotTypeChange Line)))
          --             --, HE.onValueInput (const (Just (PlotTypeChange Line)))
          --             ]
          --         , HH.text "Line plot"
          --         ]
          --     , HH.label [ classList [ "btn", "btn-secondary" ] ]
          --         [ HH.input
          --             [ HP.type_ InputRadio
          --             , HP.name "plot-type"
          --             , HP.id_ "scatter-plot"
          --             , HP.checked (state.plotType == Scatter)
          --             , HP.value "scatter"
          --             , HE.onChange (const (Just (PlotTypeChange Scatter)))
          --             --, HE.onValueInput (const (Just (PlotTypeChange Scatter)))
          --             ]
          --         , HH.text "Scatter plot"
          --         ]
          --     ]
          -- ]
          ]
      , maybe (HH.text "Select both axes to plot!")
          ( \chartData' ->
              HH.slot
                (SProxy :: _ "graphSlot")
                unit
                (echartsComponent chartData')
                chartData'
                absurd
          )
          chartData
      ]

childComponent :: forall q. H.Component HH.HTML q (ChildInput RoutingInput ParentOutput) ParentError AppMonad
childComponent =
  H.mkComponent
    { initialState
    , render
    , eval: H.mkEval H.defaultEval { handleAction = handleAction }
    }

data PlotType
  = Line
  | Scatter

derive instance eqPlotType :: Eq PlotType

instance showPlotType :: Show PlotType where
  show Line = "line"
  show Scatter = "scatter"

data Action
  = XAxisChange Attributo
  | YAxisChange Attributo
  | PlotTypeChange PlotType

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  XAxisChange a -> H.modify_ \state -> state { xAxis = Just a }
  YAxisChange a -> H.modify_ \state -> state { yAxis = Just a }
  PlotTypeChange newType -> do
    H.lift (log Info "plot type changed")
    H.modify_ \state -> state { plotType = newType }

initialState :: ChildInput RoutingInput (Tuple OverviewResponse AttributiResponse) -> State
initialState { input: _, remoteData: Tuple overviewResponse attributiResponse } =
  { overviewRows: overviewResponse.overviewRows
  , attributi: attributiResponse.attributi
  , xAxis: Nothing
  , yAxis: Nothing
  , plotType: Line
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
