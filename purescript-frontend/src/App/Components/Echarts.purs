module App.Components.Echarts where

import Control.Monad (bind)
import Data.Function (($), (<<<))
import Data.Maybe (Maybe(..))
import Data.Traversable (for_)
import Data.Unit (Unit)
import Effect (Effect)
import Effect.Class (class MonadEffect, liftEffect)
import Effect.Console (log)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Properties as HP
import Prelude (discard)
import Web.DOM (Element)

-- type ChartOptions
--   = {
--     }
type ChartSeries
  = { name :: String
    , "type" :: String
    , "data" :: Array (Array Number)
    }

type ChartOptions
  = { title ::
        { text :: String
        }
    , xAxis :: {}
    , yAxis :: {}
    , series :: Array ChartSeries
    }

foreign import data ChartObject :: Type

foreign import initEcharts :: Element -> Effect ChartObject

foreign import finalizeEcharts :: ChartObject -> Effect Unit

foreign import setEchartsOptions :: ChartObject -> ChartOptions -> Effect Unit

type State
  = { input :: Input
    , chartObject :: Maybe ChartObject
    }

data Action
  = Initialize
  | Receive Input
  | Finalize

type Input
  = { width :: Number
    , height :: Number
    , options :: ChartOptions
    }

echartsComponent :: forall o m q. MonadEffect m => Input -> H.Component HH.HTML q Input o m
echartsComponent dim =
  H.mkComponent
    { initialState
    , render: renderH
    , eval:
        H.mkEval
          $ H.defaultEval
              { handleAction = handleAction
              , initialize = Just Initialize
              , receive = Just <<< Receive
              , finalize = Just Finalize
              }
    }

initialState :: Input -> State
initialState input = { input, chartObject: Nothing }

renderH :: forall m. State -> H.ComponentHTML Action () m
renderH _ =
  HH.div
    [ HP.ref scatterRef
    , HP.style "width: 600px; height: 400px;"
    ]
    []

handleAction ::
  forall m o.
  MonadEffect m =>
  Action ->
  H.HalogenM State Action () o m Unit
handleAction = case _ of
  Initialize -> do
    r <- H.getRef scatterRef
    for_ r \ref -> do
      input <- H.gets _.input
      chart <- liftEffect (initEcharts ref)
      liftEffect (setEchartsOptions chart input.options)
      H.modify_ \state -> state { chartObject = Just chart }
  Receive newInput -> do
--    H.liftEffect (log "RECEIVED NEW INPUT")
    chart <- H.gets _.chartObject
    -- case chart of
    --   Nothing -> H.liftEffect (log "no chart object")
    --   Just _ -> H.liftEffect (log "chart object")
    for_ chart \chart' -> liftEffect (setEchartsOptions chart' newInput.options)
    H.modify_ \state -> state { input = newInput }
  Finalize -> do
    chart <- H.gets _.chartObject
    for_ chart (liftEffect <<< finalizeEcharts)

scatterRef ∷ H.RefLabel
scatterRef = H.RefLabel "scatter-plot"
