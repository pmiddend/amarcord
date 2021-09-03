module App.Components.SingleJob where

import App.API (Job, JobResponse, retrieveJob)
import App.AppMonad (AppMonad)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.HalogenUtils (singleClass)
import App.Route (JobRouteInput)
import App.Timer (timerEventSource)
import Control.Applicative (pure)
import Control.Bind (bind, discard)
import Data.Argonaut (stringify)
import Data.Function (($))
import Data.Functor ((<$>))
import Data.Maybe (Maybe(..), fromMaybe)
import Data.Semigroup ((<>))
import Data.Show (show)
import Data.Unit (Unit, unit)
import Halogen as H
import Halogen.HTML as HH
import Network.RemoteData (RemoteData(..), fromEither)

data Action
  = Initialize
  | Refresh

type State
  = { job :: Job
    }

fetchData :: JobRouteInput -> AppMonad (RemoteData String JobResponse)
fetchData i = fromEither <$> retrieveJob i.jobId

initialState :: ChildInput JobRouteInput JobResponse -> State
initialState { input: _, remoteData: { job } } = { job }

render :: forall cs. State -> H.ComponentHTML Action cs AppMonad
render state =
  HH.div_
    [ HH.h1_ [ HH.text ("Job " <> show state.job.jobId) ]
    , HH.dl [ singleClass "row" ]
        [ HH.dt [ singleClass "col-sm-3" ] [ HH.text "Status" ]
        , HH.dd [ singleClass "col-sm-9" ] [ HH.text state.job.status ]
        , HH.dt [ singleClass "col-sm-3" ] [ HH.text "Output directory" ]
        , HH.dd [ singleClass "col-sm-9" ] [ HH.text (fromMaybe "" state.job.outputDir) ]
        , HH.dt [ singleClass "col-sm-3" ] [ HH.text "Metadata" ]
        , HH.dd [ singleClass "col-sm-9" ] [ HH.code_ [ HH.text $ fromMaybe "" (stringify <$> state.job.metadata) ] ]
        ]
    , HH.h2_ [ HH.text "Output" ]
    , HH.p [ singleClass "lead" ] [ HH.text "Note that this only shows the last kilobyte of log data, not everything. It might give an indication of what's going on or what went wrong though. The view refreshes automatically." ]
    , HH.h3_ [ HH.text "Standard output" ]
    , HH.pre [ singleClass "bg-light" ] [ HH.text $ fromMaybe "No standard output received (yet)." state.job.lastStdout ]
    , HH.h3_ [ HH.text "Standard error" ]
    , HH.pre [ singleClass "bg-light" ] [ HH.text $ fromMaybe "No standard error received (yet)." state.job.lastStderr ]
    ]

refresh :: forall slots. H.HalogenM State Action slots ParentError AppMonad Unit
refresh = do
  state <- H.get
  remoteData <- H.lift (fetchData { jobId: state.job.jobId })
  case remoteData of
    Success { job } -> H.put state { job = job }
    _ -> pure unit

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  Initialize -> do
    _ <- H.subscribe (timerEventSource Refresh)
    pure unit
  Refresh -> do
    refresh
    _ <- H.subscribe (timerEventSource Refresh)
    pure unit

childComponent :: forall q. H.Component HH.HTML q (ChildInput JobRouteInput JobResponse) ParentError AppMonad
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

component :: forall query output. H.Component HH.HTML query JobRouteInput output AppMonad
component = parentComponent fetchData childComponent
