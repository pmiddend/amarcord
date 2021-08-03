module App.Components.JobList where

import App.API (JobsResponse, Job, retrieveJobs)
import App.AppMonad (AppMonad)
import App.Bootstrap (TableFlag(..), table)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.HalogenUtils (classList, singleClass)
import App.Timer (timerEventSource)
import Control.Applicative (pure)
import Control.Bind (bind, discard)
import Data.Argonaut (stringify)
import Data.Array (singleton)
import Data.Function ((<<<), (>>>))
import Data.Functor ((<$>))
import Data.Maybe (Maybe(..), fromMaybe, maybe)
import Data.Semigroup ((<>))
import Data.Show (show)
import Data.Unit (Unit, unit)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData(..), fromEither)

data Action
  = Initialize
  | Refresh

type JobListInput
  = {}

type State
  = { jobs :: Array Job
    }

fetchData :: JobListInput -> AppMonad (RemoteData String JobsResponse)
fetchData _ = fromEither <$> retrieveJobs

initialState :: ChildInput JobListInput JobsResponse -> State
initialState { input: _, remoteData: { jobs } } = { jobs }

render :: forall cs action. State -> H.ComponentHTML action cs AppMonad
render state =
  let
    makeExpander :: forall w i. String -> Int -> HH.HTML w i -> HH.HTML w i
    makeExpander suffix jid content =
      HH.div_
        [ HH.button
            [ HP.type_ HP.ButtonButton
            , classList [ "btn", "btn-dark", "btn-sm" ]
            , HP.attr (HH.AttrName "data-bs-toggle") "collapse"
            , HP.attr (HH.AttrName "data-bs-target") ("#j-" <> show jid <> "-" <> suffix)
            ]
            [ HH.text "Expand..." ]
        , HH.div
            [ singleClass "collapse", HP.id_ ("j-" <> show jid <> "-" <> suffix) ]
            [ HH.span [ singleClass "font-monospace" ] [ content ] ]
        ]

    makeFailureReason = makeExpander "fr"

    makeMetadata jid x = makeExpander "md" jid (HH.text (stringify x))

    makeOutputDirectory jid od = makeExpander "od" jid (HH.span [ classList [ "d-inline-block", "font-monospace", "text-truncate" ] ] [ HH.text od ])

    makeWorkingOn job = case job.diffraction of
      Nothing -> case job.reduction of
        Nothing -> HH.text "unknown"
        Just reduction ->
          HH.div_
            [ HH.text (reduction.crystalId <> "/" <> show reduction.runId)
            , HH.br_
            , HH.text ("Reduction " <> show reduction.dataReductionId)
            ]
      Just diffraction -> HH.text (diffraction.crystalId <> "/" <> show diffraction.runId)

    makeRow job =
      HH.tr_
        ( (HH.td_ <<< singleton)
            <$> [ makeWorkingOn job
              , HH.text (show job.jobId)
              , HH.text job.queued
              , HH.text (fromMaybe "" job.started)
              , HH.text (fromMaybe "" job.stopped)
              , HH.text job.status
              , maybe (HH.text "") (makeOutputDirectory job.jobId) job.outputDir
              , maybe (HH.text "") (makeFailureReason job.jobId <<< HH.text) job.failureReason
              , HH.text job.tool
              , HH.text (stringify job.toolInputs)
              , maybe (HH.text "") (makeMetadata job.jobId) job.metadata
              ]
        )
  in
    table
      "job-table"
      [ TableStriped ]
      ( (HH.text >>> singleton >>> HH.th_)
          <$> [ "Working on"
            , "Job ID"
            , "Queued"
            , "Started"
            , "Stopped"
            , "Status"
            , "Output Directory"
            , "Failure Reason"
            , "Tool"
            , "Tool Inputs"
            , "Job Metadata"
            ]
      )
      (makeRow <$> state.jobs)

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  Initialize -> do
    _ <- H.subscribe (timerEventSource Refresh)
    pure unit
  Refresh -> do
    remoteData <- H.lift (fetchData {})
    case remoteData of
      Success { jobs } -> H.modify_ \state -> state { jobs = jobs }
      _ -> pure unit
    _ <- H.subscribe (timerEventSource Refresh)
    pure unit

childComponent :: forall q. H.Component HH.HTML q (ChildInput JobListInput JobsResponse) ParentError AppMonad
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

component :: forall query output. H.Component HH.HTML query JobListInput output AppMonad
component = parentComponent fetchData childComponent
