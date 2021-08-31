module App.Components.JobList where

import App.API (HumanDuration(..), Job, JobsResponse, deserializeHumanDuration, retrieveJobs, serializeHumanDuration)
import App.AppMonad (AppMonad)
import App.Bootstrap (TableFlag(..), table)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.HalogenUtils (classList, singleClass)
import App.Timer (timerEventSource)
import Control.Applicative (pure)
import Control.Bind (bind, discard)
import Data.Argonaut (stringify)
import Data.Argonaut.Core (Json, toObject, toString)
import Data.Array (singleton, (:))
import Data.Eq ((==))
import Data.FoldableWithIndex (foldrWithIndex)
import Data.Function ((<<<), (>>>))
import Data.Functor ((<$>))
import Data.Int (fromString)
import Data.Maybe (Maybe(..), fromMaybe, isNothing, maybe)
import Data.Monoid (mempty)
import Data.Semigroup ((<>))
import Data.Show (show)
import Data.Unit (Unit, unit)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties (InputType(..))
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData(..), fromEither)

data Action
  = Initialize
  | Refresh
  | LimitChanged Int
  | StatusFilterChanged (Maybe String)
  | DurationFilterChanged (Maybe HumanDuration)

type JobListInput
  = { limit :: Int, statusFilter :: Maybe String, durationFilter :: Maybe HumanDuration }

type State
  = { jobs :: Array Job
    , limit :: Int
    , statusFilter :: Maybe String
    , durationFilter :: Maybe HumanDuration
    }

fetchData :: JobListInput -> AppMonad (RemoteData String JobsResponse)
fetchData i = fromEither <$> retrieveJobs i.limit i.statusFilter i.durationFilter

initialState :: ChildInput JobListInput JobsResponse -> State
initialState { input: _, remoteData: { jobs } } = { jobs, limit: 20, statusFilter: Nothing, durationFilter: Nothing }

render :: forall cs. State -> H.ComponentHTML Action cs AppMonad
render state =
  HH.div_
    [ renderForm state
    , renderTable state
    ]

renderForm :: forall cs. State -> H.ComponentHTML Action cs AppMonad
renderForm state =
  let -- make a single <option> for a job status
    makeStatus :: forall w. String -> HH.HTML w Action
    makeStatus s = HH.option [ HP.value s, HP.selected (state.statusFilter == Just s) ] [ HH.text s ]

    humanReadableHumanDuration :: HumanDuration -> String
    humanReadableHumanDuration LastDay = "yesterday"

    humanReadableHumanDuration LastWeek = "last week"

    humanReadableHumanDuration LastMonth = "last month"

    makeHumanDuration :: forall w. HumanDuration -> HH.HTML w Action
    makeHumanDuration s = HH.option [ HP.value (serializeHumanDuration s), HP.selected (state.durationFilter == Just s) ] [ HH.text (humanReadableHumanDuration s) ]

    noStatusFilterOption = (HH.option [ HP.value "", HP.selected (isNothing state.statusFilter) ] [ HH.text "any status" ])

    noDurationFilterOption = (HH.option [ HP.value "", HP.selected (isNothing state.durationFilter) ] [ HH.text "any time" ])
  in
    HH.form_
      [ HH.div [ singleClass "mb-3" ]
          [ HH.label [ HP.for "job-list-limit", singleClass "form-label" ] [ HH.text "Display only this many jobs:" ]
          , HH.input
              [ HP.type_ InputNumber
              , HP.min 0.0
              , HP.id_ ("tool-limit")
              , singleClass "form-control"
              , HP.value (show state.limit)
              , HE.onValueInput (\x -> LimitChanged <$> fromString x)
              ]
          , HH.div [ singleClass "form-text" ] [ HH.text "Set to 0 for unlimited number of jobs (warning: the table might get really large)" ]
          ]
      , HH.div [ singleClass "mb-3" ]
          [ HH.label [ HP.for "job-list-status-filter", singleClass "form-label" ] [ HH.text "Job status:" ]
          , HH.select [ singleClass "form-select", HP.id_ "job-list-status-filter", HE.onValueChange (\x -> Just (StatusFilterChanged (if x == "" then Nothing else Just x))) ]
              (noStatusFilterOption : (makeStatus <$> [ "completed", "running", "queued", "failed" ]))
          ]
      , HH.div [ singleClass "mb-3" ]
          [ HH.label [ HP.for "job-list-duration-filter", singleClass "form-label" ] [ HH.text "Jobs started since:" ]
          , HH.select [ singleClass "form-select", HP.id_ "job-list-duration-filter", HE.onValueChange (\x -> if x == "" then Just (DurationFilterChanged Nothing) else (DurationFilterChanged <<< Just) <$> deserializeHumanDuration x) ]
              (noDurationFilterOption : (makeHumanDuration <$> [ LastDay, LastWeek, LastMonth ]))
          ]
      ]

renderTable :: forall cs action. State -> H.ComponentHTML action cs AppMonad
renderTable state =
  let
    makeExpander :: forall w i. String -> Int -> HH.HTML w i -> HH.HTML w i
    makeExpander suffix jid content =
      HH.div_
        [ HH.button
            [ HP.type_ HP.ButtonButton
            , classList [ "btn", "btn-dark", "amarcord-very-small-button" ]
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
            , HH.small [ singleClass "text-muted" ] [ HH.text ("Reduction " <> show reduction.dataReductionId) ]
            ]
      Just diffraction -> HH.text (diffraction.crystalId <> "/" <> show diffraction.runId)

    makeJobStatus job "completed" _ =
      HH.div_
        [ HH.span [ singleClass "badge rounded-pill bg-success" ] [ HH.text "success" ]
        , HH.br_
        , HH.small [ singleClass "text-sm text-muted" ] [ HH.text ("finished at " <> (fromMaybe "" job.stopped)) ]
        ]

    makeJobStatus job "failed" (Just reason) =
      HH.div_
        [ HH.span [ singleClass "badge rounded-pill bg-danger" ] [ HH.text "error" ]
        , HH.br_
        , HH.small [ singleClass "text-sm text-muted" ] [ HH.text ("stopped at " <> (fromMaybe "" job.stopped) <> ", reason: " <> reason) ]
        ]

    makeJobStatus job "running" _ =
      HH.div_
        [ HH.span [ singleClass "badge rounded-pill bg-info" ] [ HH.text "running" ]
        , HH.br_
        , HH.small [ singleClass "text-sm text-muted" ] [ HH.text ("since " <> (fromMaybe "" job.started)) ]
        ]

    makeJobStatus job _ _ =
      HH.div_
        [ HH.span [ singleClass "badge rounded-pill bg-secondary" ] [ HH.text "queued" ]
        , HH.br_
        , HH.small [ singleClass "text-sm text-muted" ] [ HH.text ("since " <> job.queued) ]
        ]

    htmlifyValue :: forall w i. Json -> HH.HTML w i
    htmlifyValue v = case toString v of
      Nothing -> HH.text (stringify v)
      Just "" -> HH.em_ [ HH.text "<empty>" ]
      Just s -> HH.text s

    makeInputs :: forall w i. Json -> HH.HTML w i
    makeInputs json = case toObject json of
      Nothing -> HH.text (stringify json)
      Just jsonObject -> HH.ul_ (foldrWithIndex (\key value prevList -> (HH.li_ [ HH.text (key <> ": "), htmlifyValue value ]) : prevList) mempty jsonObject)

    makeRow job =
      HH.tr_
        ( (HH.td_ <<< singleton)
            <$> [ HH.text (show job.jobId)
              , makeWorkingOn job
              , HH.text job.comment
              , makeJobStatus job job.status job.failureReason
              , maybe (HH.text "") (makeOutputDirectory job.jobId) job.outputDir
              , HH.text job.tool
              , makeInputs job.toolInputs
              , maybe (HH.text "") (makeMetadata job.jobId) job.metadata
              ]
        )
  in
    table
      "job-table"
      [ TableStriped ]
      ( (HH.text >>> singleton >>> HH.th [ singleClass "text-nowrap" ])
          <$> [ "Job ID"
            , "Working on"
            , "Comment"
            , "Status"
            , "Directory"
            , "Tool"
            , "Tool Inputs"
            , "Metadata"
            ]
      )
      (makeRow <$> state.jobs)

refresh :: forall slots. H.HalogenM State Action slots ParentError AppMonad Unit
refresh = do
  state <- H.get
  remoteData <- H.lift (fetchData { limit: state.limit, statusFilter: state.statusFilter, durationFilter: state.durationFilter })
  case remoteData of
    Success { jobs } -> H.put state { jobs = jobs }
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
  LimitChanged newLimit -> do
    H.modify_ \state -> state { limit = newLimit }
    refresh
  StatusFilterChanged newFilter -> do
    H.modify_ \state -> state { statusFilter = newFilter }
    refresh
  DurationFilterChanged newFilter -> do
    H.modify_ \state -> state { durationFilter = newFilter }
    refresh

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
