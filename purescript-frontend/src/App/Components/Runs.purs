module App.Components.Runs where

import Prelude hiding (comparing)

import App.API (RunPropertiesResponse, RunsResponse, retrieveRunProperties, retrieveRuns)
import App.AppMonad (AppMonad)
import App.Autocomplete as Autocomplete
import App.Comment (Comment)
import App.Components.ParentComponent (ParentError, ChildInput, parentComponent)
import App.HalogenUtils (scope)
import App.Run (Run, runId, runLookup, runScalarProperty)
import App.RunProperty (RunProperty, rpDescription, rpIsSortable, rpName)
import App.RunScalar (RunScalar(..))
import App.RunValue (RunValue(..))
import App.SortOrder (SortOrder(..), invertOrder, comparing)
import DOM.HTML.Indexed.ScopeValue (ScopeValue(ScopeCol))
import Data.Array (sortBy, sortWith)
import Data.Either (Either)
import Data.Maybe (Maybe(..), maybe)
import Data.Number.Format (precision, toStringWith)
import Data.Tuple (Tuple(..))
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData, fromEither)

type State
  = { runs :: Array Run
    , runProperties :: Array RunProperty
    , selectedRunProperties :: Array RunProperty
    , runSortProperty :: String
    , runSortOrder :: SortOrder
    }

data Action
  = SortBy String

initialState :: ChildInput String (Tuple RunsResponse RunPropertiesResponse) -> State
initialState { input: x, remoteData: Tuple runsResponse runPropertiesResponse } =
  { runProperties: runPropertiesResponse.metadata
  , selectedRunProperties: runPropertiesResponse.metadata
  , runSortProperty: x
  , runs: sortBy (comparing Ascending (flip runScalarProperty x)) runsResponse.runs
  , runSortOrder: Ascending
  }

component :: forall output query. H.Component HH.HTML query String output AppMonad
component = parentComponent fetchInitialData childComponent

childComponent :: forall q. H.Component HH.HTML q (ChildInput String (Tuple RunsResponse RunPropertiesResponse)) ParentError AppMonad
childComponent =
  H.mkComponent
    { initialState
    , render
    , eval:
        H.mkEval
          H.defaultEval
            { handleAction = handleAction
            }
    }

combineEithers :: forall a b c. Either a b -> Either a c -> Either a (Tuple b c)
combineEithers x' y' = Tuple <$> x' <*> y'

fetchInitialData :: AppMonad (RemoteData String (Tuple RunsResponse RunPropertiesResponse))
fetchInitialData = fromEither <$> (combineEithers <$> retrieveRuns <*> retrieveRunProperties)

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  SortBy runProp ->
    H.modify_ \state ->
      let newSortOrder :: SortOrder
          newSortOrder = if state.runSortProperty == runProp then invertOrder state.runSortOrder else Ascending
      in state {
          runSortOrder = newSortOrder
        , runs = sortBy (comparing newSortOrder (flip runScalarProperty runProp)) state.runs
        , runSortProperty = runProp
        }

faIcon :: forall w i. String -> HH.HTML w i
faIcon name = HH.i [ HP.classes [ HH.ClassName "fa", HH.ClassName ("fa-" <> name) ] ] []

orderingToIcon :: forall w i. SortOrder -> HH.HTML w i
orderingToIcon Ascending = faIcon "sort-up"
orderingToIcon Descending = faIcon "sort-down"

render :: State -> H.ComponentHTML Action ( refinementComplete :: forall query. H.Slot query Autocomplete.Output Int ) AppMonad
render state =
  let
    makeHeader :: forall w. RunProperty -> HH.HTML w Action
    makeHeader t =
      let
        isSortProp = rpName t == state.runSortProperty
      in
        HH.th [ scope ScopeCol ]
          ( if rpIsSortable t then
              [ HH.a
                  [ HP.href ("#/runs?sort=" <> rpName t), HE.onClick \_ -> Just (SortBy (rpName t)) ]
                  ([ HH.text (rpDescription t) ] <> (if isSortProp then [ orderingToIcon state.runSortOrder ] else []))
              ]
            else
              [ HH.text (rpDescription t) ]
          )

    makeComment :: forall w. Comment -> HH.HTML w Action
    makeComment c = HH.li_ [ HH.text (c.author <> ": " <> c.text) ]

    makeProperty :: forall w. RunProperty -> Maybe RunValue -> HH.HTML w Action
    makeProperty rp value = case value of
      Nothing -> HH.td_ []
      Just (Comments cs) -> HH.td_ [ HH.ol_ (makeComment <$> cs) ]
      Just (Scalar (RunScalarNumber n)) -> HH.td_ [ HH.text (toStringWith (precision 2) n) ]
      _ -> HH.td_ [ HH.text (maybe "" show value)]

    makeRow run = HH.tr_ ((\rp -> makeProperty rp (runLookup run (rpName rp))) <$> state.selectedRunProperties)
  in
    HH.div [ HP.classes [ HH.ClassName "container-fluid" ] ]
      [ HH.h1_ [ HH.text "Runs" ]
      , HH.table
          [ HP.classes [ HH.ClassName "table" ] ]
          [ HH.thead_
              [ HH.tr_ (makeHeader <$> (state.runProperties))
              ]
          , HH.tbody_ (makeRow <$> state.runs)
          ]
      ]
