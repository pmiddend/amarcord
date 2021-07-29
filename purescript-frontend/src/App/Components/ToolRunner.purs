module App.Components.ToolRunner where

import App.API (Tool, ToolInputMap, ToolsResponse, retrieveTools, startJob)
import App.AppMonad (AppMonad)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.HalogenUtils (classList, makeRequestResult, singleClass)
import Control.Bind (bind)
import Data.Array (head, intercalate, length)
import Data.Boolean (otherwise)
import Data.BooleanAlgebra ((&&), (||))
import Data.Either (Either(..))
import Data.Eq ((==))
import Data.Foldable (any, find, foldMap, for_)
import Data.FoldableWithIndex (foldMapWithIndex)
import Data.Function (const)
import Data.Functor ((<$>))
import Data.Int (fromString)
import Data.Map as Map
import Data.Maybe (Maybe(..), fromMaybe, isJust, isNothing)
import Data.Monoid (mempty)
import Data.Ord ((<), (>))
import Data.Semigroup ((<>))
import Data.Show (show)
import Data.String (Pattern(..), contains)
import Data.Tuple (Tuple(..))
import Data.Unit (Unit)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties (InputType(..))
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData(..), fromEither)

data Action
  = ChangeTool Int
  | RunTool
  | ChangeInput String String
  | UpdateInput ToolRunnerInput
  | ChangeLimit Int

type ToolRunnerInput
  = { numberOfDiffractions :: Int, numberOfReductions :: Int, filterQuery :: String }

type State
  = { tools :: Array Tool
    , selectedToolId :: Maybe Int
    , toolInputs :: ToolInputMap
    , lastRequest :: RemoteData String String
    , createsRefinement :: Boolean
    , filterQuery :: String
    , numberOfDiffractions :: Int
    , numberOfReductions :: Int
    , limit :: Int
    }

fetchData :: ToolRunnerInput -> AppMonad (RemoteData String ToolsResponse)
fetchData _ = fromEither <$> retrieveTools

initialState :: ChildInput ToolRunnerInput ToolsResponse -> State
initialState { input: { numberOfDiffractions, numberOfReductions, filterQuery }, remoteData: { tools } } =
  { selectedToolId: Nothing
  , toolInputs: mempty
  , lastRequest: NotAsked
  , numberOfDiffractions
  , numberOfReductions
  , createsRefinement: false
  , filterQuery
  , tools
  , limit: 0
  }

render :: forall cs. State -> H.ComponentHTML Action cs AppMonad
render state =
  let
    makeOption tool =
      HH.option
        [ HP.value (show tool.toolId)
        , HP.selected (state.selectedToolId == (Just tool.toolId))
        ]
        [ HH.text tool.name ]

    disabledTool = [ HH.option [ HP.value "", HP.disabled true, HP.selected true ] [ HH.text "Select tool" ] ]

    inputs :: forall w. Array (HH.HTML w Action)
    inputs =
      foldMapWithIndex
        ( \inputName inputValue ->
            [ HH.div [ singleClass "mb-3" ]
                [ HH.label [ HP.for ("start-tool-input" <> inputName), singleClass "form-label" ] [ HH.text (inputName) ]
                , HH.input [ HP.type_ HP.InputText, HP.id_ ("start-tool-input" <> inputName), singleClass "form-control", HP.value inputValue, HE.onValueInput (\x -> Just (ChangeInput inputName x)) ]
                ]
            ]
        )
        state.toolInputs

    willBeRunOnNumber
      | state.createsRefinement = state.numberOfReductions
      | otherwise = state.numberOfDiffractions

    willBeRunOnType
      | state.createsRefinement = "reduction"
      | otherwise = "diffraction"

    buttonDisabled = willBeRunOnNumber == 0 || isNothing state.selectedToolId

    willBeRunOn = HH.strong_ [ HH.text (show willBeRunOnNumber <> " " <> willBeRunOnType <> (if willBeRunOnNumber > 1 then "s" else "")) ]
  in
    HH.form [ singleClass "mb-3" ]
      ( [ makeRequestResult state.lastRequest
        , HH.div
            [ singleClass "mb-3" ]
            [ HH.label [ HP.for "start-tool-tool-id", singleClass "form-label" ] [ HH.text "Tool" ]
            , HH.select
                [ classList [ "form-select" ]
                , HE.onValueChange (\x -> ChangeTool <$> fromString x)
                , HP.id_ "start-tool-tool-id"
                ]
                (disabledTool <> (makeOption <$> state.tools))
            ]
        ]
          <> inputs
          <> [ HH.div [ singleClass "mb-3" ]
                [ HH.label [ HP.for "tool-limit", singleClass "form-label" ] [ HH.text "Maximum number of jobs" ]
                , HH.input
                    [ HP.type_ InputNumber
                    , HP.min 0.0
                    , HP.id_ ("tool-limit")
                    , singleClass "form-control"
                    , HP.value (show state.limit)
                    , HE.onValueInput (\x -> ChangeLimit <$> fromString x)
                    ]
                , HH.div [ singleClass "form-text" ] [ HH.text "Set to 0 for unlimited number of jobs (here be dragons)" ]
                ]
            ]
          <> [ HH.div_
                [ HH.button
                    [ HP.type_ HP.ButtonButton
                    , classList [ "btn", "btn-primary" ]
                    , HP.disabled buttonDisabled
                    , HE.onClick (const (Just RunTool))
                    ]
                    [ HH.text "Run tool" ]
                , HH.div
                    [ singleClass "form-text" ]
                    ( if isJust state.selectedToolId && willBeRunOnNumber > 0 then
                        [ HH.text "The tool will be run on "
                        , willBeRunOn
                        , HH.text " unless maximum number of jobs is set."
                        ]
                      else
                        []
                    )
                ]
            ]
      )

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  ChangeLimit newLimit -> H.modify_ \state -> state { limit = newLimit }
  UpdateInput newInput ->
    H.modify_ \state ->
      state
        { numberOfDiffractions = newInput.numberOfDiffractions
        , filterQuery = newInput.filterQuery
        }
  ChangeTool newToolId -> do
    availableTools <- H.gets _.tools
    case find (\t -> t.toolId == newToolId) availableTools of
      Nothing ->
        H.modify_ \state ->
          state
            { selectedToolId = Nothing
            , toolInputs = Map.empty :: ToolInputMap
            }
      Just newTool ->
        H.modify_ \state ->
          state
            { selectedToolId = Just newTool.toolId
            , toolInputs = Map.fromFoldable (foldMap (\i -> if i."type" == "string" then [ Tuple i.name "" ] else []) newTool.inputs)
            , createsRefinement = any (\i -> contains (Pattern "reduction.") i."type") newTool.inputs
            }
  ChangeInput name newValue -> H.modify_ \state -> state { toolInputs = Map.insert name newValue state.toolInputs }
  RunTool -> do
    s <- H.get
    for_ s.selectedToolId \toolId -> do
      result <- H.lift (startJob toolId s.toolInputs s.filterQuery (if s.limit == 0 then Nothing else Just s.limit))
      case result of
        Right { jobIds } -> do
          let
            numberOfJobs = length jobIds

            jobString =
              if numberOfJobs == 1 then
                "job " <> fromMaybe "" (show <$> head jobIds)
              else if numberOfJobs < 10 then
                show numberOfJobs <> " jobs with ids: " <> intercalate ", " (show <$> jobIds)
              else
                show numberOfJobs <> " jobs"
          H.modify_ \state ->
            state
              { lastRequest = Success ("Started " <> jobString)
              }
        Left e -> H.modify_ \state -> state { lastRequest = Failure e }

childComponent :: forall q. H.Component HH.HTML q (ChildInput ToolRunnerInput ToolsResponse) ParentError AppMonad
childComponent =
  H.mkComponent
    { initialState
    , render
    , eval:
        H.mkEval
          H.defaultEval
            { handleAction = handleAction
            , receive = \{ input } -> Just (UpdateInput input)
            }
    }

component :: forall query output. H.Component HH.HTML query ToolRunnerInput output AppMonad
component = parentComponent fetchData childComponent
