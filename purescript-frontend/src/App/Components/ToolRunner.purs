module App.Components.ToolRunner where

import App.API (DiffractionList, Tool, ToolsResponse, ToolInputMap, retrieveTools, startJob)
import App.AppMonad (AppMonad)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.HalogenUtils (classList, makeRequestResult, singleClass)
import Control.Bind (bind)
import Data.Array (intercalate, length)
import Data.BooleanAlgebra ((||))
import Data.Either (Either(..))
import Data.Eq ((==))
import Data.Foldable (find, foldMap, for_)
import Data.FoldableWithIndex (foldMapWithIndex)
import Data.Function (const)
import Data.Functor ((<$>))
import Data.Int (fromString)
import Data.Map as Map
import Data.Maybe (Maybe(..), isNothing)
import Data.Monoid (mempty)
import Data.Ord ((>))
import Data.Semigroup ((<>))
import Data.Show (show)
import Data.Tuple (Tuple(..))
import Data.Unit (Unit)
import Debug.Trace (spy)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData(..), fromEither)

data Action
  = ChangeTool Int
  | RunTool
  | ChangeInput String String
  | UpdateDiffractions DiffractionList

type ToolRunnerInput
  = { diffractions :: DiffractionList }

type State
  = { tools :: Array Tool
    , selectedToolId :: Maybe Int
    , toolInputs :: ToolInputMap
    , lastRequest :: RemoteData String String
    , diffractions :: DiffractionList
    }

fetchData :: ToolRunnerInput -> AppMonad (RemoteData String ToolsResponse)
fetchData _ = fromEither <$> retrieveTools

initialState :: ChildInput ToolRunnerInput ToolsResponse -> State
initialState { input: { diffractions }, remoteData: { tools } } =
  { selectedToolId: Nothing
  , toolInputs: mempty
  , lastRequest: NotAsked
  , diffractions
  , tools
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

    numberOfDiffs = length state.diffractions
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
          <> [ HH.div_
                [ HH.button
                    [ HP.type_ HP.ButtonButton
                    , classList [ "btn", "btn-primary" ]
                    , HP.disabled (numberOfDiffs == 0 || isNothing state.selectedToolId)
                    , HE.onClick (const (Just RunTool))
                    ]
                    [ HH.text "Run tool" ]
                , HH.div
                    [ singleClass "form-text" ]
                    (if numberOfDiffs > 0 then [ HH.text ("The tool will be run on " <> show numberOfDiffs <> " diffraction" <> (if numberOfDiffs > 1 then "s" else "")) ] else [])
                ]
            ]
      )

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  UpdateDiffractions newDiffractions ->
    H.modify_ \state -> state { diffractions = spy "hello" newDiffractions }
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
            }
  ChangeInput name newValue -> H.modify_ \state -> state { toolInputs = Map.insert name newValue state.toolInputs }
  RunTool -> do
    s <- H.get
    for_ s.selectedToolId \toolId -> do
      result <- H.lift (startJob toolId s.toolInputs s.diffractions)
      case result of
        Right { jobIds } ->
          H.modify_ \state ->
            state
              { lastRequest = Success ("Started job(s) " <> intercalate ", " (show <$> jobIds))
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
            , receive = \{ input: { diffractions } } -> Just (UpdateDiffractions diffractions)
            }
    }

component :: forall query output. H.Component HH.HTML query ToolRunnerInput output AppMonad
component = parentComponent fetchData childComponent
