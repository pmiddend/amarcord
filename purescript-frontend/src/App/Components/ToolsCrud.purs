module App.Components.ToolsCrud where

import App.API (Tool, ToolsResponse, addTool, editTool, removeTool, retrieveTools)
import App.AppMonad (AppMonad)
import App.Bootstrap (TableFlag(..), plainH2_, table)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.Halogen.FontAwesome (icon)
import App.HalogenUtils (classList, makeRequestResult, singleClass)
import Control.Applicative (pure, when)
import Control.Bind (bind)
import Data.Array (find, singleton)
import Data.Either (Either(..))
import Data.Eq ((/=), (==))
import Data.Function ((<<<), (>>>))
import Data.Functor ((<$>))
import Data.Maybe (Maybe(..))
import Data.Monoid (mempty)
import Data.Semigroup ((<>))
import Data.Show (show)
import Data.String (Pattern(..), joinWith, split)
import Data.Unit (Unit, unit)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties (ButtonType(..), InputType(..))
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData(..), fromEither)
import Web.HTML (window)
import Web.HTML.Window (confirm)

type ToolsCrudInput
  = {}

type State
  = { tools :: Array Tool
    , editTool :: Tool
    , lastRequest :: RemoteData String String
    }

toolEditing :: Tool -> Boolean
toolEditing t = t.toolId /= 0

stateEditing :: State -> Boolean
stateEditing s = toolEditing s.editTool

data Action
  = ChangeTool (Tool -> Tool)
  | RemoveTool Int
  | EditTool Int
  | AppendCommandLine String
  | FinishEditing
  | CancelEditing

fetchData :: ToolsCrudInput -> AppMonad (RemoteData String ToolsResponse)
fetchData _ = fromEither <$> retrieveTools

component :: forall query output. H.Component HH.HTML query ToolsCrudInput output AppMonad
component = parentComponent fetchData childComponent

emptyTool :: Tool
emptyTool = { toolId: 0, created: "", name: "", executablePath: "", extraFiles: [], commandLine: "", description: "", inputs: mempty }

initialState :: ChildInput ToolsCrudInput ToolsResponse -> State
initialState { input: _, remoteData: { tools } } =
  { tools
  , lastRequest: NotAsked
  , editTool:
      emptyTool
  }

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  AppendCommandLine c -> do
    H.modify_ \state -> state { editTool = state.editTool { commandLine = state.editTool.commandLine <> " " <> c } }
  ChangeTool f -> do
    currentTool <- H.gets _.editTool
    H.modify_ \state -> state { editTool = f currentTool }
  RemoveTool toolId -> do
    w <- H.liftEffect window
    confirmResult <- H.liftEffect (confirm "Really delete this tool?" w)
    when confirmResult do
      toolRemoval <- H.lift (removeTool toolId)
      case toolRemoval of
        Right { tools } -> H.modify_ \state -> state { tools = tools, lastRequest = Success "Tool removed!" }
        Left e -> H.modify_ \state -> state { lastRequest = Failure e }
  EditTool toolId -> do
    tools <- H.gets _.tools
    case find (\t -> t.toolId == toolId) tools of
      Nothing -> pure unit
      Just tool -> H.modify_ \state -> state { editTool = tool }
  CancelEditing -> H.modify_ \state -> state { editTool = emptyTool }
  FinishEditing -> do
    currentTool <- H.gets _.editTool
    newTools <- H.lift ((if toolEditing currentTool then editTool else addTool) currentTool)
    case newTools of
      Right ({ tools }) -> H.modify_ \state -> state { lastRequest = Success "Edit successful!", tools = tools, editTool = emptyTool }
      Left e -> H.modify_ \state -> state { lastRequest = Failure e }

toolsTable :: forall w. State -> HH.HTML w Action
toolsTable state =
  let
    makeExtraFiles :: Array String -> HH.HTML w Action
    makeExtraFiles [] = HH.text ""

    makeExtraFiles xs = HH.ul_ ((HH.li_ <<< singleton <<< makeMonospace) <$> xs)

    makeMonospace :: String -> HH.HTML w Action
    makeMonospace x = HH.span [ singleClass "font-monospace" ] [ HH.text x ]

    makeActions tool =
      HH.div [ singleClass "text-nowrap" ]
        [ HH.button
            [ HP.type_ ButtonButton
            , classList [ "btn", "btn-danger", "btn-sm", "me-1" ]
            , HE.onClick \_ -> Just (RemoveTool tool.toolId)
            ]
            [ icon { name: "trash", size: Nothing, spin: false }, HH.text " Remove" ]
        , HH.button
            [ HP.type_ ButtonButton
            , classList [ "btn", "btn-info", "btn-sm" ]
            , HE.onClick \_ -> Just (EditTool tool.toolId)
            ]
            [ icon { name: "edit", size: Nothing, spin: false }, HH.text " Edit" ]
        ]

    makeRow tool =
      HH.tr_
        ( (HH.td_ <<< singleton)
            <$> [ makeActions tool
              , HH.text (show tool.toolId)
              , HH.text tool.name
              , makeMonospace tool.executablePath
              , makeExtraFiles tool.extraFiles
              , makeMonospace tool.commandLine
              , HH.text tool.description
              ]
        )
  in
    table
      "tools-table"
      [ TableStriped ]
      ((HH.text >>> singleton >>> HH.th_) <$> [ "Action", "ID", "Name", "Executable path", "Files", "CLI", "Description" ])
      (makeRow <$> state.tools)

editForm :: forall slots. State -> H.ComponentHTML Action slots AppMonad
editForm state =
  let
    parseExtraFiles :: String -> Array String
    parseExtraFiles = split (Pattern "\n")

    coparseExtraFiles :: Array String -> String
    coparseExtraFiles = joinWith "\n"

    specialParameters :: Array { name :: String, description :: String }
    specialParameters =
      [ { name: "${Data_Reduction.mtz_path}", description: "Full path to the MTZ file for the processing result" }
      , { name: "${Data_Reduction.resolution_cc}", description: "Cut resolution of the processing result" }
      ]

    makeParameterTableRow { name, description } =
      HH.tr_
        [ HH.td_ [ HH.button [ HP.type_ ButtonButton, singleClass "btn btn-link btn-sm", HE.onClick (\_ -> Just (AppendCommandLine name)) ] [ HH.code_ [ HH.text name ] ] ]
        , HH.td_ [ HH.text description ]
        ]
  in
    HH.form [ singleClass "mb-3" ]
      [ HH.div [ singleClass "mb-3" ]
          [ HH.label [ HP.for "editName", singleClass "form-label" ] [ HH.text "Name" ]
          , HH.input
              [ HP.type_ InputText
              , HP.id_ "editName"
              , HE.onValueChange (\newValue -> Just (ChangeTool (\t -> t { name = newValue })))
              , singleClass "form-control"
              , HP.value state.editTool.name
              ]
          , HH.div [ singleClass "form-text" ] [ HH.text "Must be unique among all tools" ]
          ]
      , HH.div [ singleClass "mb-3" ]
          [ HH.label [ HP.for "editExecutablePath", singleClass "form-label" ] [ HH.text "Executable path" ]
          , HH.input
              [ HP.type_ InputText
              , HE.onValueChange (\newValue -> Just (ChangeTool (\t -> t { executablePath = newValue })))
              , singleClass "form-control"
              , HP.value state.editTool.executablePath
              ]
          ]
      , HH.div [ singleClass "mb-3" ]
          [ HH.label [ HP.for "editExtraFiles", singleClass "form-label" ] [ HH.text "Extra files (one per line)" ]
          , HH.textarea
              [ HE.onValueChange (\newValue -> Just (ChangeTool (\t -> t { extraFiles = parseExtraFiles newValue })))
              , singleClass "form-control"
              , HP.value (coparseExtraFiles state.editTool.extraFiles)
              ]
          ]
      , HH.div [ singleClass "mb-3" ]
          [ HH.label [ HP.for "editCommandLine", singleClass "form-label" ] [ HH.text "Command line" ]
          , HH.input
              [ HP.type_ InputText
              , HE.onValueChange (\newValue -> Just (ChangeTool (\t -> t { commandLine = newValue })))
              , singleClass "form-control"
              , HP.value state.editTool.commandLine
              ]
          ]
      , HH.div [ singleClass "mb-3" ]
          [ HH.h3_ [ HH.text "Command line placeholders" ]
          , HH.p_ [ HH.small [ singleClass "text-muted" ] [ HH.text "Click to append to the command line" ] ]
          , table "command-line-placeholder-table" [ TableStriped ] [ HH.th_ [ HH.text "Flag" ], HH.th_ [ HH.text "Description" ] ]
              (makeParameterTableRow <$> specialParameters)
          ]
      , HH.div [ singleClass "mb-3" ]
          [ HH.label [ HP.for "editDescription", singleClass "form-label" ] [ HH.text "Description" ]
          , HH.input
              [ HP.type_ InputText
              , HE.onValueChange (\newValue -> Just (ChangeTool (\t -> t { description = newValue })))
              , singleClass "form-control"
              , HP.value state.editTool.description
              ]
          ]
      , HH.div_
          [ HH.button
              [ HP.type_ ButtonButton
              , classList [ "btn", "btn-primary", "me-2" ]
              , HE.onClick \_ -> Just FinishEditing
              ]
              [ icon { name: (if stateEditing state then "edit" else "plus"), size: Nothing, spin: false }, HH.text (if stateEditing state then " Edit" else " Add") ]
          , HH.button
              [ HP.type_ ButtonButton
              , classList [ "btn", "btn-secondary" ]
              , HE.onClick \_ -> Just CancelEditing
              ]
              [ icon { name: "ban", size: Nothing, spin: false }, HH.text " Cancel" ]
          ]
      ]

render :: forall slots. State -> H.ComponentHTML Action slots AppMonad
render state =
  HH.div_
    [ toolsTable state
    , plainH2_ (if stateEditing state then ("Edit " <> state.editTool.name) else "Add tool")
    , makeRequestResult state.lastRequest
    , editForm state
    ]

childComponent :: forall q. H.Component HH.HTML q (ChildInput ToolsCrudInput ToolsResponse) ParentError AppMonad
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
