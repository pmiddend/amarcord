module App.Components.EditRun where

import Prelude hiding (comparing)

import App.API (RunResponse, addComment, retrieveRun)
import App.AppMonad (AppMonad)
import App.Bootstrap (container, plainH1_, plainH2_, plainH3_, plainTd_, plainTh_, table)
import App.Comment (Comment)
import App.Components.ParentComponent (ParentError, ChildInput, parentComponent)
import App.HalogenUtils (classList, faIcon, makeRequestResult, plainTd, plainTh, singleClass)
import App.Run (Run, runComments)
import App.UnfinishedComment (UnfinishedComment, _author, _text, emptyUnfinishedComment)
import Data.Array ((:))
import Data.Either (Either(..))
import Data.Lens (set, toArrayOf, (.=))
import Data.Lens.Record (prop)
import Data.Maybe (Maybe(..))
import Data.Symbol (SProxy(..))
import Halogen (lift)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties (InputType(..))
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData(..), fromEither, isLoading)

type State
  = { runId :: Int
    , run :: Run
    , manualProperties :: Array String
    , newComment :: UnfinishedComment
    , commentRequest :: RemoteData String String
    }

_newComment = prop (SProxy :: SProxy "newComment")

_run = prop (SProxy :: SProxy "run")

_manualProperties = prop (SProxy :: SProxy "manualProperties")

_commentRequest = prop (SProxy :: SProxy "commentRequest")

data Action
  = ModifyState (State -> State)
  | AddComment
  | ConfirmDeleteComment Int

initialState :: ChildInput Int RunResponse -> State
initialState { input: runId, remoteData: runResponse } =
  { runId: runId
  , run: runResponse.run
  , manualProperties: runResponse.manual_properties
  , newComment: emptyUnfinishedComment
  , commentRequest: NotAsked
  }

component :: forall output query. H.Component HH.HTML query Int output AppMonad
component = parentComponent fetchRunData childComponent

childComponent :: forall q. H.Component HH.HTML q (ChildInput Int RunResponse) ParentError AppMonad
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

fetchRunData :: Int -> AppMonad (RemoteData String RunResponse)
fetchRunData rid = fromEither <$> (retrieveRun rid)

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  ModifyState f -> H.modify_ f
  ConfirmDeleteComment i -> pure unit
  AddComment -> do
    rid <- H.gets _.runId
    newComment <- H.gets _.newComment
    _commentRequest .= Loading
    commentResponse <- lift $ addComment rid newComment
    case commentResponse of
      Left e -> _commentRequest .= Failure e
      Right _ -> do
        H.modify_ (set _commentRequest (Success "Comment successfully added!") >>> set _newComment emptyUnfinishedComment)
        newRunData' <- lift $ fetchRunData rid
        case newRunData' of
          Success newRunData -> H.modify_ (set _run newRunData.run >>> set _manualProperties newRunData.manual_properties)
          _ -> pure unit

commentsTable :: forall w. Array Comment -> HH.HTML w Action
commentsTable comments =
  let
    makeDeleteButton :: Int -> HH.HTML w Action
    makeDeleteButton commentId =
      HH.button
        [ classList [ "btn", "btn-link", "btn-sm", "p-0" ], HE.onClick \_ -> Just (ConfirmDeleteComment commentId) ]
        [ faIcon "trash" ]

    makeRow comment =
      HH.tr_
        $ (HH.td_ [ makeDeleteButton (comment.id) ])
        : (plainTd_ <$> [ comment.created, comment.author, comment.text ])
  in
   table [] (plainTh_ <$> [ "Actions", "Created", "Author", "Text" ]) (makeRow <$> comments)

commentsSection state =
  [ plainH2_ "Comments" ]
    <> [ makeRequestResult state.commentRequest ]
    <> [ commentsTable (toArrayOf runComments state.run)
      , plainH3_ "Add comment"
      , HH.form_
          [ HH.div [ singleClass "row" ]
              [ HH.div [ singleClass "col" ]
                  [ textInput "Author" state.newComment.author (_newComment <<< _author) ]
              , HH.div [ singleClass "col" ]
                  [ textInput "Text" state.newComment.text (_newComment <<< _text) ]
              , HH.div [ singleClass "col" ]
                  [ HH.input
                      [ HP.type_ InputSubmit
                      , HP.value "Add"
                      , classList [ "form-control", "btn", "btn-primary" ]
                      , HE.onClick \_ -> Just AddComment
                      , HP.disabled (isLoading state.commentRequest || state.newComment.author == "" || state.newComment.text == "")
                      ]
                  ]
              ]
          ]
      ]

textInput placeholder value modifier =
  HH.input
    [ HP.type_ InputText
    , singleClass "form-control"
    , HP.placeholder placeholder
    , HP.value value
    , HE.onValueInput (set modifier >>> ModifyState >>> Just)
    ]

render ::
  forall slots.
  State ->
  H.ComponentHTML Action slots AppMonad
render state = container $ plainH1_ ("Run " <> show state.runId) : commentsSection state
