module App.Components.EditRun where

import Prelude hiding (comparing)
import App.API (addComment, deleteComment, retrieveRun)
import App.AppMonad (AppMonad)
import App.Bootstrap (container, plainH1_, plainH2_, plainH3_, plainTd_, plainTh_, table)
import App.Comment (Comment)
import App.Components.Modal (ModalOutput(..), modal)
import App.Components.ParentComponent (ParentError, ChildInput, parentComponent)
import App.HalogenUtils (classList, faIcon, makeRequestResult, plainTd, plainTh, singleClass)
import App.Run (Run)
import App.UnfinishedComment (UnfinishedComment, _author, _text, emptyUnfinishedComment)
import Data.Array ((:))
import Data.Either (Either(..))
import Data.Lens (set, toArrayOf, use, (.=))
import Data.Lens.Record (prop)
import Data.Maybe (Maybe(..), isJust)
import Data.Symbol (SProxy(..))
import Data.Traversable (for, for_)
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
    , newComment :: UnfinishedComment
    , commentRequest :: RemoteData String String
    , deleteCommentId :: Maybe Int
    }

_deleteCommentId = prop (SProxy :: SProxy "deleteCommentId")

_newComment = prop (SProxy :: SProxy "newComment")

_run = prop (SProxy :: SProxy "run")

_runId = prop (SProxy :: SProxy "runId")

_manualProperties = prop (SProxy :: SProxy "manualProperties")

_commentRequest = prop (SProxy :: SProxy "commentRequest")

data Action
  = ModifyState (State -> State)
  | AddComment
  | ConfirmDeleteComment Int
  | CommentModal ModalOutput

initialState :: ChildInput Int Run -> State
initialState { input: runId, remoteData: run } =
  { runId: runId
  , run: run
  , newComment: emptyUnfinishedComment
  , commentRequest: NotAsked
  , deleteCommentId: Nothing
  }

component :: forall output query. H.Component HH.HTML query Int output AppMonad
component = parentComponent fetchRunData childComponent

childComponent :: forall q. H.Component HH.HTML q (ChildInput Int Run) ParentError AppMonad
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

fetchRunData :: Int -> AppMonad (RemoteData String Run)
fetchRunData rid = fromEither <$> (retrieveRun rid)

resetRunData :: forall slots. H.HalogenM State Action slots ParentError AppMonad Unit
resetRunData = do
  rid <- use _runId
  newRunData' <- lift $ (fetchRunData rid)
  case newRunData' of
    Success newRunData -> H.modify_ (set _run newRunData)
    _ -> pure unit

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  ModifyState f -> H.modify_ f
  ConfirmDeleteComment i -> _deleteCommentId .= Just i
  CommentModal Hidden -> _deleteCommentId .= Nothing
  CommentModal Confirmed -> do
    cid' <- use _deleteCommentId
    _deleteCommentId .= Nothing
    for_ cid' \cid -> do
      _commentRequest .= Loading
      rid <- use _runId
      commentResponse <- deleteComment rid cid
      case commentResponse of
        Left e -> _commentRequest .= Failure e
        Right _ -> do
          H.modify_ (set _commentRequest (Success "Comment removed!"))
          resetRunData
  AddComment -> do
    rid <- H.gets _.runId
    newComment <- H.gets _.newComment
    _commentRequest .= Loading
    commentResponse <- addComment rid newComment
    case commentResponse of
      Left e -> _commentRequest .= Failure e
      Right _ -> do
        H.modify_ (set _commentRequest (Success "Comment added!") >>> set _newComment emptyUnfinishedComment)
        resetRunData

commentsTable :: forall w. RemoteData String String -> Array Comment -> HH.HTML w Action
commentsTable commentRequest comments =
  let
    makeDeleteButton :: Int -> HH.HTML w Action
    makeDeleteButton commentId =
      HH.button
        [ HP.disabled (isLoading commentRequest)
        , classList
            [ "btn", "btn-link", "btn-sm", "p-0" ]
        , HE.onClick \_ -> Just (ConfirmDeleteComment commentId)
        ]
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
    <> [ commentsTable state.commentRequest state.run.comments
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

type Slots q
  = ( deleteModal :: H.Slot q ModalOutput Int )

render ::
  forall q.
  State ->
  H.ComponentHTML Action (Slots q) AppMonad
render state =
  let
    deleteModal =
      HH.slot
        (SProxy :: SProxy "deleteModal")
        0
        modal
        { modalId: "delete-modal"
        , confirmText: "Delete?"
        , title: "Really delete?"
        , open: isJust (state.deleteCommentId)
        , renderBody: \_ -> HH.div_ [ HH.text "oh shit!" ]
        }
        (Just <<< CommentModal)
  in
    container $ deleteModal : plainH1_ ("Run " <> show state.runId) : commentsSection state
