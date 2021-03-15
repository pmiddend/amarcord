module App.Components.EditRun where

import Prelude hiding (comparing)

import App.API (AttributiResponse, addComment, deleteComment, retrieveRun, retrieveRunAttributi)
import App.AppMonad (AppMonad, log)
import App.Attributo (Attributo, _description, _name, rpName, rpType, validateNumeric)
import App.Bootstrap (container, plainH1_, plainH2_, plainH3_, plainTd_, plainTh_, table)
import App.Comment (Comment)
import App.Components.Modal (ModalOutput(..), modal)
import App.Components.ParentComponent (ParentError, ChildInput, parentComponent)
import App.HalogenUtils (classList, faIcon, makeRequestResult, singleClass)
import App.JSONSchemaType (JSONSchemaType(..))
import App.Logging (LogLevel(..))
import App.NumericRange (NumericRange, prettyPrintRange)
import App.Run (Run, Source, locateAttributo)
import App.RunScalar (RunScalar(..))
import App.RunValue (RunValue(..))
import App.UnfinishedComment (UnfinishedComment, _author, _text, emptyUnfinishedComment)
import App.Utils (fanoutApplicative, findCascade, mapTuple)
import Data.Array ((:))
import Data.Either (Either(..))
import Data.Lens (Lens', set, use, (%=), (.=), (<>=), (^.))
import Data.Lens.At (at)
import Data.Lens.Record (prop)
import Data.List (toUnfoldable)
import Data.Map (Map, fromFoldable, values)
import Data.Maybe (Maybe(..), fromMaybe, isJust)
import Data.Number (fromString)
import Data.Set (Set, delete, member, singleton)
import Data.Symbol (SProxy(..))
import Data.Traversable (class Foldable, for_)
import Data.Tuple (Tuple(..), fst, snd)
import Halogen (lift)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties (InputType(..))
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData(..), fromEither, isLoading)

type EditRunInput
  = Tuple Run AttributiResponse

type State
  = { runId :: Int
    , run :: Run
    , runAttributi :: Map String Attributo
    , newComment :: UnfinishedComment
    , commentRequest :: RemoteData String String
    , deleteCommentId :: Maybe Int
    , invalidFields :: Set String
    }

_invalidFields :: Lens' State (Set String)
_invalidFields = prop (SProxy :: SProxy "invalidFields")

_deleteCommentId :: Lens' State (Maybe Int)
_deleteCommentId = prop (SProxy :: SProxy "deleteCommentId")

_newComment :: Lens' State UnfinishedComment
_newComment = prop (SProxy :: SProxy "newComment")

_run :: Lens' State Run
_run = prop (SProxy :: SProxy "run")

_runAttributi :: Lens' State (Map String Attributo)
_runAttributi = prop (SProxy :: SProxy "runAttributi")

_runId :: Lens' State Int
_runId = prop (SProxy :: SProxy "runId")

_commentRequest :: Lens' State (RemoteData String String)
_commentRequest = prop (SProxy :: SProxy "commentRequest")

data CommentAction =  AddComment
  | ConfirmDeleteComment Int
  | CommentModal ModalOutput


data Action
  = ModifyState (State -> State)
  | CommentSubAction CommentAction
  | AttributoNumberChange String Number

attributiListToMap :: forall a. Foldable a => Functor a => a Attributo -> Map String Attributo
attributiListToMap attributi = fromFoldable ((\a -> Tuple (rpName a) a) <$> attributi)

initialState :: ChildInput Int EditRunInput -> State
initialState { input: runId, remoteData: (Tuple run runAttributiResponse) } =
  { runId: runId
  , run: run
  , runAttributi: attributiListToMap runAttributiResponse.attributi
  , newComment: emptyUnfinishedComment
  , commentRequest: NotAsked
  , deleteCommentId: Nothing
  , invalidFields: mempty
  }

component :: forall output query. H.Component HH.HTML query Int output AppMonad
component = parentComponent fetchRunData childComponent

childComponent :: forall q. H.Component HH.HTML q (ChildInput Int EditRunInput) ParentError AppMonad
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

fetchRunData :: Int -> AppMonad (RemoteData String (Tuple Run AttributiResponse))
fetchRunData rid = fromEither <$> (fanoutApplicative <$> retrieveRun rid <*> retrieveRunAttributi)

-- Re-retrieve run data and fill form with it
resetRunData :: forall slots. H.HalogenM State Action slots ParentError AppMonad Unit
resetRunData = do
  rid <- use _runId
  newRunData' <- lift $ (fetchRunData rid)
  case newRunData' of
    Success (Tuple newRunData newAttributi) -> H.modify_ (set _run newRunData <<< set _runAttributi (attributiListToMap newAttributi.attributi))
    _ -> pure unit

-- Retrieve attributo from state, errors if doesn't exist
-- (Shouldn't happen, but still)
withAttributo :: forall slots. String ->
                 (Attributo -> H.HalogenM State Action slots ParentError AppMonad Unit) ->
                 H.HalogenM State Action slots ParentError AppMonad Unit
withAttributo name f =  do
  attributo' <- use (_runAttributi <<< at name)
  case attributo' of
      Nothing -> lift $ log Info ("Attributo \"" <> name <> "\" not found")
      Just attributo -> f attributo

handleCommentAction :: forall slots. CommentAction -> H.HalogenM State Action slots ParentError AppMonad Unit
handleCommentAction = case _ of
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

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  CommentSubAction ca -> handleCommentAction ca
  ModifyState f -> H.modify_ f
  AttributoNumberChange attributoName newValue -> withAttributo attributoName $ \attributo ->
      if validateNumeric newValue attributo
      then _invalidFields %= delete attributoName
      else _invalidFields <>= singleton attributoName

commentsTable :: forall w. RemoteData String String -> Array Comment -> HH.HTML w Action
commentsTable commentRequest comments =
  let
    makeDeleteButton :: Int -> HH.HTML w Action
    makeDeleteButton commentId =
      HH.button
        [ HP.disabled (isLoading commentRequest)
        , classList
            [ "btn", "btn-link", "btn-sm", "p-0" ]
        , HE.onClick \_ -> Just (CommentSubAction (ConfirmDeleteComment commentId))
        ]
        [ faIcon "trash" ]

    makeRow comment =
      HH.tr_
        $ (HH.td_ [ makeDeleteButton (comment.id) ])
        : (plainTd_ <$> [ comment.created, comment.author, comment.text ])
  in
    table [] (plainTh_ <$> [ "Actions", "Created", "Author", "Text" ]) (makeRow <$> comments)

commentsSection :: forall w. State -> Array (HH.HTML w Action)
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
                      , HE.onClick \_ -> Just (CommentSubAction AddComment)
                      , HP.disabled (isLoading state.commentRequest || state.newComment.author == "" || state.newComment.text == "")
                      ]
                  ]
              ]
          ]
      ]

runValueToString :: RunValue -> String
runValueToString rv = case rv of
  Scalar (RunScalarNumber n) -> show n
  Scalar (RunScalarInt n) -> show n
  Scalar (RunScalarString n) -> n
  Comments _ -> ""

formatRange :: forall a w i. Show a => Maybe (NumericRange a) -> Array (HH.HTML w i)
formatRange Nothing = []

formatRange (Just r) = [ HH.small [ classList [ "ml-3", "form-text", "text-muted" ] ] [ HH.text ("value ∈ " <> prettyPrintRange r) ] ]

attributoToInput :: forall w. Set String -> Tuple Attributo (Maybe (Tuple Source RunValue)) -> Array (HH.HTML w Action)
attributoToInput invalidFields (Tuple rp v) =
  let
    sourceToClass = case v of
      Just (Tuple "manual" _) -> [ "manual-edit" ]
      _ -> []
    invalidToClass = if member (rpName rp) invalidFields then ["is-invalid"] else ["is-valid"]
  in
    case rpType rp of
      Just (JSONNumber { suffix: Just suffix, range }) ->
        let
          classes = [ "form-control" ] <> sourceToClass <> invalidToClass
        in
          [ HH.input [ HP.type_ InputNumber, HP.id_ (rp ^. _name), classList classes, HE.onValueInput (\newInput -> (AttributoNumberChange (rp ^. _name) <$> fromString newInput)), HP.value (fromMaybe "" ((runValueToString <<< snd) <$> v)) ]
          , HH.div [ classList [ "input-group-append" ] ] [ HH.div [ singleClass "input-group-text" ] [ HH.text suffix ] ]
          ]
            <> formatRange range
      Just (JSONNumber { suffix: Nothing, range }) -> [ HH.input [ HP.type_ InputNumber, HP.id_ (rp ^. _name), classList [ "form-control", "col-sm-10" ] ] ] <> formatRange range
      Just JSONString -> [ HH.input [ HP.type_ InputText, HP.id_ (rp ^. _name), classList [ "form-control", "col-sm-10" ] ] ]
      Just JSONInteger -> [ HH.input [ HP.type_ InputNumber, HP.id_ (rp ^. _name), classList [ "form-control", "col-sm-10" ] ] ]
      _ -> [ HH.text "" ]

attributiWithValues :: State -> Array (Tuple Attributo (Maybe (Tuple Source RunValue)))
attributiWithValues state = mapTuple makeValue (toUnfoldable (values state.runAttributi))
  where
  makeValue :: Attributo -> Maybe (Tuple Source RunValue)
  makeValue rp = findCascade (locateAttributo (rp ^. _name) state.run.attributi) fst [ "manual", "offline", "online" ]

attributiSection :: forall w. State -> Array (HH.HTML w Action)
attributiSection state =
  let
    makeFormElement :: Tuple Attributo (Maybe (Tuple Source RunValue)) -> HH.HTML w Action
    makeFormElement (Tuple rp v) =
      HH.div [ classList [ "form-group", "form-row" ] ]
        [ HH.label [ HP.for (rp ^. _name), classList [ "col-sm-2", "col-form-label" ] ] [ HH.text (rp ^. _description) ]
        , HH.div [ classList [ "input-group", "col-sm-10" ] ] (attributoToInput state.invalidFields (Tuple rp v))
        ]

    formElements = makeFormElement <$> (attributiWithValues state)
  in
    plainH2_ "Attributi" : [ HH.form_ formElements ]

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
        (Just <<< CommentSubAction <<< CommentModal)
  in
    container $ deleteModal : plainH1_ ("Run " <> show state.runId) : (attributiSection state <> commentsSection state)
