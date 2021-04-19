module App.Components.EditRun where

-- import Prelude hiding (comparing)
-- import App.API (AttributiResponse, addComment, changeRunAttributo, deleteComment, retrieveRun, retrieveRunAttributi)
-- import App.AppMonad (AppMonad, log)
-- import App.Attributo (Attributo, _description, _name, _typeSchema)
-- import App.AttributoUtil (PairedAttributo(..), ProcessedAttributo, attributiAndErrors)
-- import App.AttributoValue (AttributoValue(..))
-- import App.Bootstrap (container, plainH1_, plainH2_, plainH3_, plainTd_, plainTh_, table)
-- import App.Comment (Comment)
-- import App.Components.Modal (ModalOutput(..), modal)
-- import App.Components.ParentComponent (ParentError, ChildInput, parentComponent)
-- import App.HalogenUtils (classList, faIcon, makeAlert, makeRequestResult, singleClass)
-- import App.JSONSchemaType (JSONSchemaType(..))
-- import App.Logging (LogLevel(..))
-- import App.NumericRange (NumericRange, inRange, prettyPrintRange)
-- import App.Run (Run, _attributi)
-- import App.UnfinishedComment (UnfinishedComment, _author, _text, emptyUnfinishedComment)
-- import App.Utils (fanoutApplicative)
-- import Data.Array ((:))
-- import Data.Either (Either(..))
-- import Data.Int (fromNumber)
-- import Data.Lens (Lens', non, set, use, (.=), (^.))
-- import Data.Lens.At (at)
-- import Data.Lens.Record (prop)
-- import Data.List (List(..), intercalate, toUnfoldable)
-- import Data.Map (Map, filterKeys, fromFoldable)
-- import Data.Maybe (Maybe(..), fromMaybe, isJust)
-- import Data.Number (fromString)
-- import Data.Set as Set
-- import Data.Symbol (SProxy(..))
-- import Data.Traversable (class Foldable, for_)
-- import Data.Tuple (Tuple(..))
-- import Foreign.Object (empty)
-- import Halogen (lift)
-- import Halogen as H
-- import Halogen.HTML as HH
-- import Halogen.HTML.Events as HE
-- import Halogen.HTML.Properties (InputType(..))
-- import Halogen.HTML.Properties as HP
-- import Network.RemoteData (RemoteData(..), fromEither, isLoading)

-- type EditRunInput
--   = Tuple Run AttributiResponse

-- type State
--   = { runId :: Int
--     , run :: Run
--     , availableAttributi :: Map String Attributo
--     , newComment :: UnfinishedComment
--     , commentRequest :: RemoteData String String
--     , deleteCommentId :: Maybe Int
--     , runRequest :: RemoteData String String
--     }

-- _deleteCommentId :: Lens' State (Maybe Int)
-- _deleteCommentId = prop (SProxy :: SProxy "deleteCommentId")

-- _newComment :: Lens' State UnfinishedComment
-- _newComment = prop (SProxy :: SProxy "newComment")

-- _run :: Lens' State Run
-- _run = prop (SProxy :: SProxy "run")

-- _runRequest :: Lens' State (RemoteData String String)
-- _runRequest = prop (SProxy :: SProxy "runRequest")

-- _availableAttributi :: Lens' State (Map String Attributo)
-- _availableAttributi = prop (SProxy :: SProxy "availableAttributi")

-- _runId :: Lens' State Int
-- _runId = prop (SProxy :: SProxy "runId")

-- _commentRequest :: Lens' State (RemoteData String String)
-- _commentRequest = prop (SProxy :: SProxy "commentRequest")

-- data CommentAction
--   = AddComment
--   | ConfirmDeleteComment Int
--   | CommentModal ModalOutput

-- data Action
--   = ModifyState (State -> State)
--   | CommentSubAction CommentAction
--   | AttributoValueChange String AttributoValue
--   | AttributoBlur String

-- attributiListToMap :: forall a. Foldable a => Functor a => a Attributo -> Map String Attributo
-- attributiListToMap attributi = fromFoldable ((\a -> Tuple (a ^. _name) a) <$> attributi)

-- initialState :: ChildInput Int EditRunInput -> State
-- initialState { input: runId, remoteData: (Tuple run availableAttributiResponse) } =
--   { runId: runId
--   , run: run
--   , availableAttributi: attributiListToMap availableAttributiResponse.attributi
--   , newComment: emptyUnfinishedComment
--   , commentRequest: NotAsked
--   , deleteCommentId: Nothing
--   , runRequest: NotAsked
--   }

-- component :: forall output query. H.Component HH.HTML query Int output AppMonad
-- component = parentComponent fetchRunData childComponent

-- childComponent :: forall q. H.Component HH.HTML q (ChildInput Int EditRunInput) ParentError AppMonad
-- childComponent =
--   H.mkComponent
--     { initialState
--     , render
--     , eval:
--         H.mkEval
--           H.defaultEval
--             { handleAction = handleAction
--             }
--     }

-- fetchRunData :: Int -> AppMonad (RemoteData String (Tuple Run AttributiResponse))
-- fetchRunData rid = fromEither <$> (fanoutApplicative <$> retrieveRun rid <*> retrieveRunAttributi)

-- -- Re-retrieve run data and fill form with it
-- resetRunData :: forall slots. H.HalogenM State Action slots ParentError AppMonad Unit
-- resetRunData = do
--   rid <- use _runId
--   newRunData' <- lift $ (fetchRunData rid)
--   case newRunData' of
--     Success (Tuple newRunData newAttributi) -> H.modify_ (set _run newRunData <<< set _availableAttributi (attributiListToMap newAttributi.attributi))
--     _ -> pure unit

-- -- Retrieve attributo from state, errors if doesn't exist
-- -- (Shouldn't happen, but still)
-- withAttributo ::
--   forall slots.
--   String ->
--   (Attributo -> H.HalogenM State Action slots ParentError AppMonad Unit) ->
--   H.HalogenM State Action slots ParentError AppMonad Unit
-- withAttributo name f = do
--   attributo' <- use (_availableAttributi <<< at name)
--   case attributo' of
--     Nothing -> lift $ log Info ("Attributo \"" <> name <> "\" not found")
--     Just attributo -> f attributo

-- handleCommentAction :: forall slots. CommentAction -> H.HalogenM State Action slots ParentError AppMonad Unit
-- handleCommentAction = case _ of
--   ConfirmDeleteComment i -> _deleteCommentId .= Just i
--   CommentModal Hidden -> _deleteCommentId .= Nothing
--   CommentModal Confirmed -> do
--     cid' <- use _deleteCommentId
--     _deleteCommentId .= Nothing
--     for_ cid' \cid -> do
--       _commentRequest .= Loading
--       rid <- use _runId
--       commentResponse <- deleteComment rid cid
--       case commentResponse of
--         Left e -> _commentRequest .= Failure e
--         Right _ -> do
--           H.modify_ (set _commentRequest (Success "Comment removed!"))
--           resetRunData
--   AddComment -> do
--     rid <- H.gets _.runId
--     newComment <- H.gets _.newComment
--     _commentRequest .= Loading
--     commentResponse <- addComment rid newComment
--     case commentResponse of
--       Left e -> _commentRequest .= Failure e
--       Right _ -> do
--         H.modify_ (set _commentRequest (Success "Comment added!") >>> set _newComment emptyUnfinishedComment)
--         resetRunData

-- handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
-- handleAction = case _ of
--   CommentSubAction ca -> handleCommentAction ca
--   ModifyState f -> H.modify_ f
--   AttributoValueChange name value -> H.modify_ (set (_run <<< _attributi <<< at "manual" <<< non empty <<< at name) (Just value))
--   AttributoBlur name -> do
--     _runRequest .= Loading
--     currentState <- H.get
--     case currentState ^. _run <<< _attributi <<< at "manual" <<< non empty <<< at name of
--       Nothing -> pure unit
--       Just attributoValue -> do
--         runResponse <- changeRunAttributo (currentState ^. _runId) name attributoValue
--         case runResponse of
--           Left e -> _runRequest .= Failure e
--           Right _ -> H.modify_ (set _commentRequest (Success "Value changed!") >>> set _newComment emptyUnfinishedComment)

-- commentsTable :: forall w. RemoteData String String -> Array Comment -> HH.HTML w Action
-- commentsTable commentRequest comments =
--   let
--     makeDeleteButton :: Int -> HH.HTML w Action
--     makeDeleteButton commentId =
--       HH.button
--         [ HP.disabled (isLoading commentRequest)
--         , classList
--             [ "btn", "btn-link", "btn-sm", "p-0" ]
--         , HE.onClick \_ -> Just (CommentSubAction (ConfirmDeleteComment commentId))
--         ]
--         [ faIcon "trash" ]

--     makeRow comment =
--       HH.tr_
--         $ (HH.td_ [ makeDeleteButton (comment.id) ])
--         : (plainTd_ <$> [ comment.created, comment.author, comment.text ])
--   in
--     table [] (plainTh_ <$> [ "Actions", "Created", "Author", "Text" ]) (makeRow <$> comments)

-- commentsSection :: forall w. State -> Array (HH.HTML w Action)
-- commentsSection state =
--   [ plainH2_ "Comments" ]
--     <> [ makeRequestResult state.commentRequest ]
--     <> [ commentsTable state.commentRequest state.run.comments
--       , plainH3_ "Add comment"
--       , HH.form_
--           [ HH.div [ singleClass "row" ]
--               [ HH.div [ singleClass "col" ]
--                   [ textInput "Author" state.newComment.author (_newComment <<< _author) ]
--               , HH.div [ singleClass "col" ]
--                   [ textInput "Text" state.newComment.text (_newComment <<< _text) ]
--               , HH.div [ singleClass "col" ]
--                   [ HH.input
--                       [ HP.type_ InputSubmit
--                       , HP.value "Add"
--                       , classList [ "form-control", "btn", "btn-primary" ]
--                       , HE.onClick \_ -> Just (CommentSubAction AddComment)
--                       , HP.disabled (isLoading state.commentRequest || state.newComment.author == "" || state.newComment.text == "")
--                       ]
--                   ]
--               ]
--           ]
--       ]

-- formatRange :: forall a w i. Show a => Maybe (NumericRange a) -> Array (HH.HTML w i)
-- formatRange Nothing = []

-- formatRange (Just r) = [ HH.small [ classList [ "ml-3", "form-text", "text-muted" ] ] [ HH.text ("value ∈ " <> prettyPrintRange r) ] ]

-- attributoToInput :: forall w. ProcessedAttributo -> Array (HH.HTML w Action)
-- attributoToInput { attributo, value } =
--   let
--     sourceToClass = case value of
--       Just { source: "manual" } -> [ "manual-edit" ]
--       _ -> []

--     valueMaybeInt :: Number -> String
--     valueMaybeInt n = case fromNumber n of
--       Just n' -> show n'
--       Nothing -> show n

--     valueToString (PairedNumber _ x) = valueMaybeInt x

--     valueToString (PairedString x) = x

--     valueToString (PairedInteger x) = show x

--     valueValid = case value of
--       Just { value: (PairedNumber { range: Just range } number) } -> inRange range number
--       _ -> true

--     invalidToClass = if valueValid then [ "is-valid" ] else [ "is-invalid" ]

--     numericInput classes =
--       HH.input
--         [ HP.type_ InputNumber
--         , HP.id_ (attributo ^. _name)
--         , classList classes
--         , HE.onValueInput (\newInput -> ((AttributoValueChange (attributo ^. _name) <<< NumericAttributo) <$> fromString newInput))
--         , HP.value (fromMaybe "" ((valueToString <<< _.value) <$> value))
--         , HE.onBlur (const (Just (AttributoBlur (attributo ^. _name))))
--         ]
--   in
--     case attributo ^. _typeSchema of
--       JSONNumber { suffix: Just suffix, range } ->
--         [ numericInput ([ "form-control" ] <> sourceToClass <> invalidToClass)
--         , HH.div
--             [ classList [ "input-group-append" ] ]
--             [ HH.div [ singleClass "input-group-text" ] [ HH.text suffix ] ]
--         ]
--           <> formatRange range
--       JSONNumber { suffix: Nothing, range } -> [ numericInput [ "form-control" ] ] <> formatRange range
--       JSONString -> [ HH.input [ HP.type_ InputText, HP.id_ (attributo ^. _name), classList [ "form-control" ] ] ]
--       JSONInteger -> [ HH.input [ HP.type_ InputNumber, HP.id_ (attributo ^. _name), classList [ "form-control" ] ] ]
--       _ -> [ HH.text "" ]

-- attributiSection :: forall w. State -> Array (HH.HTML w Action)
-- attributiSection state =
--   let
--     makeFormElement :: ProcessedAttributo -> HH.HTML w Action
--     makeFormElement pa@{ attributo, value } =
--       HH.div [ classList [ "form-group", "form-row" ] ]
--         [ HH.label [ HP.for (attributo ^. _name), classList [ "col-sm-2", "col-form-label" ] ] [ HH.text (attributo ^. _description) ]
--         , HH.div [ classList [ "input-group", "col-sm-10" ] ] (attributoToInput pa)
--         ]

--     ignoredAttributi = Set.fromFoldable [ "comments", "id", "modified", "proposal_id" ]

--     filteredAttributi = filterKeys ((\a -> Set.member a ignoredAttributi) >>> not) state.availableAttributi

--     { errors, attributi } = attributiAndErrors filteredAttributi state.run.attributi

--     errorsHtml = case errors of
--       Nil -> HH.text ""
--       _ -> makeAlert "critical" ("Errors trying to process some of the attributi: " <> intercalate ", " errors)

--     formElements = makeFormElement <$> attributi
--   in
--     plainH2_ "Attributi" : makeRequestResult state.runRequest : errorsHtml : [ HH.form_ (toUnfoldable formElements) ]

-- textInput placeholder value modifier =
--   HH.input
--     [ HP.type_ InputText
--     , singleClass "form-control"
--     , HP.placeholder placeholder
--     , HP.value value
--     , HE.onValueInput (set modifier >>> ModifyState >>> Just)
--     ]

-- type Slots q
--   = ( deleteModal :: H.Slot q ModalOutput Int )

-- render ::
--   forall q.
--   State ->
--   H.ComponentHTML Action (Slots q) AppMonad
-- render state =
--   let
--     deleteModal =
--       HH.slot
--         (SProxy :: SProxy "deleteModal")
--         0
--         modal
--         { modalId: "delete-modal"
--         , confirmText: "Delete?"
--         , title: "Really delete?"
--         , open: isJust (state.deleteCommentId)
--         , renderBody: \_ -> HH.div_ [ HH.text "oh shit!" ]
--         }
--         (Just <<< CommentSubAction <<< CommentModal)
--   in
--     container $ deleteModal : plainH1_ ("Run " <> show state.runId) : (attributiSection state <> commentsSection state)
