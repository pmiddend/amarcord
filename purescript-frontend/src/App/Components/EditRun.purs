module App.Components.EditRun where

import Prelude hiding (comparing)
import App.API (RunPropertiesResponse, RunResponse, RunsResponse, addComment, retrieveRun, retrieveRunProperties, retrieveRuns)
import App.AppMonad (AppMonad)
import App.Autocomplete as Autocomplete
import App.Comment (Comment)
import App.Components.ParentComponent (ParentError, ChildInput, parentComponent)
import App.HalogenUtils (classList, makeRequestResult, scope, plainTd, plainTh, singleClass, faIcon)
import App.Route (Route(..), RunsRouteInput, createLink, routeCodec)
import App.Run (Run, runComments, runId, runLookup, runScalarProperty, runValues)
import App.RunProperty (RunProperty, rpDescription, rpIsSortable, rpName)
import App.RunScalar (RunScalar(..))
import App.RunValue (RunValue(..), _Comments, runValueComments)
import App.SortOrder (SortOrder(..), comparing, invertOrder)
import App.UnfinishedComment (UnfinishedComment, _author, _text, emptyUnfinishedComment)
import DOM.HTML.Indexed.ScopeValue (ScopeValue(ScopeCol))
import Data.Array (head, mapMaybe, sortBy, (:))
import Data.Either (Either(..))
import Data.Lens (Iso', Traversal, Traversal', set, toArrayOf, toArrayOfOn, traversed, (.=), (^.))
import Data.Lens.Fold ((^..))
import Data.Lens.Index (ix)
import Data.Lens.Iso.Newtype (_Newtype)
import Data.Lens.Prism (Prism')
import Data.Lens.Record (prop)
import Data.Map (Map)
import Data.Maybe (Maybe(..), maybe)
import Data.Number.Format (precision, toStringWith)
import Data.Symbol (SProxy(..))
import Halogen (lift, liftAff)
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

commentsTable :: forall w i. Array Comment -> HH.HTML w i
commentsTable comments =
  let
    makeDeleteButton :: HH.HTML w i
    makeDeleteButton =
      HH.button
        [ classList [ "btn", "btn-link", "btn-sm", "p-0" ] ]
        [ faIcon "trash" ]

    makeRow comment =
      HH.tr_
        $ (HH.td_ [ makeDeleteButton ])
        : (plainTd <$> [ comment.created, comment.author, comment.text ])
  in
    HH.table
      [ classList [ "table" ] ]
      [ HH.thead_
          [ HH.tr_ (plainTh <$> [ "Actions", "Created", "Author", "Text" ])
          ]
      , HH.tbody_ (makeRow <$> comments)
      ]

commentsSection state =
  [ HH.h2_ [ HH.text "Comments" ]
  ]
    <> [ makeRequestResult state.commentRequest ]
    <> [ commentsTable (toArrayOf runComments state.run)
      , HH.h3_ [ HH.text "Add comment" ]
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
render state =
  HH.div [ singleClass "container" ]
    $ [ HH.h1_ [ HH.text ("Run " <> show state.runId) ] ]
    <> commentsSection state
