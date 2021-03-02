module App.Hits where

import Prelude
import App.API (graphqlMutation, graphqlQuery)
import App.AmarQL.Enum.HitMethod (HitMethod)
import App.AmarQL.Enum.Interesting (Interesting)
import App.AmarQL.InputObject (HitInput(..))
import App.AmarQL.Mutation as Mutation
import App.AmarQL.Object.Crystal as Crystal
import App.AmarQL.Object.Hit as Hit
import App.AmarQL.Object.RemoveHit as RemoveHit
import App.AmarQL.Object.UpsertHit as UpsertHit
import App.AmarQL.Query (hits, crystals)
import App.AmarQL.Scalars (DateTime, Id)
import App.AmarQL.Scopes (Scope__Crystal, Scope__Hit, Scope__RemoveHit, Scope__UpsertHit)
import App.AppMonad (AppMonad, runAppMonad)
import App.Autocomplete as Autocomplete
import App.Config (baseUrl)
import App.HalogenUtils (makeRequestResult, messageFromResult, scope)
import App.Lenses (_requestResult)
import App.ParentComponent (ParentError, parentComponent)
import DOM.HTML.Indexed.ButtonType (ButtonType(ButtonButton))
import DOM.HTML.Indexed.InputType (InputType(InputText))
import DOM.HTML.Indexed.ScopeValue (ScopeValue(ScopeCol))
import Data.Bifunctor (lmap)
import Data.Either (Either(..), either)
import Data.Lens (Lens', lens, set, use, view, (.=))
import Data.Lens.Iso.Newtype (_Newtype)
import Data.Lens.Record (prop)
import Data.Maybe (Maybe(..), fromMaybe)
import Data.Symbol (SProxy(..))
import Effect.Aff (Aff)
import GraphQLClient (Optional(..), SelectionSet, printGraphQLError)
import Halogen (lift)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData(..), fromEither, isSuccess)

_method :: Lens' HitInput String
_method = _Newtype <<< prop (SProxy :: SProxy "method")

_comment :: Lens' HitInput String
_comment = _Newtype <<< prop (SProxy :: SProxy "comment")

toFromBoolean :: Lens' Boolean String
toFromBoolean = lens toString fromString
  where
  fromString _ "true" = true

  fromString _ _ = false

  toString true = "true"

  toString _ = "false"

_isInteresting :: Lens' HitInput String
_isInteresting = _Newtype <<< prop (SProxy :: SProxy "isInteresting") <<< toFromBoolean

_inputRefinementId :: Lens' State (Maybe String)
_inputRefinementId = prop (SProxy :: SProxy "inputRefinementId")

type Hit
  = { hitId :: Id
    , refinementId :: Int
    , method :: HitMethod
    , comment :: Maybe String
    , isInteresting :: Maybe Interesting
    }

type TableRows
  = Array Hit

type State
  = { rows :: TableRows
    , inputRow :: HitInput
    , inputRefinementId :: Maybe String
    , requestResult :: RemoteData String String
    }

_inputRow :: Lens' State HitInput
_inputRow = prop (SProxy :: SProxy "inputRow")

data Action
  = AddEntry
  | HandleEdit HitInput
  | HandleRefinementChange (Maybe String)
  | HandleDelete Id

initialTableRow :: TableRows -> HitInput
initialTableRow rows =
  HitInput
    { hitId: Absent
    , refinementId: ""
    , method: "manual"
    , comment: ""
    , bindingSite: ""
    , confidence: "low"
    , isInteresting: false
    , inspectedBy: ""
    , metadataColumn: ""
    }

initialState :: TableRows -> State
initialState rows = { rows, inputRow: initialTableRow rows, inputRefinementId: Nothing, requestResult: NotAsked }

component :: forall t333 t334 t335. H.Component HH.HTML t334 t335 t333 AppMonad
component = parentComponent fetchRows childComponent

childComponent :: forall q. H.Component HH.HTML q TableRows ParentError AppMonad
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

type CrystalRow
  = { value :: String, text :: String, created :: Maybe DateTime }

crystalsRowSelection :: SelectionSet Scope__Crystal CrystalRow
crystalsRowSelection = (\x y -> { value: x, text: x, created: y }) <$> Crystal.crystalId <*> Crystal.created

hitsRowSelection :: SelectionSet Scope__Hit Hit
hitsRowSelection = ({ hitId: _, refinementId: _, method: _, comment: _, isInteresting: _ }) <$> Hit.hitId <*> Hit.refinementId <*> Hit.method <*> Hit.comment <*> Hit.isInteresting

resultSelectionUpsert :: SelectionSet Scope__UpsertHit { errorMessage :: Maybe String }
resultSelectionUpsert = { errorMessage: _ } <$> UpsertHit.errorMessage

resultSelectionRemove :: SelectionSet Scope__RemoveHit { errorMessage :: Maybe String }
resultSelectionRemove = { errorMessage: _ } <$> RemoveHit.errorMessage

mutateAndReload :: forall slots action. AppMonad (RemoteData String String) -> H.HalogenM State action slots ParentError AppMonad (Maybe State)
mutateAndReload action = do
  result <- lift action
  if isSuccess result then do
    rowResult <- lift fetchRowsRaw
    case rowResult of
      Left e -> do
        H.raise e
        pure Nothing
      Right newRows -> do
        currentState <- H.get
        pure (Just (currentState { requestResult = result, rows = newRows }))
  else do
    _requestResult .= result
    pure Nothing

fetchCrystalsRaw :: String -> AppMonad (Either String (Array CrystalRow))
fetchCrystalsRaw query = lmap printGraphQLError <$> graphqlQuery (crystals { crystalId: Present ("%" <> query <> "%") } crystalsRowSelection)

fetchCrystals :: String -> AppMonad (Array CrystalRow)
fetchCrystals query = either (const []) identity <$> fetchCrystalsRaw query

fetchCrystalsEffect :: String -> Aff (Array CrystalRow)
fetchCrystalsEffect q = runAppMonad { baseUrl } (fetchCrystals q)

fetchRowsRaw :: AppMonad (Either String TableRows)
fetchRowsRaw = lmap printGraphQLError <$> graphqlQuery (hits hitsRowSelection)

pseudoRow :: String -> CrystalRow
pseudoRow refinementId = { value: refinementId, text: refinementId, created: Nothing }

formatCrystals :: CrystalRow -> Autocomplete.FormatResult
formatCrystals { value, text, created } = { id: value, text: text, html: text <> " <em>" <> show created <> "</em>" }

fetchRows :: AppMonad (RemoteData String TableRows)
fetchRows = fromEither <$> fetchRowsRaw

upsert :: HitInput -> AppMonad (RemoteData String String)
upsert row = messageFromResult "Add hit" <$> graphqlMutation (Mutation.upsertHit { "data": row } resultSelectionUpsert)

remove :: Int -> AppMonad (RemoteData String String)
remove i = messageFromResult ("Remove entry with ID “" <> show i <> "”") <$> graphqlMutation (Mutation.removeHit { entityId: i } resultSelectionRemove)

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  AddEntry -> do
    row <- use _inputRow
    newState <- mutateAndReload (upsert row)
    case newState of
      Nothing -> pure unit
      Just newState' -> H.put newState'
  HandleRefinementChange e -> _inputRefinementId .= e
  HandleEdit e -> _inputRow .= e
  HandleDelete i -> do
    pure unit

render :: State -> H.ComponentHTML Action ( refinementComplete :: forall query. H.Slot query Autocomplete.Output Int ) AppMonad
render state =
  let
    makeRow :: forall w. Hit -> HH.HTML w Action
    makeRow row =
      HH.tr_
        [ HH.td_ [ HH.text (show row.hitId) ]
        , HH.td_ [ HH.text (show row.refinementId) ]
        , HH.td_ [ HH.text (show row.method) ]
        , HH.td_ [ HH.text (fromMaybe "" row.comment) ]
        , HH.td_ [ HH.text (show row.isInteresting) ]
        , HH.td_ [ HH.button [ HP.type_ ButtonButton, HP.classes [ HH.ClassName "btn", HH.ClassName "btn-danger" ], HE.onClick \_ -> Just (HandleDelete row.hitId) ] [ HH.text "Remove" ] ]
        ]

    textInput :: forall w. String -> Lens' HitInput String -> HH.HTML w Action
    textInput name lens =
      HH.div [ HP.classes [ HH.ClassName "mb-3" ] ]
        [ HH.label [ HP.for ("input" <> name), HP.classes [ HH.ClassName "form-label" ] ]
            [ HH.text name
            ]
        , HH.input
            [ HP.type_ InputText
            , HP.id_ ("input" <> name)
            , HP.classes [ HH.ClassName "form-control" ]
            , HP.value (view lens state.inputRow)
            , HE.onValueInput \str -> Just (HandleEdit (set lens str (state.inputRow)))
            ]
        ]

    addNewForm =
      [ HH.form_
          [ HH.div [ HP.classes [ HH.ClassName "mb-3" ] ]
              [ HH.label [ HP.for ("inputRefinement"), HP.classes [ HH.ClassName "form-label" ] ]
                  [ HH.text "Refinement"
                  ]
              , HH.slot (SProxy :: SProxy "refinementComplete") 0 Autocomplete.component
                  { elementId: "inputRefinement"
                  , currentValue: view _inputRefinementId state
                  , searchCallback: fetchCrystalsEffect
                  , formatCallback: formatCrystals
                  , placeholder: "Type to search..."
                  } \(Autocomplete.Change output) -> Just (HandleRefinementChange output)
              ]
          , textInput "Method" _method
          , textInput "Comment" _comment
          , textInput "Interesting" _isInteresting
          , HH.button
              [ HP.type_ ButtonButton, HP.classes [ HH.ClassName "btn", HH.ClassName "btn-primary" ], HE.onClick \_ -> Just AddEntry ]
              [ HH.text "Add" ]
          ]
      ]

    makeHeader t = HH.th [ scope ScopeCol ] [ HH.text t ]
  in
    HH.div [ HP.classes [ HH.ClassName "container" ] ]
      ( [ HH.h1_ [ HH.text "Hits" ]
        , makeRequestResult (state.requestResult)
        , HH.table
            [ HP.classes [ HH.ClassName "table" ] ]
            [ HH.thead_
                [ HH.tr_ (makeHeader <$> [ "ID", "Refinement", "Method", "Comment", "Interesting?", "Actions" ])
                ]
            , HH.tbody_ (makeRow <$> state.rows)
            ]
        , HH.h2_ [ HH.text "Add new hit" ]
        ]
          <> addNewForm
      )
