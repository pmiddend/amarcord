module App.Components.Sample where

import App.API (Crystal, Puck, SampleResponse, addCrystal, addPuck, removeCrystal, removePuck, retrieveSample)
import App.AppMonad (AppMonad)
import App.Bootstrap (TableFlag(..), container, fluidContainer, plainH2_, plainH3_, table)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.Halogen.FontAwesome (icon)
import App.HalogenUtils (AlertType(..), classList, makeAlert, singleClass)
import App.Route (BeamlineRouteInput, Route(..), createLink)
import Control.Applicative (pure, when)
import Control.Apply ((<*>))
import Control.Bind (bind)
import Data.Array (elem, elemIndex, filter, head, mapMaybe, mapWithIndex, notElem, nub, null, singleton, (!!), (:))
import Data.BooleanAlgebra ((||))
import Data.Either (Either(..))
import Data.Eq (class Eq, (/=), (==))
import Data.Foldable (maximum)
import Data.Function ((<<<))
import Data.Functor ((<$>))
import Data.HeytingAlgebra (not, (&&))
import Data.Int (fromNumber, fromString)
import Data.List (List(..), fromFoldable)
import Data.Maybe (Maybe(..), fromMaybe, maybe)
import Data.Ord (class Ord)
import Data.Ring (negate, (+))
import Data.Semigroup ((<>))
import Data.Show (show)
import Data.Tuple (Tuple(..))
import Data.Unit (Unit, unit)
import Data.Void (absurd)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties (ButtonType(..), InputType(..))
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData, fromEither)
import Prelude (discard)
import Routing.Hash (setHash)
import Web.HTML (window)
import Web.HTML.Window (confirm)

data Action
  = PuckIdChange String
  | AddPuck
  | RemovePuck String
  | RemoveCrystal String
  | CrystalIdChange String
  | PuckIdInCrystalChange String
  | PuckPositionChange Int
  | AddCrystal

type PuckEdit
  = { puckId :: String
    }

type CrystalEdit
  = { crystalId :: String
    , puckId :: String
    , puckPosition :: Int
    }

type State
  = { pucks :: Array Puck
    , crystals :: Array Crystal
    , errorMessage :: Maybe String
    , puckEdit :: PuckEdit
    , crystalEdit :: CrystalEdit
    }

initialState :: ChildInput Unit SampleResponse -> State
initialState { remoteData: { crystals, pucks } } =
  { pucks
  , crystals
  , errorMessage: Nothing
  , puckEdit:
      { puckId: ""
      }
  , crystalEdit:
      { crystalId: ""
      , puckId: ""
      , puckPosition: 0
      }
  }

component :: forall query output. H.Component HH.HTML query Unit output AppMonad
component = parentComponent fetchData childComponent

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  PuckIdChange puckId -> H.modify_ \state -> state { puckEdit = { puckId } }
  CrystalIdChange newCrystalId -> H.modify_ \state -> state { crystalEdit = state.crystalEdit { crystalId = newCrystalId } }
  PuckIdInCrystalChange newPuckId -> H.modify_ \state -> state { crystalEdit = state.crystalEdit { puckId = newPuckId } }
  PuckPositionChange newPuckPosition -> H.modify_ \state -> state { crystalEdit = state.crystalEdit { puckPosition = newPuckPosition } }
  AddCrystal -> do
    s <- H.get
    let
      selectedPuck = if s.crystalEdit.puckId == "" then Nothing else Just (Tuple s.crystalEdit.puckId s.crystalEdit.puckPosition)
    newPucksAndCrystals <- H.lift (addCrystal s.crystalEdit.crystalId selectedPuck)
    case newPucksAndCrystals of
      Right { pucks: newPucks, crystals: newCrystals } ->
        H.modify_ \state ->
          state
            { crystalEdit =
              { crystalId: ""
              , puckId: fromMaybe "" (_.puckId <$> head s.pucks)
              , puckPosition: 1
              }
            , pucks = newPucks
            , crystals = newCrystals
            }
      Left e -> H.modify_ \state -> state { errorMessage = Just e }
  AddPuck -> do
    s <- H.get
    newPucks' <- H.lift (addPuck s.puckEdit.puckId)
    case newPucks' of
      Right { pucks: newPucks } -> H.modify_ \state -> state { puckEdit = { puckId: "" }, pucks = newPucks }
      Left e -> H.modify_ \state -> state { errorMessage = Just e }
  RemoveCrystal crystalId -> do
    w <- H.liftEffect window
    confirmResult <- H.liftEffect (confirm "Really delete this crystal?" w)
    when confirmResult do
      newPucksAndCrystals <- H.lift (removeCrystal crystalId)
      case newPucksAndCrystals of
        Right { crystals: newCrystals, pucks: newPucks } ->
          H.modify_ \state ->
            state
              { pucks = newPucks
              , crystals = newCrystals
              }
        Left e -> H.modify_ \state -> state { errorMessage = Just e }
  RemovePuck puckId -> do
    w <- H.liftEffect window
    confirmResult <- H.liftEffect (confirm "Really delete this puck?" w)
    when confirmResult do
      newPucksAndCrystals <- H.lift (removePuck puckId)
      case newPucksAndCrystals of
        Right { pucks: newPucks, crystals: newCrystals } ->
          H.modify_ \state ->
            state
              { pucks = newPucks
              , crystals = newCrystals
              }
        Left e -> H.modify_ \state -> state { errorMessage = Just e }

childComponent :: forall q. H.Component HH.HTML q (ChildInput Unit SampleResponse) ParentError AppMonad
childComponent =
  H.mkComponent
    { initialState
    , render
    , eval: H.mkEval H.defaultEval { handleAction = handleAction }
    }

fetchData :: Unit -> AppMonad (RemoteData String SampleResponse)
fetchData _ = fromEither <$> retrieveSample

pucksTable :: forall w. State -> HH.HTML w Action
pucksTable state =
  let
    removeButton puckId =
      HH.button
        [ HP.type_ ButtonButton
        , classList [ "btn", "btn-danger", "btn-sm" ]
        , HE.onClick \_ -> Just (RemovePuck puckId)
        ]
        [ icon { name: "trash", size: Nothing, spin: false }, HH.text " Remove" ]

    makeRow puck =
      HH.tr_
        [ HH.td_ [ removeButton puck.puckId ]
        , HH.td_ [ HH.text puck.puckId ]
        , HH.td_ [ HH.text puck.puckType ]
        ]
  in
    table
      "pucks-table"
      [ TableStriped ]
      ((HH.th_ <<< singleton <<< HH.text) <$> [ "Actions", "Puck ID", "Puck Type" ])
      (makeRow <$> state.pucks)

crystalsTable :: forall w. State -> HH.HTML w Action
crystalsTable state =
  let
    removeButton crystalId =
      HH.button
        [ HP.type_ ButtonButton
        , classList [ "btn", "btn-danger", "btn-sm" ]
        , HE.onClick \_ -> Just (RemoveCrystal crystalId)
        ]
        [ icon { name: "trash", size: Nothing, spin: false }, HH.text " Remove" ]

    makeRow crystal =
      HH.tr_
        [ HH.td_ [ removeButton crystal.crystalId ]
        , HH.td_ [ HH.text crystal.crystalId ]
        , HH.td_ [ HH.text (fromMaybe "" crystal.puckId) ]
        , HH.td_ [ HH.text (maybe "" show crystal.puckPosition) ]
        ]
  in
    table
      "crystals-table"
      [ TableStriped ]
      ((HH.th_ <<< singleton <<< HH.text) <$> [ "Actions", "Crystal ID", "Puck ID", "Puck Position" ])
      (makeRow <$> state.crystals)

puckIdTaken :: State -> Boolean
puckIdTaken state = state.puckEdit.puckId `elem` (_.puckId <$> state.pucks)

crystalInputValid :: State -> Boolean
crystalInputValid s =
  s.crystalEdit.crystalId /= ""
    && s.crystalEdit.crystalId `notElem` (_.crystalId <$> s.crystals)
    && (s.crystalEdit.puckId == "" || s.crystalEdit.puckId `elem` (_.puckId <$> s.pucks))

crystalAddForm :: forall w. State -> HH.HTML w Action
crystalAddForm state =
  let
    makeOption puck =
      HH.option
        [ HP.value puck.puckId
        , HP.selected (state.crystalEdit.puckId == puck.puckId)
        ]
        [ HH.text puck.puckId ]

    noPuckOption = HH.option [ HP.value "", HP.selected (state.crystalEdit.puckId == "") ] [ HH.text "no puck" ]
  in
    HH.form [ singleClass "mb-5" ]
      [ HH.div [ singleClass "mb-3" ]
          [ HH.label [ HP.for "input-crystal-id", singleClass "form-label" ] [ HH.text "Crystal ID" ]
          , HH.input
              [ HP.type_ InputText
              , HP.placeholder "Crystal ID"
              , HP.id_ "input-crystal-id"
              , singleClass "form-control"
              , HP.value state.crystalEdit.crystalId
              , HE.onValueInput (Just <<< CrystalIdChange)
              ]
          ]
      , HH.div [ singleClass "mb-3" ]
          [ HH.label [ HP.for "input-puck-id", singleClass "form-label" ] [ HH.text "Puck ID" ]
          , HH.select
              [ singleClass "form-select"
              , HE.onValueChange (Just <<< PuckIdInCrystalChange)
              , HP.id_ "crystal-puck-id"
              ]
              (noPuckOption : (makeOption <$> state.pucks))
          ]
      , HH.div [ singleClass "mb-3" ]
          [ HH.label [ HP.for "input-puck-position", singleClass "form-label" ] [ HH.text "Puck Position" ]
          , HH.input
              [ HP.type_ InputNumber
              , HP.placeholder "Puck Position"
              , HP.id_ "input-puck-position"
              , singleClass "form-control"
              , HP.value (show state.crystalEdit.puckPosition)
              , HE.onValueInput (\x -> PuckPositionChange <$> fromString x)
              ]
          , HH.div [ singleClass "form-text" ] [ HH.text "only important if a puck is selected" ]
          ]
      , HH.button
          [ HP.disabled (not (crystalInputValid state))
          , HP.type_ ButtonButton
          , singleClass "btn btn-primary"
          , HP.id_ "crystal-add-button"
          , HE.onClick \_ -> Just AddCrystal
          ]
          [ icon { name: "plus", size: Nothing, spin: false }, HH.text " Add" ]
      ]

puckAddForm :: forall w. State -> HH.HTML w Action
puckAddForm state =
  HH.form [ singleClass "row mb-5" ]
    [ HH.div [ singleClass "col-6" ]
        [ HH.div [ singleClass "input-group" ]
            [ HH.input
                [ HP.type_ InputText
                , HP.placeholder "Puck ID"
                , singleClass "form-control"
                , HP.value state.puckEdit.puckId
                , HE.onValueInput (Just <<< PuckIdChange)
                , HP.id_ "puck-add-puck-id"
                ]
            , HH.button
                [ HP.disabled (puckIdTaken state)
                , HP.type_ ButtonButton
                , HP.id_ "puck-add-add-button"
                , singleClass "btn btn-primary"
                , HE.onClick \_ -> Just AddPuck
                ]
                [ icon { name: "plus", size: Nothing, spin: false }, HH.text " Add" ]
            ]
        ]
    ]

render :: forall cs. State -> H.ComponentHTML Action cs AppMonad
render state =
  container
    [ maybe (HH.text "") (makeAlert AlertDanger) state.errorMessage
    , HH.h2_ [ icon { name: "hockey-puck", size: Nothing, spin: false }, HH.text " Pucks" ]
    , HH.hr_
    , plainH3_ "Add puck"
    , puckAddForm state
    , pucksTable state
    , HH.h2 [ singleClass "mt-5" ] [ icon { name: "gem", size: Nothing, spin: false }, HH.text " Crystals" ]
    , HH.hr_
    , plainH3_ "Add crystal"
    , crystalAddForm state
    , crystalsTable state
    ]
