module App.Components.Sample where

import App.API (Crystal, Puck, SampleResponse, addPuck, removePuck, retrieveSample)
import App.AppMonad (AppMonad)
import App.Bootstrap (TableFlag(..), fluidContainer, plainH2_, plainH3_, table)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.Halogen.FontAwesome (icon)
import App.HalogenUtils (AlertType(..), classList, makeAlert, singleClass)
import App.Route (BeamlineRouteInput, Route(..), createLink)
import Control.Applicative (pure, when)
import Control.Apply ((<*>))
import Control.Bind (bind)
import Data.Array (elem, elemIndex, filter, head, mapMaybe, mapWithIndex, notElem, nub, null, singleton, (!!))
import Data.Either (Either(..))
import Data.Eq (class Eq, (/=), (==))
import Data.Foldable (maximum)
import Data.Function ((<<<))
import Data.Functor ((<$>))
import Data.HeytingAlgebra (not, (&&))
import Data.Int (fromString)
import Data.List (List(..), fromFoldable)
import Data.Maybe (Maybe(..), fromMaybe, maybe)
import Data.Ord (class Ord)
import Data.Ring (negate, (+))
import Data.Semigroup ((<>))
import Data.Show (show)
import Data.Unit (Unit, unit)
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
      , puckPosition: (negate 1)
      }
  }

component :: forall query output. H.Component HH.HTML query Unit output AppMonad
component = parentComponent fetchData childComponent

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  PuckIdChange puckId -> H.modify_ \state -> state { puckEdit = { puckId } }
  AddPuck -> do
    s <- H.get
    newPucks' <- H.lift (addPuck s.puckEdit.puckId)
    case newPucks' of
      Right { pucks: newPucks } -> H.modify_ \state -> state { puckEdit = { puckId: "" }, pucks = newPucks }
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
    table [ TableStriped ] ((HH.th_ <<< singleton <<< HH.text) <$> [ "Actions", "Puck ID", "Puck Type" ]) (makeRow <$> state.pucks)

crystalsTable :: forall w a. State -> HH.HTML w a
crystalsTable state =
  let
    makeRow crystal =
      HH.tr_
        [ HH.td_ [ HH.text crystal.crystalId ]
        , HH.td_ [ HH.text (fromMaybe "" crystal.puckId) ]
        , HH.td_ [ HH.text (maybe "" show crystal.puckPosition) ]
        ]
  in
    table [ TableStriped ] ((HH.th_ <<< singleton <<< HH.text) <$> [ "Crystal ID", "Puck ID", "Puck Position" ]) (makeRow <$> state.crystals)

puckIdTaken :: State -> Boolean
puckIdTaken state = state.puckEdit.puckId `elem` (_.puckId <$> state.pucks)

puckAddForm :: forall w. State -> HH.HTML w Action
puckAddForm state =
  HH.form [ singleClass "row mb-3" ]
    [ HH.div [ singleClass "col-6" ]
        [ HH.div [ singleClass "input-group" ]
            [ HH.input
                [ HP.type_ InputText
                , HP.placeholder "Puck ID"
                , singleClass "form-control"
                , HP.value state.puckEdit.puckId
                , HE.onValueInput (Just <<< PuckIdChange)
                ]
            , HH.button
                [ HP.disabled (puckIdTaken state)
                , HP.type_ ButtonButton
                , singleClass "btn btn-primary"
                , HE.onClick \_ -> Just AddPuck
                ]
                [ HH.text "Add" ]
            ]
        ]
    ]

render :: forall cs. State -> H.ComponentHTML Action cs AppMonad
render state =
  fluidContainer
    [ maybe (HH.text "") (makeAlert AlertDanger) state.errorMessage
    , HH.h2_ [ icon { name: "hockey-puck", size: Nothing, spin: false }, HH.text " Pucks" ]
    , HH.hr_
    , pucksTable state
    , plainH3_ "Add puck"
    , puckAddForm state
    , HH.h2 [ singleClass "mt-5" ] [ icon { name: "gem", size: Nothing, spin: false }, HH.text " Crystals" ]
    , HH.hr_
    , crystalsTable state
    ]
