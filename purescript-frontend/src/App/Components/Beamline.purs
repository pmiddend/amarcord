module App.Components.Beamline where

import App.API (DewarEntry, DewarResponse, DiffractionEntry, DiffractionResponse, Puck, PucksResponse, addDiffraction, addPuckToTable, removeSingleDewarEntry, removeWholeTable, retrieveDewarTable, retrieveDiffractions, retrievePucks)
import App.AppMonad (AppMonad)
import App.Bootstrap (TableFlag(..), fluidContainer, plainH2_, plainH3_, table)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.Halogen.FontAwesome (icon)
import App.HalogenUtils (AlertType(..), classList, makeAlert, singleClass)
import App.Route (BeamlineRouteInput, Route(..), createLink)
import Control.Applicative (pure, when)
import Control.Apply ((<*>))
import Control.Bind (bind)
import Data.Array (elem, elemIndex, filter, head, mapMaybe, mapWithIndex, notElem, nub, null, (!!))
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
import Data.Ring ((+))
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

type DewarEdit
  = { editDewarPosition :: Int
    , editPuckId :: String
    }

type State
  = { pucks :: Array Puck
    , dewarTable :: Array DewarEntry
    , diffractions :: Array DiffractionEntry
    , errorMessage :: Maybe String
    , dewarEdit :: DewarEdit
    , diffractionPuckId :: String
    , diffractionCrystalId :: String
    , diffractionRunId :: Int
    , diffractionOutcome :: String
    , diffractionBeamIntensity :: String
    , diffractionComment :: String
    }

type StateChanger
  = State -> State

data BeamlineData
  = BeamlineData PucksResponse DewarResponse DiffractionResponse

derive instance eqAssociatedTable :: Eq BeamlineData

derive instance ordAssociatedTable :: Ord BeamlineData

data Action
  = Initialize
  | RemoveDewar Int String
  | AddDewar
  | DiffractionStateChange StateChanger
  | PuckIdChange String
  | DiffractionPuckIdChange String
  | DiffractionCrystalIdChange String
  | DiffractionRunIdChange Int
  | DewarPositionChange Int
  | WipeDewarTable
  | AddDiffraction

firstMissing :: List Int -> Int
firstMissing Nil = 1

firstMissing (Cons 2 _) = 1

firstMissing xs = firstMissing' xs
  where
  firstMissing' Nil = 1

  firstMissing' (Cons x r@(Cons y ys)) = if x + 1 == y then firstMissing' r else x + 1

  firstMissing' (Cons x Nil) = x + 1

choosePuck :: Array Puck -> Array DewarEntry -> Maybe Puck
choosePuck pucks dt =
  let
    takenPucks = _.puckId <$> dt
  in
    head (filter (not <<< (\x -> x.puckId `elem` takenPucks)) pucks)

chooseDewarEdit :: Array Puck -> Array DewarEntry -> DewarEdit
chooseDewarEdit pucks dt =
  { editDewarPosition: firstMissing (_.dewarPosition <$> (fromFoldable dt))
  , editPuckId: (fromMaybe { puckId: "", puckType: "UNI" } (choosePuck pucks dt)).puckId
  }

initialState :: ChildInput BeamlineRouteInput BeamlineData -> State
initialState { input: { puckId }
, remoteData: BeamlineData pucksResponse dewarResponse diffractionsResponse
} =
  { pucks: pucksResponse.pucks
  , dewarTable: dewarResponse.dewarTable
  , diffractions: diffractionsResponse.diffractions
  , errorMessage: Nothing
  , dewarEdit: chooseDewarEdit pucksResponse.pucks dewarResponse.dewarTable
  , diffractionCrystalId: fromMaybe "" (head (_.crystalId <$> diffractionsResponse.diffractions))
  , diffractionRunId: 1
  , diffractionOutcome: "success"
  , diffractionBeamIntensity: ""
  , diffractionComment: ""
  , diffractionPuckId:
      case puckId of
        Nothing -> ""
        Just puckId' -> if puckId' `elem` (_.puckId <$> dewarResponse.dewarTable) then puckId' else ""
  }

component :: forall query output. H.Component HH.HTML query BeamlineRouteInput output AppMonad
component = parentComponent fetchData childComponent

childComponent :: forall q. H.Component HH.HTML q (ChildInput BeamlineRouteInput BeamlineData) ParentError AppMonad
childComponent =
  H.mkComponent
    { initialState
    , render
    , eval:
        H.mkEval
          H.defaultEval
            { handleAction = handleAction
            , initialize = Just Initialize
            }
    }

handleDewarResponse = case _ of
  Right newTable -> do
    H.modify_ \state ->
      state
        { dewarTable = newTable.dewarTable
        , dewarEdit = chooseDewarEdit state.pucks newTable.dewarTable
        , diffractionPuckId = if state.diffractionPuckId `elem` (_.puckId <$> newTable.dewarTable) then state.diffractionPuckId else ""
        }
  Left e -> H.modify_ \state -> state { errorMessage = Just e }

updateHash :: forall slots. H.HalogenM State Action slots ParentError AppMonad Unit
updateHash = do
  oldState <- H.get
  H.liftEffect
    ( setHash
        ( createLink
            ( Beamline
                { puckId: if oldState.diffractionPuckId == "" then Nothing else Just oldState.diffractionPuckId
                }
            )
        )
    )

selectRunId :: String -> Array DiffractionEntry -> Int
selectRunId crystalId diffs = maybe 1 (\x -> x + 1) (maximum (mapMaybe _.runId (filter (\x -> x.crystalId == crystalId) diffs)))

nextCrystalId currentId diffractions =
  let
    crystalIds = nub (_.crystalId <$> diffractions)

    currentIndex = elemIndex currentId crystalIds
  in
    case currentIndex of
      Nothing -> currentId
      Just i -> case crystalIds !! (i + 1) of
        Nothing -> currentId
        Just x -> x

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  Initialize -> pure unit
  DiffractionCrystalIdChange newCrystalId -> do
    H.modify_ \state ->
      state
        { diffractionCrystalId = newCrystalId
        , diffractionRunId = selectRunId newCrystalId state.diffractions
        }
  DiffractionStateChange changer -> H.modify_ \state -> changer state
  AddDiffraction -> do
    w <- H.liftEffect window
    confirmResult <- H.liftEffect (confirm "Really add diffraction?" w)
    if confirmResult then do
      s <- H.get
      let
        newDiffraction =
          { crystalId: s.diffractionCrystalId
          , runId: s.diffractionRunId
          , diffraction: s.diffractionOutcome
          , beamIntensity: s.diffractionBeamIntensity
          , comment: s.diffractionComment
          }
      diffs <- H.lift (addDiffraction newDiffraction s.diffractionPuckId)
      case diffs of
        Right newDiffractions -> do
          H.modify_ \state ->
            let
              newCrystalId = nextCrystalId state.diffractionCrystalId state.diffractions
            in
              state
                { diffractions = newDiffractions.diffractions
                , diffractionRunId = selectRunId newCrystalId newDiffractions.diffractions
                , diffractionCrystalId = newCrystalId
                , diffractionComment = ""
                }
        Left e -> H.modify_ \state -> state { errorMessage = Just e }
    else
      pure unit
  DiffractionRunIdChange newCrystalId -> pure unit
  DiffractionPuckIdChange newPuckId -> do
    diffs <- H.lift (retrieveDiffractions newPuckId)
    case diffs of
      Right newDiffractions -> do
        H.modify_ \state ->
          let
            newCrystalId = fromMaybe "" (head (_.crystalId <$> newDiffractions.diffractions))
          in
            state
              { diffractionPuckId = newPuckId
              , diffractions = newDiffractions.diffractions
              , diffractionCrystalId = newCrystalId
              , diffractionRunId = selectRunId newCrystalId newDiffractions.diffractions
              }
        updateHash
      Left e -> H.modify_ \state -> state { errorMessage = Just e }
  PuckIdChange newPuckId -> H.modify_ \state -> state { dewarEdit = state.dewarEdit { editPuckId = newPuckId } }
  WipeDewarTable -> do
    w <- H.liftEffect window
    confirmResult <- H.liftEffect (confirm "Really delete all entries?" w)
    when confirmResult do
      result <- H.lift removeWholeTable
      handleDewarResponse result
      updateHash
  DewarPositionChange newDewarPosition ->
    H.modify_ \state ->
      state
        { dewarEdit = state.dewarEdit { editDewarPosition = newDewarPosition }
        }
  AddDewar -> do
    s <- H.get
    result <- H.lift (addPuckToTable s.dewarEdit.editDewarPosition s.dewarEdit.editPuckId)
    handleDewarResponse result
  RemoveDewar dewarPosition puckId -> do
    result <- H.lift (removeSingleDewarEntry dewarPosition)
    handleDewarResponse result
    updateHash

fetchData :: BeamlineRouteInput -> AppMonad (RemoteData String BeamlineData)
fetchData { puckId } = do
  pucks <- retrievePucks
  dewarTable' <- retrieveDewarTable
  let
    applyDiffractions :: Maybe String -> AppMonad (Either String DiffractionResponse)
    applyDiffractions pid = case pid of
      Nothing -> pure (Right { diffractions: [] })
      Just dewPos' -> case pid of
        Nothing -> pure (Right { diffractions: [] })
        Just pid' -> retrieveDiffractions pid'
  diffractions <- applyDiffractions puckId
  let
    result :: Either String BeamlineData
    result = BeamlineData <$> pucks <*> dewarTable' <*> diffractions
  pure (fromEither result)

dewarTable :: forall w. State -> HH.HTML w Action
dewarTable state =
  let
    makeRow { puckId, dewarPosition } =
      HH.tr [ classList (if puckId == state.diffractionPuckId then [ "table-info" ] else []) ]
        [ HH.td_
            [ HH.button
                [ HP.type_ ButtonButton
                , classList [ "btn", "btn-danger", "btn-sm" ]
                , HE.onClick \_ -> Just (RemoveDewar dewarPosition puckId)
                ]
                [ icon { name: "trash", size: Nothing, spin: false }, HH.text " Remove" ]
            ]
        , HH.td_ [ HH.text (show dewarPosition) ]
        , HH.td_ [ HH.a [ HP.href (createLink (Beamline { puckId: Just puckId })), HE.onClick \_ -> Just (DiffractionPuckIdChange puckId) ] [ HH.text puckId ] ]
        ]
  in
    table "dewar-table" [ TableStriped ]
      [ HH.th_ [ HH.text "Actions" ]
      , HH.th_ [ HH.text "Position" ]
      , HH.th_ [ HH.text "Puck ID" ]
      ]
      (makeRow <$> state.dewarTable)

dewarForm :: forall w. State -> HH.HTML w Action
dewarForm state =
  let
    makeOption puck =
      HH.option
        [ HP.value puck.puckId
        , HP.selected (state.dewarEdit.editPuckId == puck.puckId)
        ]
        [ HH.text puck.puckId ]

    takenPuckIds = _.puckId <$> state.dewarTable

    takenPositions = _.dewarPosition <$> state.dewarTable

    addEnabled = state.dewarEdit.editDewarPosition `notElem` takenPositions && state.dewarEdit.editPuckId /= ""

    availablePucks = filter (\x -> x.puckId `notElem` takenPuckIds) state.pucks

    pucksAvailable = not (null availablePucks)

    puckSelector =
      HH.select
        [ classList [ "form-select" ]
        , HE.onValueChange (Just <<< PuckIdChange)
        , HP.id_ "puck-id"
        ]
        (makeOption <$> availablePucks)
  in
    if pucksAvailable then
      HH.form [ singleClass "row align-items-center" ]
        [ HH.div [ singleClass "col-auto" ]
            [ HH.div [ singleClass "form-floating" ]
                [ HH.input
                    [ HP.type_ InputNumber
                    , HP.id_ "dewar-position"
                    , HP.placeholder "Dewar position"
                    , classList [ "form-control" ]
                    , HP.value (show state.dewarEdit.editDewarPosition)
                    , HE.onValueChange (\x -> DewarPositionChange <$> fromString x)
                    ]
                , HH.label [ HP.for "dewar-position" ] [ HH.text "Dewar position" ]
                ]
            ]
        , HH.div [ singleClass "col-auto" ]
            [ HH.div [ singleClass "form-floating" ] [ puckSelector, HH.label [ HP.for "puck-id" ] [ HH.text "Puck ID" ] ]
            ]
        , HH.div [ singleClass "col-auto" ]
            [ HH.button
                [ HP.type_ ButtonButton
                , classList [ "btn", "btn-primary", "ml-2" ]
                , HE.onClick \_ -> Just AddDewar
                , HP.id_ "add-dewar-button"
                , HP.enabled addEnabled
                ]
                [ icon { name: "plus", size: Nothing, spin: false }, HH.text " Add" ]
            ]
        ]
    else
      HH.p [ singleClass "text-muted" ] [ HH.text "No pucks/positions available to add to the table." ]

diffractionForm :: forall w. State -> HH.HTML w Action
diffractionForm state =
  let
    pucksInDewar :: Array String
    pucksInDewar = _.puckId <$> state.dewarTable

    pucksAvailable = not (null pucksInDewar)

    formatOutcome (Just "success") = HH.span [ singleClass "badge rounded-pill bg-success" ] [ HH.text "success" ]

    formatOutcome (Just "no diffraction") = HH.span [ singleClass "badge rounded-pill bg-warning text-dark" ] [ HH.text "no diffraction" ]

    formatOutcome (Just "ice / salt") = HH.span [ singleClass "badge rounded-pill bg-warning text-dark" ] [ HH.text "ice/salt" ]

    formatOutcome (Just "no crystal") = HH.span [ singleClass "badge rounded-pill bg-warning text-dark" ] [ HH.text "no crystal" ]

    formatOutcome _ = HH.text ""

    headers = [ "Actions", "Crystal ID", "Run ID", "Puck Pos", "Diffraction", "Comment" ]

    makeRow :: DiffractionEntry -> HH.HTML w Action
    makeRow diff =
      HH.tr [ classList (if diff.crystalId == state.diffractionCrystalId then [ "table-info" ] else []) ]
        [ HH.td_
            [ HH.button
                [ HP.type_ ButtonButton
                , classList [ "btn", "btn-secondary", "btn-sm" ]
                , HE.onClick \_ -> Just (DiffractionCrystalIdChange diff.crystalId)
                ]
                [ icon { name: "plus-square", size: Nothing, spin: false }, HH.text "Collect" ]
            ]
        , HH.td_ [ HH.text diff.crystalId ]
        , HH.td_ [ HH.text (maybe "" show diff.runId) ]
        , HH.td_ [ HH.text (show diff.puckPositionId) ]
        , HH.td_ [ formatOutcome diff.diffraction ]
        , HH.td_ [ HH.text (fromMaybe "" diff.comment) ]
        ]

    diffractionTable =
      table
        "diffractions-table"
        [ TableStriped ]
        ((\x -> HH.th [ singleClass "text-nowrap" ] [ HH.text x ]) <$> headers)
        (makeRow <$> state.diffractions)

    makeCrystalOption crystalId =
      HH.option
        [ HP.value crystalId
        , HP.selected (state.diffractionCrystalId == crystalId)
        ]
        [ HH.text crystalId ]

    makeOutcomeOption outcome =
      HH.option
        [ HP.value outcome
        , HP.selected (state.diffractionOutcome == outcome)
        ]
        [ HH.text outcome ]

    crystalIdSelect =
      HH.select
        [ classList [ "form-select" ]
        , HE.onValueChange (Just <<< DiffractionCrystalIdChange)
        , HP.id_ "diffraction-crystal-id"
        ]
        (makeCrystalOption <$> (_.crystalId <$> state.diffractions))

    makeRadio idx option =
      HH.div [ singleClass "form-check" ]
        [ HH.input
            [ singleClass "form-check-input"
            , HP.type_ InputRadio
            , HP.name "diffraction-outcome"
            , HP.id_ ("diffraction-outcome" <> show idx)
            , HP.checked (state.diffractionOutcome == option)
            , HE.onValueChange (\_ -> Just (DiffractionStateChange (\state' -> state' { diffractionOutcome = option })))
            ]
        , HH.label [ HP.for ("diffraction-outcome" <> show idx), singleClass "form-check-label" ] [ HH.text option ]
        ]

    makeRadios options = mapWithIndex makeRadio options

    addDiffractionForm =
      HH.form_
        [ HH.div [ singleClass "form-group" ]
            [ HH.label [ HP.for "diffraction-crystal-id", singleClass "form-label" ] [ HH.text "Crystal ID" ]
            , crystalIdSelect
            ]
        , HH.div [ singleClass "form-group" ]
            [ HH.label [ HP.for "diffraction-run-id", singleClass "form-label" ] [ HH.text "Run ID" ]
            , HH.input
                [ HP.type_ InputNumber
                , HP.id_ "diffraction-run-id"
                , classList [ "form-control", "mr-2" ]
                , HP.value (show state.diffractionRunId)
                , HE.onValueChange (\x -> DiffractionRunIdChange <$> fromString x)
                ]
            ]
        , HH.div [ singleClass "form-group" ]
            ([ HH.label [ HP.for "diffraction-outcome", singleClass "form-label" ] [ HH.text "Outcome" ] ] <> makeRadios [ "success", "no diffraction", "no crystal", "ice / salt" ])
        , HH.div [ singleClass "form-group" ]
            [ HH.label [ HP.for "diffraction-beam-intensity", singleClass "form-label" ] [ HH.text "Beam intensity" ]
            , HH.input
                [ HP.type_ InputText
                , HP.id_ "diffraction-beam-intensity"
                , classList [ "form-control", "mr-2" ]
                , HP.value state.diffractionBeamIntensity
                , HE.onValueChange (\x -> Just (DiffractionStateChange (\state' -> state' { diffractionBeamIntensity = x })))
                ]
            ]
        , HH.div [ singleClass "form-group", singleClass "form-label" ]
            [ HH.label [ HP.for "diffraction-comment" ] [ HH.text "Comment" ]
            , HH.input
                [ HP.type_ InputText
                , HP.id_ "diffraction-comment"
                , classList [ "form-control", "mr-2" ]
                , HP.value state.diffractionComment
                , HE.onValueChange (\x -> Just (DiffractionStateChange (\state' -> state' { diffractionComment = x })))
                ]
            ]
        , HH.button
            [ HP.type_ ButtonButton
            , classList [ "btn", "btn-primary", "mt-2", "mb-2" ]
            , HE.onClick \_ -> Just AddDiffraction
            , HP.id_ "diffraction-add-button"
            ]
            [ icon { name: "plus", size: Nothing, spin: false }, HH.text " Add diffraction run" ]
        ]
  in
    if pucksAvailable then
      HH.div_
        [ if state.diffractionPuckId /= "" then
            HH.div [ singleClass "row mt-3" ]
              [ HH.div [ singleClass "col" ] [ diffractionTable ]
              , HH.div [ singleClass "col" ]
                  [ HH.div [ singleClass "sticky-top" ]
                      [ plainH3_ "Add diffraction"
                      , addDiffractionForm
                      ]
                  ]
              ]
          else
            HH.text ""
        ]
    else
      HH.p_ [ HH.em_ [ HH.text "No pucks in dewar" ] ]

render :: forall cs. State -> H.ComponentHTML Action cs AppMonad
render state =
  fluidContainer
    [ maybe (HH.text "") (makeAlert AlertDanger) state.errorMessage
    , plainH2_ "Dewar Table"
    , HH.hr_
    , HH.p_
        [ HH.button
            [ HP.type_ ButtonButton
            , classList [ "btn", "btn-danger" ]
            , HE.onClick \_ -> Just WipeDewarTable
            ]
            [ icon { name: "broom", size: Nothing, spin: false }, HH.text " Wipe table" ]
        ]
    , dewarTable state
    , dewarForm state
    , HH.h2 [ singleClass "mt-4" ] [ if state.diffractionPuckId == "" then HH.text "Diffractions" else HH.text ("Diffractions — Puck " <> state.diffractionPuckId) ]
    , HH.hr_
    , if state.diffractionPuckId == "" then HH.p [ singleClass "text-muted" ] [ HH.text "Please select a puck from the table above." ] else diffractionForm state
    ]
