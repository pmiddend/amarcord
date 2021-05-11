module App.Components.Beamline where

import App.API (DewarEntry, DewarResponse, DiffractionEntry, DiffractionResponse, Puck, PucksResponse, addDiffraction, addPuckToTable, removeSingleDewarEntry, removeWholeTable, retrieveDewarTable, retrieveDiffractions, retrievePucks)
import App.AppMonad (AppMonad)
import App.Bootstrap (TableFlag(..), fluidContainer, plainH1_, plainH2_, plainH3_, table)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.HalogenUtils (AlertType(..), classList, makeAlert, singleClass)
import App.Route (BeamlineRouteInput, Route(..), createLink)
import Control.Applicative (pure)
import Control.Apply ((<*>))
import Control.Bind (bind)
import Data.Array (cons, elem, filter, head, mapMaybe, notElem, null)
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
    , diffractionPinhole :: String
    , diffractionFocusing :: String
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
  , editPuckId: (fromMaybe { puckId: "" } (choosePuck pucks dt)).puckId
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
  , diffractionCrystalId: ""
  , diffractionRunId: 1
  , diffractionOutcome: "success"
  , diffractionBeamIntensity: ""
  , diffractionPinhole: ""
  , diffractionFocusing: ""
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
          , pinhole: s.diffractionPinhole
          , focusing: s.diffractionFocusing
          , comment: s.diffractionComment
          }
      diffs <- H.lift (addDiffraction newDiffraction s.diffractionPuckId)
      case diffs of
        Right newDiffractions -> do
          H.modify_ \state ->
            state
              { diffractions = newDiffractions.diffractions
              , diffractionRunId = selectRunId state.diffractionCrystalId newDiffractions.diffractions
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
          state
            { diffractionPuckId = newPuckId
            , diffractions = newDiffractions.diffractions
            }
        updateHash
      Left e -> H.modify_ \state -> state { errorMessage = Just e }
  PuckIdChange newPuckId -> H.modify_ \state -> state { dewarEdit = state.dewarEdit { editPuckId = newPuckId } }
  WipeDewarTable -> do
    w <- H.liftEffect window
    confirmResult <- H.liftEffect (confirm "Really delete all entries?" w)
    if confirmResult then do
      result <- H.lift removeWholeTable
      handleDewarResponse result
    else
      pure unit
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
      HH.tr_
        [ HH.td_
            [ HH.button
                [ HP.type_ ButtonButton
                , classList [ "btn", "btn-warning", "btn-sm" ]
                , HE.onClick \_ -> Just (RemoveDewar dewarPosition puckId)
                ]
                [ HH.text "Remove" ]
            ]
        , HH.td_ [ HH.text (show dewarPosition) ]
        , HH.td_ [ HH.text puckId ]
        ]
  in
    table [ TableStriped ]
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
        [ classList [ "form-select", "form-control" ]
        , HE.onValueChange (Just <<< PuckIdChange)
        ]
        (makeOption <$> availablePucks)
  in
    HH.form [ singleClass "form-inline" ]
      [ HH.div [ classList [ "form-group" ] ]
          [ HH.label [ HP.for "dewar-position", classList [ "my-1", "mr-2" ] ] [ HH.text "Dewar position" ]
          , HH.input
              [ HP.type_ InputNumber
              , HP.id_ "dewar-position"
              , classList [ "form-control", "mr-2" ]
              , HP.value (show state.dewarEdit.editDewarPosition)
              , HE.onValueChange (\x -> DewarPositionChange <$> fromString x)
              ]
          , HH.label [ HP.for "puck-id", classList [ "mr-2" ] ] [ HH.text "Puck ID" ]
          , if pucksAvailable then puckSelector else HH.p_ [ HH.em_ [ HH.text "all taken" ] ]
          , HH.button
              [ HP.type_ ButtonButton
              , classList [ "btn", "btn-primary", "ml-2" ]
              , HE.onClick \_ -> Just AddDewar
              , HP.enabled addEnabled
              ]
              [ HH.text "Add" ]
          ]
      ]

diffractionForm :: forall w. State -> HH.HTML w Action
diffractionForm state =
  let
    makePuckOption :: String -> HH.HTML w Action
    makePuckOption puckId =
      HH.option
        [ HP.value puckId
        , HP.selected (state.diffractionPuckId == puckId)
        ]
        [ HH.text puckId ]

    pucksInDewar :: Array String
    pucksInDewar = _.puckId <$> state.dewarTable

    pucksAvailable = not (null pucksInDewar)

    emptyOption = HH.option [ HP.enabled false, HP.selected (state.diffractionPuckId == ""), HP.value "" ] [ HH.text "Select puck" ]

    puckSelector =
      HH.select
        [ classList [ "form-select", "form-control" ]
        , HE.onValueChange (Just <<< DiffractionPuckIdChange)
        ]
        (cons emptyOption (makePuckOption <$> pucksInDewar))

    inputForm =
      HH.form [ classList [ "form-inline", "mb-2" ] ]
        [ HH.div [ classList [ "form-group" ] ]
            [ HH.label [ HP.for "diffraction-puck-id", classList [ "mr-2" ] ] [ HH.text "Puck ID" ]
            , puckSelector
            ]
        ]

    headers = [ "Actions", "Crystal ID", "Run ID", "Puck Position ID", "Dewar Position", "Diffraction", "Comment" ]

    makeRow :: DiffractionEntry -> HH.HTML w Action
    makeRow diff =
      HH.tr [ classList (if diff.crystalId == state.diffractionCrystalId then [ "table-info" ] else []) ]
        [ HH.td_
            [ HH.button
                [ HP.type_ ButtonButton
                , classList [ "btn", "btn-secondary", "btn-sm" ]
                , HE.onClick \_ -> Just (DiffractionCrystalIdChange diff.crystalId)
                ]
                [ HH.text "Add run" ]
            ]
        , HH.td_ [ HH.text diff.crystalId ]
        , HH.td_ [ HH.text (maybe "" show diff.runId) ]
        , HH.td_ [ HH.text (show diff.puckPositionId) ]
        , HH.td_ [ HH.text (maybe "" show diff.dewarPosition) ]
        , HH.td_ [ HH.text (fromMaybe "" diff.diffraction) ]
        , HH.td_ [ HH.text (fromMaybe "" diff.comment) ]
        ]

    diffractionTable =
      table
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
        [ classList [ "form-select", "form-control" ]
        , HE.onValueChange (Just <<< DiffractionCrystalIdChange)
        ]
        (makeCrystalOption <$> (_.crystalId <$> state.diffractions))

    addDiffractionForm =
      HH.form_
        [ HH.div [ singleClass "form-group" ]
            [ HH.label [ HP.for "diffraction-crystal-id" ] [ HH.text "Crystal ID" ]
            , crystalIdSelect
            ]
        , HH.div [ singleClass "form-group" ]
            [ HH.label [ HP.for "diffraction-run-id" ] [ HH.text "Run ID" ]
            , HH.input
                [ HP.type_ InputNumber
                , HP.id_ "diffraction-run-id"
                , classList [ "form-control", "mr-2" ]
                , HP.value (show state.diffractionRunId)
                , HE.onValueChange (\x -> DiffractionRunIdChange <$> fromString x)
                ]
            ]
        , HH.div [ singleClass "form-group" ]
            [ HH.label [ HP.for "diffraction-outcome" ] [ HH.text "Diffraction" ]
            , HH.select
                [ classList [ "form-select", "form-control" ]
                , HE.onValueChange (\v -> Just (DiffractionStateChange (\state' -> state' { diffractionOutcome = v })))
                ]
                (makeOutcomeOption <$> [ "success", "no diffraction", "no crystal", "ice salt" ])
            ]
        , HH.div [ singleClass "form-group" ]
            [ HH.label [ HP.for "diffraction-beam-intensity" ] [ HH.text "Beam intensity" ]
            , HH.input
                [ HP.type_ InputNumber
                , HP.id_ "diffraction-beam-intensity"
                , classList [ "form-control", "mr-2" ]
                , HP.value (show state.diffractionBeamIntensity)
                , HE.onValueChange (\x -> Just (DiffractionStateChange (\state' -> state' { diffractionBeamIntensity = x })))
                ]
            ]
        , HH.div [ singleClass "form-group" ]
            [ HH.label [ HP.for "diffraction-pinhole" ] [ HH.text "Pinhole" ]
            , HH.select
                [ classList [ "form-select", "form-control" ]
                , HE.onValueChange (\v -> Just (DiffractionStateChange (\state' -> state' { diffractionPinhole = v })))
                ]
                (makeOutcomeOption <$> [ "undefined", "20 um", "50 um", "100 um", "200 um" ])
            ]
        , HH.div [ singleClass "form-group" ]
            [ HH.label [ HP.for "diffraction-focusing" ] [ HH.text "Focusing" ]
            , HH.input
                [ HP.type_ InputNumber
                , HP.id_ "diffraction-focusing"
                , classList [ "form-control", "mr-2" ]
                , HP.value (show state.diffractionFocusing)
                , HE.onValueChange (\x -> Just (DiffractionStateChange (\state' -> state' { diffractionFocusing = x })))
                ]
            ]
        , HH.div [ singleClass "form-group" ]
            [ HH.label [ HP.for "diffraction-comment" ] [ HH.text "Comment" ]
            , HH.input
                [ HP.type_ InputNumber
                , HP.id_ "diffraction-comment"
                , classList [ "form-control", "mr-2" ]
                , HP.value (show state.diffractionComment)
                , HE.onValueChange (\x -> Just (DiffractionStateChange (\state' -> state' { diffractionComment = x })))
                ]
            ]
        , HH.button
            [ HP.type_ ButtonButton
            , classList [ "btn", "btn-primary" ]
            , HE.onClick \_ -> Just AddDiffraction
            ]
            [ HH.text "Add diffraction run" ]
        ]
  in
    if pucksAvailable then
      HH.div_
        [ inputForm
        , HH.div [ singleClass "row" ]
            [ HH.div [ singleClass "col" ] [ diffractionTable ]
            , HH.div [ singleClass "col" ]
                [ plainH3_ "Add diffraction"
                , addDiffractionForm
                ]
            ]
        ]
    else
      HH.p_ [ HH.em_ [ HH.text "No pucks in dewar" ] ]

render :: forall cs. State -> H.ComponentHTML Action cs AppMonad
render state =
  fluidContainer
    [ plainH1_ "P11 Beamline Overview"
    , maybe (HH.text "") (makeAlert AlertDanger) state.errorMessage
    , plainH2_ "Dewar Table"
    , HH.p_
        [ HH.button
            [ HP.type_ ButtonButton
            , classList [ "btn", "btn-danger" ]
            , HE.onClick \_ -> Just WipeDewarTable
            ]
            [ HH.text "Wipe table" ]
        ]
    , dewarTable state
    , dewarForm state
    , plainH2_ "Diffractions"
    , diffractionForm state
    ]
