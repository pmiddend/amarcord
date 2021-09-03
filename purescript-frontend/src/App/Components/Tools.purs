module App.Components.Tools where

import App.API (CrystalFilter, CrystalFilterResponse, Tool, ToolsResponse, retrieveCrystalFilters, retrieveReductionCount, retrieveTools, startJobsSimple)
import App.AppMonad (AppMonad)
import App.Bootstrap (container)
import App.Components.ParentComponent (ChildInput, ParentError, parentComponent)
import App.Halogen.FontAwesome (icon)
import App.HalogenUtils (AlertType(..), makeAlert, makeAlertHtml, singleClass)
import App.Route (Route(..), ToolsRouteInput, createLink)
import App.SQL (sqlConditionToString)
import App.SortOrder (SortOrder(..))
import App.Utils (Endo, dehomoEither, startsWith)
import Control.Applicative (pure, (<*>))
import Control.Bind (bind, (>>=))
import Data.Array (any, filter, length, (:))
import Data.Boolean (otherwise)
import Data.BooleanAlgebra (not)
import Data.Either (Either(..), isLeft)
import Data.Eq (class Eq, eq, (==))
import Data.EuclideanRing ((-))
import Data.Foldable (find, foldMap)
import Data.FoldableWithIndex (foldrWithIndex)
import Data.Function (const, ($), (<<<))
import Data.Functor (map, (<$>))
import Data.Int (fromString)
import Data.Map (Map, delete, fromFoldable, insert, lookup, member)
import Data.Maybe (Maybe(..), fromMaybe, isNothing, maybe)
import Data.Monoid (mempty)
import Data.Ord (class Ord, min)
import Data.Ring ((*))
import Data.Semigroup ((<>))
import Data.Show (show)
import Data.Symbol (SProxy(..))
import Data.Tuple (Tuple(..))
import Data.Unit (Unit, unit)
import Data.Void (Void)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties (InputType(..))
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData(..), fromEither)

data JobDecision
  = Process
  | Refine

parseDecision :: String -> Maybe JobDecision
parseDecision "process" = Just Process

parseDecision "refine" = Just Refine

parseDecision _ = Nothing

derive instance eqJobDecision :: Eq JobDecision

type RefinementData
  = { onlyUnrefined :: Boolean
    , reductionMethod :: Maybe String
    , selectedFilters :: Map String (Maybe String)
    , toolParameters :: Map String String
    , limit :: Int
    , toolId :: Maybe Int
    , reductionCount :: Int
    , comment :: String
    , jobStartResult :: RemoteData String String
    }

data JobFormData
  = RefinementData RefinementData
  | ProcessData

jobFormDataToDecision :: JobFormData -> JobDecision
jobFormDataToDecision (RefinementData _) = Refine

jobFormDataToDecision ProcessData = Process

type JobForm
  = { jobFormData :: Maybe JobFormData
    , tools :: Array Tool
    , crystalFilters :: Array CrystalFilter
    }

type State
  = { errorMessage :: Maybe String
    , jobForm :: JobForm
    }

data ToolsData
  = ToolsData ToolsResponse CrystalFilterResponse

derive instance eqAssociatedTable :: Eq ToolsData

derive instance ordAssociatedTable :: Ord ToolsData

data Action
  = DecisionChange JobDecision
  | RefinementToggleOnlyNonrefined
  | RefinementReductionMethodChanged String
  | RefinementCrystalFilterChanged String String
  | RefinementLimitChanged Int
  | RefinementCommentChanged String
  | RefinementToolChanged Int
  | RefinementToolParameterChanged String String
  | RefinementStart

initialState :: ChildInput ToolsRouteInput ToolsData -> State
initialState { input: sorting, remoteData: ToolsData { tools } { columnsWithValues } } =
  { errorMessage: Nothing
  , jobForm: { jobFormData: Nothing, tools, crystalFilters: columnsWithValues }
  }

component :: forall query output. H.Component HH.HTML query ToolsRouteInput output AppMonad
component = parentComponent fetchData childComponent

childComponent :: forall q. H.Component HH.HTML q (ChildInput ToolsRouteInput ToolsData) ParentError AppMonad
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

modifyRefinementData :: Endo RefinementData -> Endo State
modifyRefinementData f state = case state.jobForm.jobFormData of
  Just (RefinementData rd) -> state { jobForm = state.jobForm { jobFormData = Just (RefinementData (f rd)) } }
  _ -> state

refreshRefinementData :: forall slots. State -> RefinementData -> H.HalogenM State Action slots ParentError AppMonad Unit
refreshRefinementData state rd = do
  reductionCount' <- H.lift (retrieveReductionCount rd.selectedFilters rd.reductionMethod rd.onlyUnrefined)
  case reductionCount' of
    Right { totalResults: reductionCount } -> do
      let
        rdWithCount = RefinementData (rd { reductionCount = reductionCount })
      H.put (state { jobForm = state.jobForm { jobFormData = Just rdWithCount } })
    Left e -> H.put (state { errorMessage = Just e })

modifyRefinementDataStateful :: forall slots. Endo RefinementData -> H.HalogenM State Action slots ParentError AppMonad Unit
modifyRefinementDataStateful f = do
  state <- H.get
  case state.jobForm.jobFormData of
    Just (RefinementData oldRd) -> refreshRefinementData state (f oldRd)
    _ -> pure unit

handleAction :: forall slots. Action -> H.HalogenM State Action slots ParentError AppMonad Unit
handleAction = case _ of
  RefinementStart -> do
    state <- H.get
    case state.jobForm.jobFormData of
      Just (RefinementData rd) -> case rd.toolId of
        Nothing -> pure unit
        Just toolId -> do
          result <-
            H.lift
              $ startJobsSimple
                  toolId
                  rd.comment
                  (if rd.limit == 0 then Nothing else Just rd.limit)
                  rd.toolParameters
                  rd.selectedFilters
                  rd.reductionMethod
                  rd.onlyUnrefined
          case result of
            Left e -> H.modify_ (modifyRefinementData \rd' -> rd' { jobStartResult = Failure e })
            Right { jobIds } -> H.modify_ (modifyRefinementData \rd' -> rd' { jobStartResult = Success $ "Started " <> show (length jobIds) <> (if length jobIds == 1 then " job!" else " jobs!") })
      _ -> pure unit
  DecisionChange v -> do
    case v of
      Process -> H.modify_ \state -> state { jobForm = state.jobForm { jobFormData = Just ProcessData } }
      Refine -> do
        state <- H.get
        let
          rd =
            { onlyUnrefined: true
            , reductionMethod: Nothing
            , selectedFilters: mempty
            , limit: 0
            , toolId: Nothing
            , reductionCount: 0
            , toolParameters: mempty
            , comment: ""
            , jobStartResult: NotAsked
            }
        refreshRefinementData state rd
  RefinementToggleOnlyNonrefined -> modifyRefinementDataStateful (\rd -> rd { onlyUnrefined = not rd.onlyUnrefined })
  RefinementReductionMethodChanged newMethod -> modifyRefinementDataStateful (\rd -> rd { reductionMethod = if newMethod == "" then Nothing else Just newMethod })
  RefinementCrystalFilterChanged columnName value ->
    modifyRefinementDataStateful case value of
      "" -> \rd -> rd { selectedFilters = delete columnName rd.selectedFilters }
      "None" -> \rd -> rd { selectedFilters = insert columnName Nothing rd.selectedFilters }
      value' -> (\rd -> rd { selectedFilters = insert columnName (Just value') rd.selectedFilters })
  RefinementLimitChanged newLimit -> H.modify_ (modifyRefinementData \rd -> rd { limit = newLimit })
  RefinementCommentChanged newComment -> H.modify_ (modifyRefinementData \rd -> rd { comment = newComment })
  RefinementToolChanged newToolId -> do
    jobForm' <- H.gets _.jobForm
    let
      chosenTool = find (\t -> t.toolId == newToolId) jobForm'.tools

      toolParameters = (\newTool -> fromFoldable (foldMap (\i -> if i."type" == "string" then [ Tuple i.name "" ] else []) newTool.inputs)) <$> chosenTool
    modifyRefinementDataStateful (\rd -> rd { toolId = Just newToolId, toolParameters = fromMaybe mempty toolParameters })
  RefinementToolParameterChanged paramName paramValue -> H.modify_ (modifyRefinementData \rd -> rd { toolParameters = insert paramName paramValue rd.toolParameters })

fetchData :: ToolsRouteInput -> AppMonad (RemoteData String ToolsData)
fetchData {} = do
  tools <- retrieveTools
  crystalFilters <- retrieveCrystalFilters
  pure (fromEither (ToolsData <$> tools <*> crystalFilters))

type Slots
  = ( jobList :: forall query. H.Slot query Void Int
    , toolRunner :: forall query. H.Slot query Void Int
    , toolsCrud :: forall query. H.Slot query Void Int
    )

_jobList = SProxy :: SProxy "jobList"

_toolRunner = SProxy :: SProxy "toolRunner"

_toolsCrud = SProxy :: SProxy "toolsCrud"

jobForm :: forall w. State -> HH.HTML w Action
jobForm state =
  let
    classifyTool :: Tool -> JobDecision
    classifyTool t
      | any (\ti -> ti.type `startsWith` "Data_Reduction.") t.inputs = Refine
      | otherwise = Process

    totalProcessTools :: Int
    totalProcessTools = length (filter (\t -> t == Process) (classifyTool <$> state.jobForm.tools))

    refinementTools :: Array Tool
    refinementTools = filter (\t -> classifyTool t == Refine) state.jobForm.tools

    totalRefineTools :: Int
    totalRefineTools = length state.jobForm.tools - totalProcessTools

    processText :: String
    processText
      | totalProcessTools == 0 = "No tools available, add one below"
      | totalProcessTools == 1 = "One tool available"
      | otherwise = show totalProcessTools <> " tools available"

    refineText :: String
    refineText
      | totalRefineTools == 0 = "No tools available, add one below"
      | totalRefineTools == 1 = "One tool available"
      | otherwise = show totalRefineTools <> " tools available"

    introButton =
      HH.div [ singleClass "d-flex btn-group" ]
        [ HH.input
            [ HP.type_ InputRadio
            , singleClass "btn-check"
            , HP.name "job-form-decision"
            , HP.value "process"
            , HP.id_ "job-form-decision-process"
            , HP.disabled (totalProcessTools == 0)
            , HP.checked ((jobFormDataToDecision <$> state.jobForm.jobFormData) == Just Process)
            , HE.onValueChange ((map DecisionChange) <<< parseDecision)
            ]
        , HH.label
            [ HP.for "job-form-decision-process"
            , singleClass ("btn " <> if totalProcessTools == 0 then "btn-secondary" else "btn-success")
            ]
            [ HH.text "Process diffractions", HH.br_, HH.small_ [ HH.text processText ] ]
        , HH.input
            [ HP.type_ InputRadio
            , singleClass "btn-check"
            , HP.name "job-form-decision"
            , HP.value "refine"
            , HP.id_ "job-form-decision-refine"
            , HP.disabled (totalRefineTools == 0)
            , HP.checked ((jobFormDataToDecision <$> state.jobForm.jobFormData) == Just Refine)
            , HE.onValueChange ((map DecisionChange) <<< parseDecision)
            ]
        , HH.label
            [ HP.for "job-form-decision-refine"
            , singleClass ("btn " <> if totalRefineTools == 0 then "btn-secondary" else "btn-primary")
            ]
            [ HH.text "Refine", HH.br_, HH.small_ [ HH.text refineText ] ]
        ]

    linkToAdminister = if totalRefineTools * totalProcessTools == 0 then HH.a [ HP.href (createLink ToolsAdmin) ] [ HH.text "→ Add a tool" ] else HH.text ""

    refinementForm :: RefinementData -> HH.HTML w Action
    refinementForm rd =
      let
        noReductionMethodOption = HH.option [ HP.value "", HP.selected (rd.reductionMethod == Nothing) ] [ HH.text "any method is fine" ]

        noCrystalFilterOption columnName = HH.option [ HP.value "", HP.selected (not (columnName `member` rd.selectedFilters)) ] [ HH.text "any value is fine" ]

        separatorOption = HH.option [ HP.disabled true ] [ HH.text "──────────" ]

        makeReductionMethodOption rm = HH.option [ HP.value rm, HP.selected (rd.reductionMethod == Just rm) ] [ HH.text rm ]

        reductionMethodOptions = makeReductionMethodOption <$> [ "xds_full", "staraniso", "other" ]

        filterOptionIsSelected :: String -> Maybe String -> Boolean
        filterOptionIsSelected columnName value = fromMaybe false (eq value <$> columnName `lookup` rd.selectedFilters)

        makeCrystalFilterOption :: String -> Maybe String -> HH.HTML w Action
        makeCrystalFilterOption columnName value = HH.option [ HP.value (fromMaybe "None" value), HP.selected (filterOptionIsSelected columnName value) ] [ HH.text (fromMaybe "None" value) ]

        makeCrystalFilterElement :: CrystalFilter -> HH.HTML w Action
        makeCrystalFilterElement { columnName, values } =
          HH.div [ singleClass "mt-3 row" ]
            [ HH.div [ singleClass "col" ] [ HH.label [ singleClass "form-label col-form-label", HP.for ("refine-form-crystal-filter" <> columnName) ] [ HH.text columnName ] ]
            , HH.div [ singleClass "col" ] [ HH.select [ singleClass "form-select col", HP.id_ ("refine-form-crystal-filter" <> columnName), HE.onValueChange (\x -> Just (RefinementCrystalFilterChanged columnName x)) ] (noCrystalFilterOption columnName : separatorOption : (makeCrystalFilterOption columnName <$> values)) ]
            ]

        crystalFilterElements = makeCrystalFilterElement <$> state.jobForm.crystalFilters

        viewInAnalysisTable =
          let
            crystalConditions = (foldrWithIndex (\column value priorList -> { column: "Crystals." <> column, value } : priorList) mempty rd.selectedFilters)

            reductionConditions = case rd.reductionMethod of
              Nothing -> []
              Just m -> [ { column: "Data_Reduction.method", value: Just m } ]

            filterQuery = sqlConditionToString (crystalConditions <> reductionConditions)
          in
            HH.a [ HP.href (createLink (Analysis { sortColumn: "crystals_crystal_id", sortOrder: Ascending, filterQuery })) ] [ HH.text "→ View datasets in analysis table" ]

        noToolOption = HH.option [ HP.value "0", HP.disabled true, HP.selected (rd.toolId == Nothing) ] [ HH.text "Choose a tool" ]

        makeToolOption tool = HH.option [ HP.value (show tool.toolId), HP.selected (rd.toolId == Just tool.toolId) ] [ HH.text (tool.name) ]

        reductionCount = if rd.limit == 0 then rd.reductionCount else min rd.limit rd.reductionCount

        buttonText =
          if rd.reductionCount == 0 then
            Left "No datasets found to refine (maybe all have been refined already?)"
          else if isNothing rd.toolId then
            Left "Please choose a tool to use"
          else
            Right ("Refine " <> show reductionCount <> (if reductionCount == 1 then " dataset" else " datasets"))

        sectionTool =
          let
            chosenTool :: Maybe Tool
            chosenTool = rd.toolId >>= \tid -> find (\t -> t.toolId == tid) state.jobForm.tools

            makeParameterRow name value prevRows =
              ( HH.div [ singleClass "mt-3 row" ]
                  [ HH.div [ singleClass "col" ]
                      [ HH.label [ singleClass "form-label col-form-label", HP.for ("refine-form-tool-parameter" <> name) ] [ HH.text name ]
                      ]
                  , HH.div [ singleClass "col" ] [ HH.input [ singleClass "form-control", HP.type_ InputText, HP.value value, HE.onValueChange (\v -> Just (RefinementToolParameterChanged name v)) ] ]
                  ]
              )
                : prevRows

            toolUserParameters = foldrWithIndex makeParameterRow mempty rd.toolParameters

            toolDescription = case chosenTool of
              Nothing -> []
              Just { description: "" } -> []
              Just { description } -> [ HH.div [ singleClass "mt-3 row" ] [ HH.p_ [ HH.text "Description: ", HH.em_ [ HH.text description ] ] ] ]
          in
            ( [ HH.h3 [ singleClass "mt-3" ] [ icon { name: "cogs", size: Nothing, spin: false }, HH.text " Tool" ]
              , HH.div [ singleClass "mt-3 row" ]
                  [ HH.div [ singleClass "col" ]
                      [ HH.label [ singleClass "form-label col-form-label", HP.for "refine-form-limit" ] [ HH.text "Tool" ]
                      , HH.div [ singleClass "form-text" ] [ HH.text "After selecting a tool, you might need to enter some more parameters specific to the tool" ]
                      ]
                  , HH.div [ singleClass "col" ] [ HH.select [ singleClass "form-select", HP.id_ "refine-form-tool", HE.onValueChange (\x -> RefinementToolChanged <$> fromString x), HP.value (show (fromMaybe 0 rd.toolId)) ] (noToolOption : (makeToolOption <$> refinementTools)) ]
                  ]
              ]
                <> toolUserParameters
                <> toolDescription
            )

        jobStartResult = case rd.jobStartResult of
          Failure e -> makeAlert AlertDanger e
          Success e -> makeAlertHtml AlertSuccess [ HH.text e, HH.br_, HH.text "Go to the ", HH.a [ HP.href (createLink Jobs) ] [ HH.text "job list" ], HH.text " to view your jobs" ]
          _ -> HH.text ""

        sectionStartRefinement =
          [ HH.h3 [ singleClass "mt-3" ] [ icon { name: "bong", size: Nothing, spin: false }, HH.text " Starting the refinement" ]
          , HH.div [ singleClass "mt-3 row" ]
              [ HH.div [ singleClass "col" ]
                  [ HH.label [ singleClass "form-label col-form-label", HP.for "refine-form-limit" ] [ HH.text "Limit" ]
                  , HH.div [ singleClass "form-text" ] [ HH.text "If this is 0, there will be no limit - all datasets will be refined" ]
                  ]
              , HH.div [ singleClass "col" ] [ HH.input [ HP.type_ InputNumber, singleClass "form-control", HP.id_ "refine-form-limit", HE.onValueChange (\x -> RefinementLimitChanged <$> fromString x), HP.value (show rd.limit) ] ]
              ]
          , HH.div [ singleClass "mt-3 row" ]
              [ HH.div [ singleClass "col" ]
                  [ HH.label [ singleClass "form-label col-form-label", HP.for "refine-form-comment" ] [ HH.text "Comment" ]
                  , HH.div [ singleClass "form-text" ] [ HH.text "If you want, describe what you're doing. The text will be attached to the job, so you can see it in the job overview" ]
                  ]
              , HH.div [ singleClass "col" ] [ HH.input [ HP.type_ InputText, singleClass "form-control", HP.id_ "refine-form-comment", HE.onValueChange (Just <<< RefinementCommentChanged), HP.value rd.comment ] ]
              ]
          , jobStartResult
          , HH.div [ singleClass "mt-3 mb-2 row" ]
              [ HH.button [ HP.type_ HP.ButtonButton, singleClass ("btn " <> (if isLeft buttonText then "btn-secondary" else "btn-primary")), HE.onClick \_ -> Just RefinementStart, HP.disabled (isLeft buttonText) ] [ HH.text (dehomoEither buttonText) ]
              ]
          ]
      in
        HH.form_
          ( [ HH.h3 [ singleClass "mt-3" ] [ icon { name: "puzzle-piece", size: Nothing, spin: false }, HH.text " Refinement parameters" ]
            , HH.p [ singleClass "lead" ] [ HH.text "Here you can specify exactly what processing results you want to refine. Below, you can see how many refinements will take place." ]
            , HH.div [ singleClass "mt-3 row" ]
                [ HH.div [ singleClass "col" ] [ HH.label [ singleClass "form-label col-form-label", HP.for "refine-form-reduction-method" ] [ HH.text "Reduction method" ] ]
                , HH.div [ singleClass "col" ] [ HH.select [ singleClass "form-select col", HP.id_ "refine-form-reduction-method", HE.onValueChange (Just <<< RefinementReductionMethodChanged) ] (noReductionMethodOption : separatorOption : reductionMethodOptions) ]
                ]
            ]
              <> crystalFilterElements
              <> [ HH.div [ singleClass "form-check mt-3 mb-3" ]
                    [ HH.input
                        [ HP.type_ InputCheckbox
                        , singleClass "form-check-input"
                        , HP.id_ "refine-form-only-nonrefined"
                        , HP.checked rd.onlyUnrefined
                        , HE.onValueChange (const (Just RefinementToggleOnlyNonrefined))
                        ]
                    , HH.label [ singleClass "form-check-label", HP.for "refine-form-only-nonrefined" ] [ HH.text "Only results that have not been refined yet" ]
                    ]
                , viewInAnalysisTable
                ]
              <> sectionTool
              <> sectionStartRefinement
          )

    actualForm = case state.jobForm.jobFormData of
      Just ProcessData -> HH.h2_ [ HH.text "Processing is a TODO right now, sorry" ]
      Just (RefinementData rd) -> refinementForm rd
      Nothing -> HH.text ""
  in
    HH.div_
      [ introButton
      , linkToAdminister
      , actualForm
      ]

render :: State -> H.ComponentHTML Action Slots AppMonad
render state =
  container
    [ HH.h2_ [ icon { name: "user-cog", size: Nothing, spin: false }, HH.text " I want to..." ]
    , jobForm state
    , maybe (HH.text "") (makeAlert AlertDanger) state.errorMessage
    ]
