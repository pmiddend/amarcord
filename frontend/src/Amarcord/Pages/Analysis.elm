module Amarcord.Pages.Analysis exposing (Model, Msg(..), init, update, view)

import Amarcord.API.DataSet exposing (DataSetId)
import Amarcord.API.ExperimentType exposing (ExperimentTypeId)
import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.API.RequestsHtml exposing (showHttpError)
import Amarcord.Attributo exposing (Attributo, AttributoId, AttributoType, AttributoValue, attributoIsChemicalId, convertAttributoFromApi, convertAttributoMapFromApi, prettyPrintAttributoValue)
import Amarcord.AttributoHtml exposing (formatFloatHumanFriendly, formatIntHumanFriendly)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert)
import Amarcord.CrystFELMerge as CrystFELMerge exposing (mergeModelToString, modelToMergeParameters)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Dialog as Dialog
import Amarcord.Html exposing (br_, div_, h5_, input_, li_, span_, tbody_, td_, th_, thead_, tr_)
import Amarcord.Route exposing (makeFilesLink)
import Amarcord.Util exposing (HereAndNow, posixDiffHumanFriendly, posixDiffMinutes)
import Api exposing (send)
import Api.Data exposing (JsonAnalysisDataSet, JsonDataSet, JsonExperimentType, JsonMergeParameters, JsonMergeResult, JsonMergeResultStateDone, JsonMergeResultStateError, JsonMergeResultStateQueued, JsonMergeResultStateRunning, JsonPolarisation, JsonReadAnalysisResults, JsonRefinementResult, JsonStartMergeJobForDataSetOutput, MergeResultFom, MergeResultShell, ScaleIntensities(..))
import Api.Request.Analysis exposing (readAnalysisResultsApiAnalysisAnalysisResultsBeamtimeIdGet)
import Api.Request.Merging exposing (startMergeJobForDataSetApiMergingDataSetIdStartPost)
import Dict exposing (Dict)
import Dict.Extra
import Html exposing (Html, a, abbr, button, dd, div, dl, dt, em, h2, h4, input, label, node, p, small, span, sup, table, td, text, th, tr, ul)
import Html.Attributes exposing (attribute, checked, class, colspan, disabled, for, href, id, style, title, type_)
import Html.Events exposing (onClick, onInput)
import Http
import List.Extra
import Maybe
import Maybe.Extra as MaybeExtra
import RemoteData exposing (RemoteData(..), fromResult, isLoading)
import Set exposing (Set)
import String
import Time exposing (Posix, millisToPosix)


type Msg
    = AnalysisResultsReceived (Result Http.Error JsonReadAnalysisResults)
    | StartMerge DataSetId
    | QuickStartMerge DataSetId
    | CancelMerge
    | SubmitMerge DataSetId CrystFELMerge.Model
    | MergeFinished (Result Http.Error JsonStartMergeJobForDataSetOutput)
    | Refresh Posix
    | OpenMergeResultDetail DetailMerge
    | CloseMergeResultDetail
    | CrystFELMergeMessage CrystFELMerge.Msg
    | ToggleAccordionShowModelParameters MergeResultId
    | SortDataSetsAscending
    | SetFilterExperimentType ExperimentTypeId
    | SetFilterMergeStatus (Maybe MergeStatus)
    | SetFilterExperimentTypeAttributo ExperimentTypeAttributoFilter String
    | SetFilterExperimentTypeAllAttributi (List ExperimentTypeAttributoFilter)
    | SetFilterExperimentTypeNoneAttributi (List ExperimentTypeAttributoFilter)


type MergeStatus
    = Merged
    | Unmerged


type SelectedMergeResult
    = NoMergeResultSelected
    | MergeResultSelected DetailMerge


type alias MergeResultId =
    Int


type alias MergeResultWrapper =
    { mergeResult : JsonMergeResult, showResults : Bool }


type alias ActivatedMergeForm =
    { dataSetId : Int
    , mergeParameters : CrystFELMerge.Model
    }


type alias DetailMerge =
    { id : Int, fom : MergeResultFom, shell_foms : List MergeResultShell, refinementResults : List JsonRefinementResult }


type alias MergeRequest =
    { dataSetId : Int
    , request : RemoteData Http.Error JsonStartMergeJobForDataSetOutput
    }


type alias ExperimentTypeAttributoFilter =
    { expType : JsonExperimentType, attrId : AttributoId, attrValue : AttributoValue }


type alias Model =
    { hereAndNow : HereAndNow
    , analysisRequest : RemoteData Http.Error JsonReadAnalysisResults
    , activatedMergeForm : Maybe ActivatedMergeForm
    , mergeRequest : Maybe MergeRequest
    , selectedMergeResult : SelectedMergeResult
    , hiddenParametersMergeResultIds : Set MergeResultId
    , hiddenExperimentTypeIds : Set ExperimentTypeId
    , hiddenExperimentTypeAttributiFilters : List ExperimentTypeAttributoFilter
    , selectMergeStatus : Maybe MergeStatus
    , dataSetsSortingAscending : Bool
    , beamtimeId : BeamtimeId
    }


init : HereAndNow -> BeamtimeId -> ( Model, Cmd Msg )
init hereAndNow beamtimeId =
    ( { hereAndNow = hereAndNow
      , analysisRequest = Loading
      , activatedMergeForm = Nothing
      , mergeRequest = Nothing
      , selectedMergeResult = NoMergeResultSelected
      , hiddenParametersMergeResultIds = Set.empty
      , hiddenExperimentTypeIds = Set.empty
      , hiddenExperimentTypeAttributiFilters = []
      , selectMergeStatus = Nothing
      , dataSetsSortingAscending = True
      , beamtimeId = beamtimeId
      }
    , send AnalysisResultsReceived (readAnalysisResultsApiAnalysisAnalysisResultsBeamtimeIdGet beamtimeId)
    )


viewFilters : Set ExperimentTypeId -> Maybe MergeStatus -> JsonReadAnalysisResults -> Html Msg
viewFilters hiddenExperimentTypeIds mergeStatus { experimentTypes } =
    let
        mergeFilterId =
            "merge_results_filter"

        expTypeFilterId =
            "exp_type_filter"

        experimentTypeFilterButton expType =
            let
                buttonExpTypeId =
                    expTypeFilterId ++ "_" ++ expType.name
            in
            [ input_
                [ type_ "radio"
                , class "btn-check"
                , id buttonExpTypeId
                , checked (not <| Set.member expType.id hiddenExperimentTypeIds)
                , onClick (SetFilterExperimentType expType.id)
                ]
            , label
                [ class "btn btn-outline-primary"
                , style "border-color" "black"
                , for buttonExpTypeId
                ]
                [ small [] [ text expType.name ] ]
            ]

        mergeResultPresentFilter =
            let
                btnInput ms labelText =
                    input_
                        [ type_ "radio"
                        , class "btn-check"
                        , id <| mergeFilterId ++ "_" ++ String.toLower labelText
                        , checked (mergeStatus == ms)
                        , onClick (SetFilterMergeStatus ms)
                        ]

                btnLabel labelText =
                    label
                        [ class "btn btn-outline-primary"
                        , style "border-color" "black"
                        , for <| mergeFilterId ++ "_" ++ String.toLower labelText
                        ]
                        [ small [] [ text labelText ] ]

                btnCombi ms labelText =
                    [ btnInput ms labelText, btnLabel labelText ]
            in
            div [ class "btn-group mb-1", id mergeFilterId ]
                ((btnCombi Nothing "All"
                    ++ btnCombi (Just Merged) "Merged"
                 )
                    ++ btnCombi (Just Unmerged) "Unmerged"
                )
    in
    div []
        [ div_
            [ label [ style "width" "10rem", for expTypeFilterId ] [ icon { name = "clipboard-check" }, text " Experiment Type " ]
            , div [ class "btn-group mb-1", id expTypeFilterId ] <| List.concatMap experimentTypeFilterButton experimentTypes
            ]
        , div_
            [ label [ style "width" "10rem", for mergeFilterId ] [ icon { name = "diagram-2" }, text " Merge Status " ]
            , mergeResultPresentFilter
            ]
        ]


viewInner : HereAndNow -> Maybe MergeRequest -> Maybe ActivatedMergeForm -> Maybe MergeStatus -> Set ExperimentTypeId -> Set MergeResultId -> List ExperimentTypeAttributoFilter -> Bool -> JsonReadAnalysisResults -> List (Html Msg)
viewInner hereAndNow mergeRequest activatedMergeForm selectedMergeStatus hiddenExperimentTypeIds hiddenMergeResultIds hiddenExperimentTypeAttributoFilters datasetSortingIdAscending { experimentTypes, attributi, chemicalIdToName, dataSets } =
    List.map
        (\experimentTypePrime ->
            viewResultsTableForSingleExperimentType
                (List.map convertAttributoFromApi attributi)
                hereAndNow
                mergeRequest
                activatedMergeForm
                (List.foldr (\{ chemicalId, name } -> Dict.insert chemicalId name) Dict.empty chemicalIdToName)
                experimentTypePrime
                selectedMergeStatus
                hiddenMergeResultIds
                hiddenExperimentTypeAttributoFilters
                datasetSortingIdAscending
                (Maybe.withDefault [] <| Maybe.map .dataSets <| List.Extra.find (\{ experimentType } -> experimentType == experimentTypePrime.id) dataSets)
        )
        (List.filter (\expType -> not <| Set.member expType.id hiddenExperimentTypeIds) experimentTypes)



-- we should probably refactor the OpenAPI spec to use a tagged union in the first place, but for now, this is the band-aid


type MergeResultStateUnion
    = MergeResultStateQueued JsonMergeResultStateQueued
    | MergeResultStateError JsonMergeResultStateError
    | MergeResultStateRunning JsonMergeResultStateRunning
    | MergeResultStateDone JsonMergeResultStateDone


createMergeResultUnion : JsonMergeResult -> Maybe MergeResultStateUnion
createMergeResultUnion mr =
    case mr.stateQueued of
        Nothing ->
            case mr.stateError of
                Nothing ->
                    case mr.stateRunning of
                        Nothing ->
                            mr.stateDone |> Maybe.map MergeResultStateDone

                        Just running ->
                            Just (MergeResultStateRunning running)

                Just error ->
                    Just (MergeResultStateError error)

        Just queued ->
            Just (MergeResultStateQueued queued)


viewResultsTableForSingleExperimentType :
    List (Attributo AttributoType)
    -> HereAndNow
    -> Maybe MergeRequest
    -> Maybe ActivatedMergeForm
    -> Dict Int String
    -> JsonExperimentType
    -> Maybe MergeStatus
    -> Set MergeResultId
    -> List ExperimentTypeAttributoFilter
    -> Bool
    -> List JsonAnalysisDataSet
    -> Html Msg
viewResultsTableForSingleExperimentType attributi hereAndNow mergeRequest activatedMergeForm chemicalIdsToName experimentType selectedMergeStatus hiddenMergeResultIds hiddenExperimentTypeAttributoFilters datasetSortingIdAscending dataSets =
    let
        mergeRequestIsLoading : Maybe MergeRequest -> Bool
        mergeRequestIsLoading x =
            case x of
                Nothing ->
                    False

                Just { request } ->
                    isLoading request

        viewDataSet : JsonAnalysisDataSet -> List (Html Msg)
        viewDataSet experimentTypeResults =
            let
                dataSet : JsonDataSet
                dataSet =
                    experimentTypeResults.dataSet

                mergeResultWrappers : List MergeResultWrapper
                mergeResultWrappers =
                    List.map
                        (\mr -> { mergeResult = mr, showResults = Set.member mr.id hiddenMergeResultIds })
                        experimentTypeResults.mergeResults

                mergeRowHeaders : List (Html msg)
                mergeRowHeaders =
                    [ text "ID"
                    , text "Parameters"
                    , text "Runs"
                    , text "Time (min)"
                    , text "Resolution (Å)"
                    , text "Completeness (%)"
                    , text "Multiplicity"
                    , span_ [ text "CC", Html.sub [] [ text "1/2" ] ]
                    , span_ [ text "CC", sup [] [ text "*" ] ]
                    , text "Wilson B factor (Å²)"
                    , text "Files"
                    , div_ []
                    ]

                scaleIntensitiesToString : ScaleIntensities -> String
                scaleIntensitiesToString x =
                    case x of
                        ScaleIntensitiesOff ->
                            "off"

                        ScaleIntensitiesDebyewaller ->
                            "Debye-Waller"

                        ScaleIntensitiesNormal ->
                            "Scale intensities"

                viewMergeParameters : JsonMergeParameters -> Html msg
                viewMergeParameters { mergeModel, scaleIntensities, postRefinement, iterations, polarisation, startAfter, stopAfter, relB, noPr, noDeltaCcHalf, maxAdu, minMeasurements, logs, minRes, pushRes, w } =
                    let
                        dtClass =
                            []

                        ddClass =
                            []

                        boolDtDl header b =
                            if b then
                                [ dt dtClass [ text header ] ]

                            else
                                []

                        dtDl dtContent dlContent =
                            [ dt dtClass [ text dtContent ]
                            , dd ddClass [ dlContent ]
                            ]

                        polarisationToDescription : JsonPolarisation -> String
                        polarisationToDescription { angle, percent } =
                            if angle == 0 then
                                "Horizontal "
                                    ++ (if percent == 100 then
                                            "e-field"

                                        else
                                            String.fromInt percent ++ "%"
                                       )

                            else if angle == 90 then
                                "Vertical "
                                    ++ (if percent == 100 then
                                            "e-field"

                                        else
                                            String.fromInt percent ++ "%"
                                       )

                            else
                                String.fromInt angle ++ "° " ++ String.fromInt percent ++ "%"

                        maybeDtDl header value =
                            case value of
                                Nothing ->
                                    []

                                Just realValue ->
                                    dtDl header realValue
                    in
                    div_
                        [ dl []
                            (dtDl "Model" (text <| mergeModelToString mergeModel)
                                ++ dtDl "Scale intensities" (text <| scaleIntensitiesToString scaleIntensities)
                                ++ boolDtDl "Post refinement" postRefinement
                                ++ dtDl "Iterations" (text <| String.fromInt iterations)
                                ++ maybeDtDl "Polarisation" (Maybe.map (text << polarisationToDescription) polarisation)
                                ++ boolDtDl "Reject bad patterns according to ΔCC½" noDeltaCcHalf
                                ++ maybeDtDl "Detector saturation cutoff" (Maybe.map (text << formatFloatHumanFriendly) maxAdu)
                                ++ dtDl "Minimum number of measurements per merged reflection" (text <| String.fromInt minMeasurements)
                                ++ boolDtDl "Write partiality model diagnostics" logs
                                ++ maybeDtDl "Require minimum estimated pattern resolution" (Maybe.map (text << formatFloatHumanFriendly) minRes)
                                ++ maybeDtDl "Exclude measurements above resolution limit" (Maybe.map (text << formatFloatHumanFriendly) pushRes)
                                ++ maybeDtDl "Indexing assignment refinement" (Maybe.map text w)
                                ++ dtDl "Reject crystals with absolute B factors ≥ Å²" (text <| String.fromFloat <| relB)
                                ++ boolDtDl "Disable the orientation/physics model part of the refinement calculation" noPr
                                ++ maybeDtDl "Start after crystals" (Maybe.map (text << String.fromInt) startAfter)
                                ++ maybeDtDl "Stop after crystals" (Maybe.map (text << String.fromInt) stopAfter)
                            )
                        ]

                showDatasetByAttributiFilters =
                    let
                        attrValuePossibleFilters =
                            List.map (\( attrId, attrValue ) -> { expType = experimentType, attrId = attrId, attrValue = attrValue }) (Dict.toList <| convertAttributoMapFromApi dataSet.attributi)
                    in
                    List.Extra.allDifferent (attrValuePossibleFilters ++ hiddenExperimentTypeAttributoFilters)

                showEmptyDatasets =
                    case selectedMergeStatus of
                        Just Merged ->
                            False

                        Just Unmerged ->
                            True

                        Nothing ->
                            True

                showDatasetsWithMergeJobs =
                    case selectedMergeStatus of
                        Just Merged ->
                            True

                        Just Unmerged ->
                            False

                        Nothing ->
                            True

                viewMergeResultRow : MergeResultWrapper -> Html Msg
                viewMergeResultRow mrw =
                    let
                        remainingHeaders =
                            List.length mergeRowHeaders - 1

                        id =
                            mrw.mergeResult.id

                        parameters =
                            mrw.mergeResult.parameters

                        runs =
                            mrw.mergeResult.runs

                        isShowingMergeParams =
                            mrw.showResults
                    in
                    tr_ <|
                        [ td_ [ text (String.fromInt id) ]
                        , td_
                            [ span [ class "accordion accordion-flush" ]
                                [ div [ class "accordion-item" ]
                                    [ div [ class "accordion-header" ]
                                        [ button
                                            [ class
                                                ("accordion-button accordion-merge-parameters-header-button"
                                                    ++ (if isShowingMergeParams then
                                                            ""

                                                        else
                                                            " collapsed"
                                                       )
                                                )
                                            , type_ "button"
                                            , onClick (ToggleAccordionShowModelParameters id)
                                            ]
                                            [ span [] [ text "Show merge parameters" ]
                                            ]
                                        ]
                                    , div
                                        [ class
                                            ("accordion-collapse collapse "
                                                ++ (if isShowingMergeParams then
                                                        " show"

                                                    else
                                                        ""
                                                   )
                                            )
                                        ]
                                        [ div [ class "accordion-body" ]
                                            [ viewMergeParameters parameters
                                            ]
                                        ]
                                    ]
                                ]
                            ]
                        , td [ class "text-nowrap" ] (List.intersperse br_ <| List.map text runs)
                        ]
                            ++ (case createMergeResultUnion mrw.mergeResult of
                                    Just (MergeResultStateRunning { started }) ->
                                        [ td
                                            [ colspan remainingHeaders ]
                                            [ div [ class "spinner-border spinner-border-sm text-primary" ] []
                                            , em [ class "mb-3" ]
                                                [ text " Running for "
                                                , text <|
                                                    posixDiffHumanFriendly hereAndNow.now (millisToPosix started)
                                                ]
                                            ]
                                        ]

                                    Just (MergeResultStateError { error }) ->
                                        [ td [ colspan remainingHeaders ] [ span_ [ text <| "Error: " ++ error ++ "." ] ] ]

                                    Just (MergeResultStateDone { started, stopped, result }) ->
                                        let
                                            refinementResults =
                                                mrw.mergeResult.refinementResults

                                            floatWithShell overall outer =
                                                td_ [ text <| formatFloatHumanFriendly overall ++ " (" ++ formatFloatHumanFriendly outer ++ ")" ]

                                            fom =
                                                result.fom

                                            mtzFileId =
                                                result.mtzFileId

                                            shells =
                                                result.detailedFoms
                                        in
                                        [ td_ [ text <| String.fromInt <| posixDiffMinutes (millisToPosix stopped) (millisToPosix started) ]
                                        , td_
                                            [ text <|
                                                formatFloatHumanFriendly fom.oneOverDFrom
                                                    ++ "–"
                                                    ++ formatFloatHumanFriendly fom.oneOverDTo
                                                    ++ " ("
                                                    ++ formatFloatHumanFriendly fom.outerShell.minRes
                                                    ++ "–"
                                                    ++ formatFloatHumanFriendly fom.outerShell.maxRes
                                                    ++ ")"
                                            ]
                                        , floatWithShell fom.completeness fom.outerShell.completeness
                                        , floatWithShell fom.redundancy fom.outerShell.redundancy
                                        , floatWithShell fom.cc fom.outerShell.cc
                                        , floatWithShell fom.ccstar fom.outerShell.ccstar
                                        , td_ [ text <| Maybe.withDefault "" <| Maybe.map formatFloatHumanFriendly fom.wilson ]
                                        , td_
                                            [ icon { name = "file-binary" }
                                            , a [ href (makeFilesLink mtzFileId) ] [ text "MTZ" ]
                                            ]
                                        , td_
                                            [ icon { name = "card-list" }
                                            , a
                                                [ class "link-primary"
                                                , onClick (OpenMergeResultDetail { id = id, fom = fom, shell_foms = shells, refinementResults = refinementResults })
                                                ]
                                                [ text "Details" ]
                                            ]
                                        ]

                                    _ ->
                                        [ td
                                            [ colspan remainingHeaders ]
                                            [ div [ class "spinner-border spinner-border-sm text-secondary" ] [], span_ [ text " In queue..." ] ]
                                        ]
                               )
            in
            if ((showDatasetsWithMergeJobs && (not <| List.isEmpty mergeResultWrappers)) || (showEmptyDatasets && List.isEmpty mergeResultWrappers)) && showDatasetByAttributiFilters then
                [ let
                    currentRowIsMerging =
                        case activatedMergeForm of
                            Just { dataSetId } ->
                                dataSetId == dataSet.id

                            _ ->
                                False

                    actions =
                        if currentRowIsMerging then
                            em [ class "text-muted" ] [ text "Enter merging parameters..." ]

                        else
                            let
                                mergeNotPossible =
                                    experimentTypeResults.numberOfIndexingResults == 0

                                mergeButtons =
                                    if mergeNotPossible then
                                        div_ []

                                    else
                                        div [ class "btn-group" ]
                                            [ button
                                                [ type_ "button"
                                                , class "btn btn-sm btn-outline-primary"
                                                , onClick (QuickStartMerge dataSet.id)
                                                , disabled (mergeRequestIsLoading mergeRequest)
                                                ]
                                                [ icon { name = "send-exclamation" }, text <| " Quick Merge" ]
                                            , button
                                                [ type_ "button"
                                                , class "btn btn-sm btn-outline-secondary"
                                                , onClick (StartMerge dataSet.id)
                                                , disabled (mergeRequestIsLoading mergeRequest)
                                                ]
                                                [ icon { name = "send" }, text <| " Merge" ]
                                            ]
                            in
                            mergeButtons
                  in
                  tr
                    (if List.isEmpty mergeResultWrappers && not currentRowIsMerging then
                        [ style "border-bottom" "1pt solid black" ]

                     else
                        []
                    )
                    [ td_ [ text (String.fromInt dataSet.id) ]
                    , td_ [ viewDataSetTable attributi hereAndNow.zone chemicalIdsToName (convertAttributoMapFromApi dataSet.attributi) False Nothing ]
                    , td_ (List.intersperse br_ <| List.map text experimentTypeResults.runs)
                    , td_
                        [ text <|
                            if List.isEmpty experimentTypeResults.runs then
                                ""

                            else
                                MaybeExtra.unwrap "" (\summary -> formatFloatHumanFriendly summary.hitRate ++ "%") dataSet.summary
                        ]
                    , td_
                        [ text <|
                            if List.isEmpty experimentTypeResults.runs then
                                ""

                            else
                                MaybeExtra.unwrap "" (\summary -> formatFloatHumanFriendly summary.indexingRate ++ "%") dataSet.summary
                        ]
                    , td_
                        [ text <|
                            if List.isEmpty experimentTypeResults.runs then
                                ""

                            else
                                MaybeExtra.unwrap "" (\summary -> formatIntHumanFriendly summary.indexedFrames) dataSet.summary
                        ]
                    , td [ class "text-nowrap" ]
                        [ actions
                        ]
                    ]
                , case activatedMergeForm of
                    Just { dataSetId, mergeParameters } ->
                        if dataSetId == dataSet.id then
                            tr
                                [ style "border-bottom" "1pt solid black" ]
                                [ td [ colspan 7, style "padding-left" "2em", class "text-muted" ]
                                    [ Html.map CrystFELMergeMessage (CrystFELMerge.view mergeParameters)
                                    , div_
                                        [ button [ type_ "button", class "btn btn-primary me-3", onClick (SubmitMerge dataSetId mergeParameters) ]
                                            [ icon { name = "send" }, text " Start Merge" ]
                                        , button [ type_ "button", class "btn btn-secondary", onClick CancelMerge ]
                                            [ icon { name = "x-lg" }, text " Cancel" ]
                                        ]
                                    ]
                                ]

                        else
                            text ""

                    _ ->
                        text ""
                , if List.isEmpty mergeResultWrappers then
                    text ""

                  else
                    tr [ style "border-bottom" "1pt solid black" ]
                        [ td [ colspan 7, style "padding-left" "2em", class "text-muted" ]
                            [ h5_ [ icon { name = "diagram-2" }, text "Merge results" ]
                            , case mergeRequest of
                                Nothing ->
                                    text ""

                                Just { request } ->
                                    case request of
                                        Failure e ->
                                            div_ [ makeAlert [ AlertDanger ] [ showHttpError e ] ]

                                        _ ->
                                            text ""
                            , table
                                [ class "table table-sm text-muted", style "font-size" "0.8rem", style "margin-bottom" "4rem" ]
                                [ thead_ <| [ tr_ (List.map (\header -> th_ [ header ]) mergeRowHeaders) ]
                                , tbody_ <| List.map viewMergeResultRow mergeResultWrappers
                                ]
                            ]
                        ]
                ]

            else
                []

        experimentTypeAttributi : Set AttributoId
        experimentTypeAttributi =
            Set.fromList <| List.concatMap (List.map .attributoId << .attributi << .dataSet) dataSets

        attributoValueSelector : AttributoId -> AttributoValue -> String
        attributoValueSelector attrId attrValue =
            case List.Extra.find (\a -> a.id == attrId) attributi of
                Nothing ->
                    "what is this? " ++ String.fromInt attrId

                Just attr ->
                    if attributoIsChemicalId attr.type_ then
                        case String.toInt (prettyPrintAttributoValue attrValue) of
                            Nothing ->
                                prettyPrintAttributoValue attrValue

                            Just chemicalId ->
                                case Dict.get chemicalId chemicalIdsToName of
                                    Nothing ->
                                        prettyPrintAttributoValue attrValue

                                    Just ch ->
                                        ch

                    else
                        prettyPrintAttributoValue attrValue

        checkAttributeFilterIsPresent : ExperimentTypeAttributoFilter -> List ExperimentTypeAttributoFilter -> Bool
        checkAttributeFilterIsPresent { expType, attrId, attrValue } hiddenFilters =
            List.member { expType = expType, attrId = attrId, attrValue = attrValue } hiddenFilters

        allNoneResetCheckButton : AttributoId -> List AttributoValue -> Html Msg
        allNoneResetCheckButton attributoId valuesForAttributo =
            let
                filterFromAttributoPair : AttributoId -> AttributoValue -> ExperimentTypeAttributoFilter
                filterFromAttributoPair aid avalue =
                    { expType = experimentType, attrId = aid, attrValue = avalue }

                attrFilters : List ExperimentTypeAttributoFilter
                attrFilters =
                    List.map
                        (filterFromAttributoPair attributoId)
                        valuesForAttributo
            in
            li_
                [ div [ class "dropdown-item btn-group" ]
                    [ button
                        [ type_ "button"
                        , class "btn btn-sm btn-outline-primary"
                        , onClick (SetFilterExperimentTypeAllAttributi attrFilters)
                        ]
                        [ text "All" ]
                    , button
                        [ type_ "button"
                        , class "btn btn-sm btn-outline-secondary"
                        , onClick (SetFilterExperimentTypeNoneAttributi attrFilters)
                        ]
                        [ text "None" ]
                    ]
                ]

        checkboxForOneAttributoValue : ( AttributoId, AttributoValue ) -> Html Msg
        checkboxForOneAttributoValue ( attrId, attrValue ) =
            let
                selectedAttributoValue =
                    { expType = experimentType, attrId = attrId, attrValue = attrValue }
            in
            li_
                [ div [ class "dropdown-item" ]
                    [ div [ class "form-check" ]
                        [ input
                            [ class "form-check-input"
                            , type_ "checkbox"
                            , for (String.fromInt attrId)
                            , onInput (SetFilterExperimentTypeAttributo selectedAttributoValue)
                            , checked <| not <| checkAttributeFilterIsPresent selectedAttributoValue hiddenExperimentTypeAttributoFilters
                            ]
                            []
                        , label [ class "form-check-label" ] [ text <| attributoValueSelector attrId attrValue ]
                        ]
                    ]
                ]

        dictMapValues : (b -> c) -> Dict a b -> Dict a c
        dictMapValues f =
            Dict.map (\_ -> f)

        dictAttrIdValues : Dict AttributoId (List AttributoValue)
        dictAttrIdValues =
            List.concatMap (Dict.toList << convertAttributoMapFromApi << .attributi << .dataSet) dataSets
                |> List.Extra.unique
                |> Dict.Extra.groupBy Tuple.first
                |> dictMapValues (List.map Tuple.second)

        dropdownForAttributo : Attributo AttributoType -> Html Msg
        dropdownForAttributo attributo =
            let
                attributoValueCheckBoxes : AttributoId -> List AttributoValue -> List (Html Msg)
                attributoValueCheckBoxes attrName attrValues =
                    List.map (\v -> checkboxForOneAttributoValue ( attrName, v )) attrValues
            in
            case Dict.get attributo.id dictAttrIdValues of
                Nothing ->
                    span_ []

                Just listValues ->
                    let
                        checkBoxes =
                            attributoValueCheckBoxes attributo.id listValues
                    in
                    if List.length checkBoxes > 1 then
                        span [ class "dropdown px-1" ]
                            [ button
                                [ class "btn btn-sm  btn-outline-secondary dropdown-toggle dropdown-toggle-split"
                                , attribute "data-bs-toggle" "dropdown"
                                , attribute "data-bs-auto-close" "outside"
                                ]
                                [ text (attributo.name ++ " ") ]
                            , ul [ class "dropdown-menu" ] <|
                                allNoneResetCheckButton attributo.id listValues
                                    :: checkBoxes
                            ]

                    else
                        span_ []

        attributiFilters : List (Html Msg)
        attributiFilters =
            List.map dropdownForAttributo <| List.filterMap (\aid -> List.Extra.find (\a -> a.id == aid) attributi) <| Set.toList experimentTypeAttributi
    in
    div_
        [ h2 [ class "mt-2 mb-4", style "text-shadow" "0.5px 0.5px 1px gray" ] [ span_ [ text experimentType.name ] ]
        , div [ class "pb-3", style "border-bottom" "1pt solid lightgray" ] <|
            label [ style "width" "10rem" ] [ icon { name = "card-list" }, text " Attributi Filter " ]
                :: attributiFilters
        , table [ class "table amarcord-table-fix-head table-borderless" ]
            [ thead_
                [ tr_
                    [ th [ onClick SortDataSetsAscending ]
                        [ text "ID"
                        , icon
                            { name =
                                if datasetSortingIdAscending then
                                    "caret-down-fill"

                                else
                                    "caret-up-fill"
                            }
                        ]
                    , th_ [ text "Attributi" ]
                    , th_ [ text "Runs" ]
                    , th_ [ abbr [ title "Hit Rate" ] [ text "HR" ] ]
                    , th_ [ abbr [ title "Indexing Rate" ] [ text "IR" ] ]
                    , th_ [ abbr [ title "Indexed Frames" ] [ text "Ind.F." ] ]
                    , th_ [ text "Actions" ]
                    ]
                ]
            , let
                sortedDatasets =
                    List.sortWith (\a b -> compare a.dataSet.id b.dataSet.id) dataSets

                requestedSortedDatasets : List JsonAnalysisDataSet
                requestedSortedDatasets =
                    if datasetSortingIdAscending then
                        List.reverse <| sortedDatasets

                    else
                        sortedDatasets
              in
              tbody_ <| List.concatMap viewDataSet requestedSortedDatasets
            ]
        ]


modalMergeResultDetail : Model -> Html Msg
modalMergeResultDetail m =
    case m.selectedMergeResult of
        MergeResultSelected mr ->
            Dialog.view
                (Just <|
                    { header = Just (span_ [ text "Merge results overview - ID: ", text <| String.fromInt mr.id ])
                    , body = Just <| span_ [ modalBodyShells mr.fom mr.shell_foms mr.refinementResults ]
                    , closeMessage = Just CloseMergeResultDetail
                    , containerClass = Nothing
                    , modalDialogClass = Just "modal-dialog modal-xl"
                    , footer = Nothing
                    }
                )

        NoMergeResultSelected ->
            text ""


modalBodyShells : MergeResultFom -> List MergeResultShell -> List JsonRefinementResult -> Html Msg
modalBodyShells fom shells refinementResults =
    let
        singleShellRow : MergeResultShell -> Html Msg
        singleShellRow shellRow =
            tr_
                [ td_ [ text <| formatFloatHumanFriendly shellRow.minRes ++ "–" ++ formatFloatHumanFriendly shellRow.maxRes ]
                , td_ [ text <| formatIntHumanFriendly shellRow.nref ]
                , td_ [ text <| formatIntHumanFriendly shellRow.reflectionsPossible ]
                , td_ [ text <| formatFloatHumanFriendly shellRow.completeness ]
                , td_ [ text <| formatFloatHumanFriendly shellRow.redundancy ]
                , td_ [ text <| formatFloatHumanFriendly shellRow.snr ]
                , td_ [ text <| formatFloatHumanFriendly shellRow.rSplit ]
                , td_ [ text <| formatFloatHumanFriendly shellRow.cc ]
                , td_ [ text <| formatFloatHumanFriendly shellRow.ccstar ]
                ]

        overallRow =
            tr [ class "table-success" ]
                [ td_ [ text <| formatFloatHumanFriendly fom.oneOverDFrom ++ "–" ++ formatFloatHumanFriendly fom.oneOverDTo ]
                , td_ [ text <| formatIntHumanFriendly fom.reflectionsTotal ]
                , td_ [ text <| formatIntHumanFriendly fom.reflectionsPossible ]
                , td_ [ text <| formatFloatHumanFriendly fom.completeness ]
                , td_ [ text <| formatFloatHumanFriendly fom.redundancy ]
                , td_ [ text <| formatFloatHumanFriendly fom.snr ]
                , td_ [ text <| formatFloatHumanFriendly fom.rSplit ]
                , td_ [ text <| formatFloatHumanFriendly fom.cc ]
                , td_ [ text <| formatFloatHumanFriendly fom.ccstar ]
                ]

        uglymol : Int -> Int -> String -> Html msg
        uglymol pdbId mtzId prefix =
            node "uglymol-viewer" [ attribute "pdbid" (String.fromInt pdbId), attribute "mtzid" (String.fromInt mtzId), attribute "idprefix" prefix ] []

        viewRefinementResult : JsonRefinementResult -> Html msg
        viewRefinementResult { id, pdbFileId, mtzFileId, rFree, rWork, rmsBondAngle, rmsBondLength } =
            div_
                [ uglymol pdbFileId mtzFileId ("refinement-" ++ String.fromInt id)
                , div [ class "d-flex" ]
                    [ span_ [ icon { name = "file-binary" }, a [ href (makeFilesLink pdbFileId) ] [ text "PDB" ] ]
                    , span_ [ icon { name = "file-binary" }, a [ href (makeFilesLink mtzFileId) ] [ text "MTZ" ] ]
                    ]
                , div_
                    [ table [ class "table table-sm" ]
                        [ thead_
                            [ tr_
                                [ th_ [ text "R", Html.sub [] [ text "free" ] ]
                                , th_ [ text "R", Html.sub [] [ text "work" ] ]
                                , th_ [ text "RMS Bond Angle" ]
                                , th_ [ text "RMS Bond Length" ]
                                ]
                            ]
                        , tbody_
                            [ tr_
                                [ td_ [ text (formatFloatHumanFriendly rFree) ]
                                , td_ [ text (formatFloatHumanFriendly rWork) ]
                                , td_ [ text (formatFloatHumanFriendly rmsBondAngle) ]
                                , td_ [ text (formatFloatHumanFriendly rmsBondLength) ]
                                ]
                            ]
                        ]
                    ]
                ]
    in
    div_ <|
        [ h5_ [ text "Figures of merit" ]
        , table [ class "table table-striped table-sm" ]
            [ thead_
                [ tr_
                    [ th_ [ text "Resolution (Å)" ]
                    , th_ [ text "Reflections" ]
                    , th_ [ text "Possible reflections" ]
                    , th_ [ text "Completeness (%)" ]
                    , th_ [ text "Multiplicity" ]
                    , th_ [ text "SNR" ]
                    , th_ [ text "R", Html.sub [] [ text "split" ] ]
                    , th_ [ text "CC", Html.sub [] [ text "1/2" ] ]
                    , th_ [ text "CC", sup [] [ text "*" ] ]
                    ]
                ]
            , tbody_ (List.append (List.map singleShellRow (List.sortBy .oneOverDCentre shells)) [ overallRow ])
            ]
        ]
            ++ (if List.isEmpty refinementResults then
                    []

                else
                    [ h5_ [ text "Refinement results" ]
                    , p [ class "text-muted" ] [ text "The view below controls just like Coot! However, sulphur is more green, and here we also have colors for Mg, P, Cl, Ca, Mn, Fe, Ni." ]
                    ]
                        ++ List.map viewRefinementResult refinementResults
               )


view : Model -> Html Msg
view model =
    div [ class "container" ] <|
        case model.analysisRequest of
            NotAsked ->
                List.singleton <| text ""

            Loading ->
                List.singleton <| loadingBar "Loading analysis results..."

            Failure e ->
                List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve Attributi" ], showHttpError e ]

            Success r ->
                modalMergeResultDetail model
                    :: viewFilters
                        model.hiddenExperimentTypeIds
                        model.selectMergeStatus
                        r
                    :: viewInner
                        model.hereAndNow
                        model.mergeRequest
                        model.activatedMergeForm
                        model.selectMergeStatus
                        model.hiddenExperimentTypeIds
                        model.hiddenParametersMergeResultIds
                        model.hiddenExperimentTypeAttributiFilters
                        model.dataSetsSortingAscending
                        r


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        AnalysisResultsReceived analysisResults ->
            ( { model | analysisRequest = fromResult analysisResults }, Cmd.none )

        StartMerge dataSetId ->
            ( { model | activatedMergeForm = Just { dataSetId = dataSetId, mergeParameters = CrystFELMerge.init model.beamtimeId } }, Cmd.none )

        QuickStartMerge dataSetId ->
            ( { model | activatedMergeForm = Nothing, mergeRequest = Just { dataSetId = dataSetId, request = Loading } }
            , send MergeFinished (startMergeJobForDataSetApiMergingDataSetIdStartPost dataSetId (CrystFELMerge.quickMergeParameters model.beamtimeId))
            )

        CancelMerge ->
            ( { model | activatedMergeForm = Nothing }, Cmd.none )

        SubmitMerge dataSetId mergeParameters ->
            case model.activatedMergeForm of
                Nothing ->
                    ( model, Cmd.none )

                Just _ ->
                    ( { model | mergeRequest = Just { dataSetId = dataSetId, request = Loading }, activatedMergeForm = Nothing }
                    , send MergeFinished (startMergeJobForDataSetApiMergingDataSetIdStartPost dataSetId (modelToMergeParameters mergeParameters))
                    )

        MergeFinished result ->
            case model.mergeRequest of
                Nothing ->
                    ( model, Cmd.none )

                Just { dataSetId } ->
                    ( { model
                        | mergeRequest =
                            Just
                                { dataSetId = dataSetId
                                , request = RemoteData.fromResult result
                                }
                      }
                    , send AnalysisResultsReceived (readAnalysisResultsApiAnalysisAnalysisResultsBeamtimeIdGet model.beamtimeId)
                    )

        Refresh posix ->
            let
                newHereAndNow =
                    { now = posix, zone = model.hereAndNow.zone }
            in
            ( { model | hereAndNow = newHereAndNow }, send AnalysisResultsReceived (readAnalysisResultsApiAnalysisAnalysisResultsBeamtimeIdGet model.beamtimeId) )

        OpenMergeResultDetail mr ->
            ( { model | selectedMergeResult = MergeResultSelected mr }, Cmd.none )

        CloseMergeResultDetail ->
            ( { model | selectedMergeResult = NoMergeResultSelected }, Cmd.none )

        CrystFELMergeMessage cfMsg ->
            case model.activatedMergeForm of
                Nothing ->
                    ( model, Cmd.none )

                Just { dataSetId, mergeParameters } ->
                    let
                        ( cfModel, cfCmd ) =
                            CrystFELMerge.update cfMsg mergeParameters
                    in
                    ( { model
                        | activatedMergeForm =
                            Just
                                { dataSetId = dataSetId
                                , mergeParameters = cfModel
                                }
                      }
                    , Cmd.map CrystFELMergeMessage cfCmd
                    )

        ToggleAccordionShowModelParameters mrid ->
            let
                hiddenMergeJobParamsIds =
                    model.hiddenParametersMergeResultIds
            in
            if Set.member mrid hiddenMergeJobParamsIds then
                ( { model | hiddenParametersMergeResultIds = Set.remove mrid hiddenMergeJobParamsIds }, Cmd.none )

            else
                ( { model | hiddenParametersMergeResultIds = Set.insert mrid hiddenMergeJobParamsIds }, Cmd.none )

        SortDataSetsAscending ->
            let
                dss =
                    model.dataSetsSortingAscending
            in
            ( { model | dataSetsSortingAscending = not dss }, Cmd.none )

        SetFilterExperimentType experimentTypeId ->
            let
                hiddenExpTypes =
                    model.hiddenExperimentTypeIds
            in
            if Set.member experimentTypeId hiddenExpTypes then
                ( { model | hiddenExperimentTypeIds = Set.remove experimentTypeId hiddenExpTypes }, Cmd.none )

            else
                ( { model | hiddenExperimentTypeIds = Set.insert experimentTypeId hiddenExpTypes }, Cmd.none )

        SetFilterMergeStatus ms ->
            ( { model | selectMergeStatus = ms }, Cmd.none )

        SetFilterExperimentTypeAttributo attrFilter _ ->
            let
                hlfs =
                    model.hiddenExperimentTypeAttributiFilters
            in
            if List.member attrFilter model.hiddenExperimentTypeAttributiFilters then
                ( { model
                    | hiddenExperimentTypeAttributiFilters =
                        List.Extra.unique <|
                            List.Extra.remove attrFilter hlfs
                  }
                , Cmd.none
                )

            else
                ( { model | hiddenExperimentTypeAttributiFilters = List.Extra.unique (attrFilter :: hlfs) }, Cmd.none )

        SetFilterExperimentTypeAllAttributi attrFilters ->
            let
                hlfs =
                    model.hiddenExperimentTypeAttributiFilters

                newHlfs =
                    List.filter (\af -> List.Extra.notMember af attrFilters) hlfs
            in
            ( { model | hiddenExperimentTypeAttributiFilters = List.Extra.unique <| newHlfs }, Cmd.none )

        SetFilterExperimentTypeNoneAttributi attrFilters ->
            let
                hlfs =
                    model.hiddenExperimentTypeAttributiFilters

                newHlfs =
                    List.Extra.unique (hlfs ++ attrFilters)
            in
            ( { model | hiddenExperimentTypeAttributiFilters = List.Extra.unique <| newHlfs }, Cmd.none )
