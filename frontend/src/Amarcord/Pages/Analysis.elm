module Amarcord.Pages.Analysis exposing (Model, Msg(..), init, update, view)

import Amarcord.API.DataSet exposing (DataSetId)
import Amarcord.API.ExperimentType exposing (ExperimentTypeId)
import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.Attributo exposing (Attributo, AttributoId, AttributoType, AttributoValue, attributoIsChemicalId, convertAttributoFromApi, convertAttributoMapFromApi, convertAttributoValueFromApi, prettyPrintAttributoValue)
import Amarcord.AttributoHtml exposing (formatFloatHumanFriendly, formatIntHumanFriendly)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, viewAlert)
import Amarcord.CommandLineParser exposing (coparseCommandLine)
import Amarcord.CrystFELMerge as CrystFELMerge exposing (mergeModelToString, modelToMergeParameters)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Dialog as Dialog
import Amarcord.Html exposing (br_, code_, div_, em_, h5_, hr_, input_, li_, p_, span_, strongText, tbody_, td_, th_, thead_, tr_)
import Amarcord.HttpError exposing (HttpError(..), send, showError)
import Amarcord.IndexingParameters as IndexingParameters
import Amarcord.Route as Route exposing (makeFilesLink, makeIndexingIdErrorLogLink, makeIndexingIdLogLink)
import Amarcord.Util exposing (HereAndNow, none, posixDiffHumanFriendly, posixDiffMinutes)
import Api.Data exposing (DBJobStatus(..), JsonCreateIndexingForDataSetOutput, JsonDataSet, JsonDataSetWithIndexingResults, JsonExperimentType, JsonIndexingParameters, JsonIndexingParametersWithResults, JsonIndexingResult, JsonMergeParameters, JsonMergeResult, JsonMergeResultFom, JsonMergeResultShell, JsonMergeResultStateDone, JsonMergeResultStateError, JsonMergeResultStateQueued, JsonMergeResultStateRunning, JsonPolarisation, JsonQueueMergeJobOutput, JsonReadAnalysisResults, JsonReadExperimentTypes, JsonReadIndexingParametersOutput, JsonRefinementResult, ScaleIntensities(..))
import Api.Request.Analysis exposing (readAnalysisResultsApiAnalysisAnalysisResultsBeamtimeIdExperimentTypeIdGet)
import Api.Request.Experimenttypes exposing (readExperimentTypesApiExperimentTypesBeamtimeIdGet)
import Api.Request.Merging exposing (queueMergeJobApiMergingPost)
import Api.Request.Processing exposing (indexingJobQueueForDataSetApiIndexingPost, readIndexingParametersApiIndexingParametersDataSetIdGet)
import Basics.Extra exposing (safeDivide)
import Browser.Navigation as Nav
import Dict exposing (Dict)
import Dict.Extra
import Html exposing (Html, a, button, dd, div, dl, dt, em, form, h4, img, input, label, node, p, small, span, sup, table, td, text, th, tr, ul)
import Html.Attributes exposing (attribute, checked, class, colspan, disabled, for, href, id, src, style, type_)
import Html.Events exposing (onClick, onInput)
import List.Extra
import Maybe
import Maybe.Extra
import RemoteData exposing (RemoteData(..), fromResult, isLoading)
import Result.Extra
import Scroll exposing (scrollY)
import Set exposing (Set)
import String
import Task
import Time exposing (Posix, millisToPosix, posixToMillis)


type alias IndexingParametersId =
    Int


type alias ProcessingParametersInput =
    { dataSetId : DataSetId
    , indexingParamFormModel : IndexingParameters.Model
    }


type Msg
    = AnalysisResultsReceived (Result HttpError JsonReadAnalysisResults)
    | OpenMergeForm DataSetId IndexingParametersId
    | OpenDataSet DataSetId
    | OpenProcessingFormWithExisting JsonIndexingParameters DataSetId
    | CloseDataSet
    | ToggleIndexingParameterExpansion Int
    | IndexingParametersMsg IndexingParameters.Msg
    | ProcessingParametersWithExistingReceived JsonIndexingParameters DataSetId (Result HttpError JsonReadIndexingParametersOutput)
    | ProcessingParametersReceived (Result HttpError JsonReadIndexingParametersOutput)
    | ExperimentTypesReceived (Result HttpError JsonReadExperimentTypes)
    | SubmitQuickMerge DataSetId IndexingParametersId
    | ProcessingSubmitted (Result HttpError JsonCreateIndexingForDataSetOutput)
    | CloseMergeForm
    | SubmitMerge DataSetId CrystFELMerge.Model
    | SubmitProcessing
    | MergeFinished (Result HttpError JsonQueueMergeJobOutput)
    | Refresh Posix
    | ScrollDone
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
    | OpenProcessingForm Int
    | CancelProcessing


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
    { mergeParameters : CrystFELMerge.Model
    }


type alias DetailMerge =
    { id : Int, fom : JsonMergeResultFom, shell_foms : List JsonMergeResultShell, refinementResults : List JsonRefinementResult }


type alias MergeRequest =
    { dataSetId : Int
    , indexingParametersId : Int
    , request : RemoteData HttpError JsonQueueMergeJobOutput
    }


type alias ExperimentTypeAttributoFilter =
    { attrId : AttributoId, attrValue : AttributoValue }


type alias Model =
    { hereAndNow : HereAndNow
    , navKey : Nav.Key
    , experimentTypesRequest : RemoteData HttpError JsonReadExperimentTypes
    , processingParametersRequest : RemoteData HttpError JsonReadIndexingParametersOutput
    , submitProcessingRequest : RemoteData HttpError JsonCreateIndexingForDataSetOutput
    , analysisRequest : RemoteData HttpError JsonReadAnalysisResults
    , selectedExperimentTypeId : Maybe Int
    , activatedMergeForm : Maybe ActivatedMergeForm
    , mergeRequest : Maybe MergeRequest
    , selectedMergeResult : SelectedMergeResult
    , expandedMergeResultIds : Set MergeResultId
    , hiddenExperimentTypeAttributiFilters : List ExperimentTypeAttributoFilter
    , selectMergeStatus : Maybe MergeStatus
    , dataSetsSortingAscending : Bool
    , beamtimeId : BeamtimeId
    , currentProcessingParameters : Maybe ProcessingParametersInput
    , selectedDataSetId : Maybe Int
    , expandedIndexingParameterIds : Set Int
    }


init : Nav.Key -> HereAndNow -> BeamtimeId -> Maybe ExperimentTypeId -> ( Model, Cmd Msg )
init navKey hereAndNow beamtimeId experimentTypeId =
    ( { hereAndNow = hereAndNow
      , navKey = navKey
      , analysisRequest = NotAsked
      , activatedMergeForm = Nothing
      , experimentTypesRequest = Loading
      , processingParametersRequest = NotAsked
      , mergeRequest = Nothing
      , submitProcessingRequest = NotAsked
      , selectedMergeResult = NoMergeResultSelected
      , selectedExperimentTypeId = experimentTypeId
      , expandedMergeResultIds = Set.empty
      , hiddenExperimentTypeAttributiFilters = []
      , selectMergeStatus = Nothing
      , dataSetsSortingAscending = True
      , beamtimeId = beamtimeId
      , currentProcessingParameters = Nothing
      , selectedDataSetId = Nothing
      , expandedIndexingParameterIds = Set.empty
      }
    , send ExperimentTypesReceived (readExperimentTypesApiExperimentTypesBeamtimeIdGet beamtimeId)
    )


viewFilters : Maybe MergeStatus -> Html Msg
viewFilters mergeStatus =
    let
        mergeFilterId =
            "merge_results_filter"

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
            [ label [ style "width" "10rem", for mergeFilterId ] [ icon { name = "diagram-2" }, text " Merge Status " ]
            , mergeResultPresentFilter
            ]
        ]


viewInner : Model -> JsonReadAnalysisResults -> Html Msg
viewInner model { attributi, chemicalIdToName, dataSets, experimentType } =
    viewResultsTableForSingleExperimentType
        model
        (List.map convertAttributoFromApi attributi)
        (List.foldr (\{ chemicalId, name } -> Dict.insert chemicalId name) Dict.empty chemicalIdToName)
        experimentType
        dataSets


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


viewMergeResultRow : List (Html msg) -> HereAndNow -> MergeResultWrapper -> Html Msg
viewMergeResultRow mergeRowHeaders hereAndNow mrw =
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


viewJobStatus : Bool -> DBJobStatus -> Html msg
viewJobStatus hasError x =
    case x of
        DBJobStatusQueued ->
            span [ class "badge text-bg-secondary" ] [ span [ class "spinner-border spinner-border-sm" ] [], text " queued" ]

        DBJobStatusRunning ->
            span [ class "badge text-bg-info" ] [ span [ class "spinner-border spinner-border-sm" ] [], text " running" ]

        DBJobStatusDone ->
            if hasError then
                span [ class "badge text-bg-warning" ] [ text "error" ]

            else
                span [ class "badge text-bg-success" ] [ text "success" ]


viewRate : Int -> Int -> Html cmd
viewRate part total =
    small [ class "text-muted" ]
        [ text <|
            formatFloatHumanFriendly
                (case safeDivide (toFloat part) (toFloat total) of
                    Nothing ->
                        0

                    Just rate ->
                        rate * 100.0
                )
                ++ "%"
        ]


viewIndexingResults : Posix -> List JsonIndexingResult -> Html Msg
viewIndexingResults now results =
    let
        tableHeaders : List String
        tableHeaders =
            [ "ID", "Run", "Status", "Frames", "Hits", "Ixed", "Other" ]

        viewHistogram fileId =
            div [ class "col" ]
                [ a
                    [ href (makeFilesLink fileId)
                    ]
                    [ img [ src (makeFilesLink fileId), class "img-fluid" ] [] ]
                ]

        viewJobDuration started stopped =
            Maybe.withDefault
                []
                (Maybe.map
                    (\startedPrime ->
                        [ br_
                        , span [ class "form-text" ]
                            [ em_
                                [ text
                                    (posixDiffHumanFriendly
                                        (millisToPosix (Maybe.withDefault (posixToMillis now) stopped))
                                        (millisToPosix startedPrime)
                                    )
                                ]
                            ]
                        ]
                    )
                    started
                )

        viewIndexingResultRow : JsonIndexingResult -> Html Msg
        viewIndexingResultRow { id, runExternalId, hasError, status, started, stopped, programVersion, frames, hits, indexedFrames, detectorShiftXMm, detectorShiftYMm, unitCellHistogramsFileId, generatedGeometryFile } =
            tr_
                [ td_ [ text (String.fromInt id) ]
                , td_ [ text (String.fromInt runExternalId) ]
                , td_
                    (viewJobStatus hasError status
                        :: viewJobDuration started stopped
                    )
                , td_
                    [ text
                        (if hasError then
                            ""

                         else
                            formatIntHumanFriendly frames
                        )
                    ]
                , td_
                    (if hasError then
                        [ text "" ]

                     else
                        [ text (formatIntHumanFriendly hits), br_, viewRate hits frames ]
                    )
                , td_
                    (if hasError then
                        [ text "" ]

                     else
                        [ text (formatIntHumanFriendly indexedFrames)
                        , br_
                        , viewRate indexedFrames hits
                        ]
                    )
                , td_
                    [ div_ <|
                        Maybe.withDefault [] <|
                            Maybe.map2
                                (\x y -> [ strongText "Detector shift: ", text (formatFloatHumanFriendly x ++ "mm, " ++ formatFloatHumanFriendly y ++ "mm"), br_ ])
                                detectorShiftXMm
                                detectorShiftYMm
                    , div_ <|
                        if String.isEmpty programVersion then
                            []

                        else
                            [ strongText "CrystFEL version: ", text programVersion ]
                    , span [ class "hstack gap-1" ]
                        [ a [ href (makeIndexingIdLogLink id) ] [ icon { name = "link-45deg" }, text " Show job log" ]
                        , if hasError then
                            a [ href (makeIndexingIdErrorLogLink id) ] [ icon { name = "link-45deg" }, text "Show error" ]

                          else
                            text ""
                        ]
                    , case unitCellHistogramsFileId of
                        Nothing ->
                            text ""

                        Just ucFileId ->
                            div_
                                [ strongText "Unit cell histograms:"
                                , viewHistogram ucFileId
                                ]
                    , if String.isEmpty generatedGeometryFile then
                        text ""

                      else
                        div_ [ strongText "Geometry file: ", text generatedGeometryFile ]
                    ]
                ]
    in
    table [ class "table table-sm" ]
        [ thead_ [ tr_ (List.map (th_ << List.singleton << text) tableHeaders) ]
        , tbody_ (List.map viewIndexingResultRow results)
        ]


mergeActions : { a | id : DataSetId } -> Maybe MergeRequest -> IndexingParametersId -> Html Msg
mergeActions dataSet mergeRequest indexingParametersId =
    let
        mergeRequestIsLoading : Maybe MergeRequest -> Bool
        mergeRequestIsLoading x =
            case x of
                Nothing ->
                    False

                Just { request } ->
                    isLoading request
    in
    div [ class "btn-group" ]
        [ button
            [ type_ "button"
            , class "btn btn-sm btn-outline-primary"
            , onClick (SubmitQuickMerge dataSet.id indexingParametersId)
            , disabled (mergeRequestIsLoading mergeRequest)
            ]
            [ icon { name = "send-exclamation" }, text <| " Quick Merge" ]
        , button
            [ type_ "button"
            , class "btn btn-sm btn-outline-secondary"
            , onClick (OpenMergeForm dataSet.id indexingParametersId)
            , disabled (mergeRequestIsLoading mergeRequest)
            ]
            [ icon { name = "send" }, text <| " Merge" ]
        ]


foldIntervals : List Int -> List ( Int, Int )
foldIntervals list =
    case list of
        [] ->
            []

        x :: _ ->
            let
                ( pairs, lastPair ) =
                    List.foldl f ( [], ( x, x ) ) list

                f : Int -> ( List ( Int, Int ), ( Int, Int ) ) -> ( List ( Int, Int ), ( Int, Int ) )
                f newNumber ( oldPairs, ( startSequence, endSequence ) ) =
                    if newNumber == endSequence then
                        ( oldPairs, ( startSequence, endSequence ) )

                    else if newNumber == endSequence + 1 then
                        ( oldPairs, ( startSequence, endSequence + 1 ) )

                    else
                        ( ( startSequence, endSequence ) :: oldPairs, ( newNumber, newNumber ) )
            in
            lastPair :: pairs


createRunRanges : List Int -> List String
createRunRanges =
    List.map
        (\( start, end ) ->
            if start == end then
                String.fromInt start

            else
                String.fromInt start ++ "-" ++ String.fromInt end
        )
        << foldIntervals
        << List.sort


viewSingleIndexingResultRow : Model -> JsonDataSet -> JsonIndexingParametersWithResults -> List (Html Msg)
viewSingleIndexingResultRow model dataSet ({ parameters, indexingResults, mergeResults } as p) =
    let
        detailsExpanded parametersId =
            Set.member parametersId model.expandedIndexingParameterIds

        hasJobsInProgress =
            List.any (\{ status } -> status == DBJobStatusRunning || status == DBJobStatusQueued) indexingResults

        processingInProgressButton =
            if hasJobsInProgress then
                button
                    [ disabled True, class "btn btn-outline-secondary" ]
                    [ div [ class "spinner-border text-secondary spinner-border-sm" ] [] ]

            else
                text ""

        hideShowDetailsButton parametersId =
            div [ class "btn-group" ]
                [ button
                    [ class "btn btn-sm btn-secondary"
                    , style "border-radius" "0"
                    , type_ "button"
                    , onClick (ToggleIndexingParameterExpansion parametersId)
                    ]
                    [ icon
                        { name =
                            if detailsExpanded parametersId then
                                "arrows-angle-contract"

                            else
                                "arrows-angle-expand"
                        }
                    , text
                        (if detailsExpanded parametersId then
                            " Hide indexing details"

                         else
                            " Show indexing details"
                        )
                    ]
                , processingInProgressButton
                ]
    in
    case parameters.id of
        Nothing ->
            []

        Just parametersId ->
            let
                successfulResults : List JsonIndexingResult
                successfulResults =
                    List.filter (\ir -> not ir.hasError) indexingResults

                frames : Int
                frames =
                    List.foldr (\new old -> new.frames + old) 0 successfulResults

                hits : Int
                hits =
                    List.foldr (\new old -> new.hits + old) 0 successfulResults

                indexedFrames : Int
                indexedFrames =
                    List.foldr (\new old -> new.indexedFrames + old) 0 successfulResults
            in
            [ tr_
                [ td_ [ text (String.fromInt parametersId) ]
                , td_ [ text <| String.join ", " (createRunRanges (List.map .runExternalId successfulResults)) ]
                , td_ [ text (formatIntHumanFriendly frames) ]
                , td_ [ text (formatIntHumanFriendly hits) ]
                , td_ [ text (formatIntHumanFriendly indexedFrames) ]
                ]
            , if Set.member parametersId model.expandedIndexingParameterIds then
                tr [ id ("indexing-params" ++ String.fromInt parametersId) ]
                    [ td [ colspan (List.length indexingAndMergeResultHeaders) ]
                        [ div_ [ hideShowDetailsButton parametersId ]
                        , div [ class "border shadow-sm p-3 bg-light" ] [ viewSingleIndexing model dataSet p ]
                        ]
                    ]

              else
                tr_ [ td [ colspan (List.length indexingAndMergeResultHeaders) ] [ div_ [ hideShowDetailsButton parametersId ] ] ]
            , tr_
                [ td [ colspan (List.length indexingAndMergeResultHeaders) ]
                    [ h5_ [ text "Merge Results" ]
                    , viewMergeResults model dataSet parameters mergeResults
                    ]
                ]
            ]


indexingAndMergeResultHeaders : List (Html msg)
indexingAndMergeResultHeaders =
    List.map text [ "ID", "Runs", "Frames", "Hits", "Ixed" ]


viewIndexingAndMergeResultsTable : Model -> JsonDataSet -> List JsonIndexingParametersWithResults -> Html Msg
viewIndexingAndMergeResultsTable model dataSet indexingParametersAndResults =
    table
        [ class "table table-borderless shadow-sm border p-3" ]
        [ thead_ <| [ tr_ (List.map (\header -> th_ [ header ]) indexingAndMergeResultHeaders) ]
        , tbody_ <| List.concatMap (viewSingleIndexingResultRow model dataSet) indexingParametersAndResults
        ]


viewMergeResultsTable : Model -> List JsonMergeResult -> Html Msg
viewMergeResultsTable model mergeResults =
    if List.isEmpty mergeResults then
        text ""

    else
        let
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

            mergeResultWrappers : List MergeResultWrapper
            mergeResultWrappers =
                List.map
                    (\mr -> { mergeResult = mr, showResults = Set.member mr.id model.expandedMergeResultIds })
                    mergeResults
        in
        table
            [ class "table table-sm text-muted", style "font-size" "0.8rem", style "margin-bottom" "4rem" ]
            [ thead_ <| [ tr_ (List.map (\header -> th_ [ header ]) mergeRowHeaders) ]
            , tbody_ <| List.map (viewMergeResultRow mergeRowHeaders model.hereAndNow) mergeResultWrappers
            ]


viewMergeResults : Model -> JsonDataSet -> JsonIndexingParameters -> List JsonMergeResult -> Html Msg
viewMergeResults model dataSet indexingParameters mergeResults =
    div_ <|
        [ case model.activatedMergeForm of
            Just { mergeParameters } ->
                div_
                    [ Html.map CrystFELMergeMessage (CrystFELMerge.view mergeParameters)
                    , div [ class "mb-3 hstack gap-3" ]
                        [ button [ type_ "button", class "btn btn-primary", onClick (SubmitMerge mergeParameters.dataSetId mergeParameters) ]
                            [ icon { name = "send" }, text " Start Merge" ]
                        , button [ type_ "button", class "btn btn-secondary", onClick CloseMergeForm ]
                            [ icon { name = "x-lg" }, text " Cancel" ]
                        ]
                    ]

            Nothing ->
                text ""
        , Maybe.withDefault (text "") <| Maybe.map (mergeActions dataSet model.mergeRequest) indexingParameters.id
        , case model.mergeRequest of
            Nothing ->
                text ""

            Just { request, dataSetId, indexingParametersId } ->
                if dataSetId == dataSet.id && Just indexingParametersId == indexingParameters.id then
                    case request of
                        Failure e ->
                            div_ [ viewAlert [ AlertDanger ] [ showError e ] ]

                        _ ->
                            text ""

                else
                    text ""
        , viewMergeResultsTable model mergeResults
        ]


viewSingleIndexing : Model -> JsonDataSet -> JsonIndexingParametersWithResults -> Html Msg
viewSingleIndexing model dataSet { parameters, indexingResults } =
    div_
        [ dl [ class "row" ]
            [ dt [ class "col-3" ] [ text "Online indexing?" ]
            , dd [ class "col-9" ]
                [ text
                    (if parameters.isOnline then
                        "yes"

                     else
                        "no"
                    )
                ]
            , dt [ class "col-3" ] [ text "Cell description" ]
            , dd [ class "col-9" ]
                [ case parameters.cellDescription of
                    Nothing ->
                        em_ [ text "none" ]

                    Just "" ->
                        em_ [ text "none" ]

                    Just cellDescription ->
                        text cellDescription
                ]
            , dt [ class "col-3" ] [ text "Geometry file" ]
            , dd [ class "col-9" ]
                [ text
                    (if String.isEmpty parameters.geometryFile then
                        "auto-detect"

                     else
                        parameters.geometryFile
                    )
                ]
            ]
        , p_ [ strongText "Command line: ", br_, code_ [ text parameters.commandLine ] ]
        , button
            [ class "btn btn-sm btn-dark mb-3"
            , type_ "button"
            , onClick
                (OpenProcessingFormWithExisting parameters dataSet.id)
            ]
            [ icon { name = "back" }, text " Open processing form with these parameters" ]
        , h5_ [ text "Indexing Results" ]
        , viewIndexingResults model.hereAndNow.now indexingResults
        ]


viewProcessingParameterForm : Model -> ProcessingParametersInput -> Html Msg
viewProcessingParameterForm model { indexingParamFormModel } =
    form [ class "mb-3 p-3 border shadow-sm", style "background-color" "white", id "processing-form" ]
        [ h4 [] [ icon { name = "send" }, text " Enter Processing Parameters" ]
        , p_
            [ text "The configuration below is used to start "
            , em_ [ text "indexamajig" ]
            , text ", which belongs to the "
            , a [ href "https://www.desy.de/~twhite/crystfel/index.html" ] [ text "CrystFEL" ]
            , text " software suite. "
            , em_ [ text "indexamajig" ]
            , text " takes a list of diffraction snapshots from crystals in random orientations and attempts to find peaks, index and integrate each one."
            ]
        , Html.map IndexingParametersMsg <| IndexingParameters.view indexingParamFormModel
        , div [ class "hstack gap-3 mb-3" ]
            [ button
                [ type_ "button"
                , class "btn btn-primary"
                , onClick SubmitProcessing
                , disabled
                    (isLoading model.submitProcessingRequest
                        || Result.Extra.isErr (IndexingParameters.toCommandLine indexingParamFormModel)
                        || IndexingParameters.isEditOpen indexingParamFormModel
                    )
                ]
                [ viewSpinnerOrIcon model.submitProcessingRequest
                , text " Start Processing Job"
                ]
            , button
                [ type_ "button"
                , class "btn btn-secondary"
                , onClick CancelProcessing
                , disabled (isLoading model.submitProcessingRequest)
                ]
                [ icon { name = "x-lg" }, text " Cancel" ]
            ]
        , case model.submitProcessingRequest of
            Failure e ->
                viewAlert [ AlertDanger ] <|
                    [ h4 [ class "alert-heading" ]
                        [ text "Failed to start processing job. Read the error message carefully!"
                        ]
                    , showError e
                    ]

            _ ->
                text ""
        ]


viewSpinnerOrIcon : RemoteData e a -> Html msg
viewSpinnerOrIcon request =
    if isLoading request then
        span [ class "spinner-border spinner-border-sm" ] []

    else
        icon { name = "send" }


viewDataSetProcessingButtons : Model -> JsonDataSet -> Html Msg
viewDataSetProcessingButtons model dataSet =
    case model.currentProcessingParameters of
        Nothing ->
            div_
                [ button
                    [ class "btn btn-primary mb-1"
                    , type_ "button"
                    , onClick (OpenProcessingForm dataSet.id)
                    , disabled (isLoading model.processingParametersRequest)
                    ]
                    [ viewSpinnerOrIcon model.processingParametersRequest
                    , text " Start new processing job"
                    ]
                , hr_
                ]

        Just _ ->
            text ""


viewProcessingResultsForDataSet : Model -> JsonDataSet -> List JsonIndexingParametersWithResults -> Html Msg
viewProcessingResultsForDataSet model dataSet indexingResults =
    div [ class "bg-light border shadow-sm p-3" ]
        [ viewDataSetProcessingButtons model dataSet
        , case model.processingParametersRequest of
            Failure e ->
                viewAlert [ AlertDanger ] <|
                    [ h4 [ class "alert-heading" ]
                        [ text "Failed to retrieve indexing parameters. Try reloading and if that doesn't work, contact the admins."
                        ]
                    , showError e
                    ]

            _ ->
                text ""
        , case model.submitProcessingRequest of
            Success { dataSetId } ->
                if dataSetId == dataSet.id then
                    p [ class "text-success" ] [ text "Job submitted!" ]

                else
                    text ""

            _ ->
                text ""
        , case model.currentProcessingParameters of
            Nothing ->
                text ""

            Just currentProcessingParameters ->
                viewProcessingParameterForm model currentProcessingParameters
        , div_ [ viewIndexingAndMergeResultsTable model dataSet indexingResults ]
        ]


viewDataSet :
    Model
    -> List (Attributo AttributoType)
    -> Dict Int String
    -> JsonDataSetWithIndexingResults
    -> List (Html Msg)
viewDataSet model attributi chemicalIdsToName { dataSet, runs, indexingResults } =
    [ tr []
        [ td_ [ text (String.fromInt dataSet.id) ]
        , td_
            [ viewDataSetTable attributi
                model.hereAndNow.zone
                chemicalIdsToName
                (convertAttributoMapFromApi dataSet.attributi)
                False
                False
                Nothing
            ]
        , td_ (List.intersperse br_ <| List.map text runs)
        ]
    , tr []
        [ td [ colspan 4 ]
            [ let
                dataSetHasJobsInProgress =
                    List.any
                        (\indexingResultsAndMergeResults ->
                            List.any (\{ status } -> status == DBJobStatusRunning || status == DBJobStatusQueued) indexingResultsAndMergeResults.indexingResults
                                || List.any (\{ stateRunning, stateQueued } -> Maybe.Extra.isJust stateRunning || Maybe.Extra.isJust stateQueued) indexingResultsAndMergeResults.mergeResults
                        )
                        indexingResults

                processingInProgressButton =
                    if dataSetHasJobsInProgress then
                        button
                            [ disabled True, class "btn btn-outline-secondary" ]
                            [ div [ class "spinner-border text-secondary spinner-border-sm" ] [] ]

                    else
                        text ""
              in
              if model.selectedDataSetId == Just dataSet.id then
                div_
                    [ div [ class "btn-group" ]
                        [ button [ class "btn btn-secondary", style "border-radius" "0", type_ "button", onClick CloseDataSet ]
                            [ icon { name = "arrows-angle-contract" }
                            , text <|
                                " "
                                    ++ (if List.isEmpty indexingResults then
                                            "not processed yet"

                                        else
                                            String.fromInt (List.length indexingResults) ++ " processing result(s)"
                                       )
                            ]
                        , processingInProgressButton
                        ]
                    , viewProcessingResultsForDataSet model dataSet indexingResults
                    ]

              else
                div [ class "btn-group" ]
                    [ button [ class "btn btn-secondary", style "border-radius" "0", type_ "button", onClick (OpenDataSet dataSet.id) ]
                        [ icon { name = "arrows-angle-expand" }
                        , text <|
                            " "
                                ++ (if List.isEmpty indexingResults then
                                        "not processed yet"

                                    else
                                        String.fromInt (List.length indexingResults) ++ " processing result(s)"
                                   )
                        ]
                    , processingInProgressButton
                    ]
            ]
        ]
    ]


viewResultsTableForSingleExperimentType :
    Model
    -> List (Attributo AttributoType)
    -> Dict Int String
    -> JsonExperimentType
    -> List JsonDataSetWithIndexingResults
    -> Html Msg
viewResultsTableForSingleExperimentType model attributi chemicalIdsToName experimentType dataSets =
    let
        experimentTypeAttributi : Set AttributoId
        experimentTypeAttributi =
            Set.fromList <| List.map .id experimentType.attributi

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
        checkAttributeFilterIsPresent { attrId, attrValue } hiddenFilters =
            List.member { attrId = attrId, attrValue = attrValue } hiddenFilters

        allNoneResetCheckButton : AttributoId -> List AttributoValue -> Html Msg
        allNoneResetCheckButton attributoId valuesForAttributo =
            let
                filterFromAttributoPair : AttributoId -> AttributoValue -> ExperimentTypeAttributoFilter
                filterFromAttributoPair aid avalue =
                    { attrId = aid, attrValue = avalue }

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
                    { attrId = attrId, attrValue = attrValue }
            in
            li_
                [ div [ class "dropdown-item" ]
                    [ div [ class "form-check" ]
                        [ input
                            [ class "form-check-input"
                            , type_ "checkbox"
                            , for (String.fromInt attrId)
                            , onInput (SetFilterExperimentTypeAttributo selectedAttributoValue)
                            , checked <| not <| checkAttributeFilterIsPresent selectedAttributoValue model.hiddenExperimentTypeAttributiFilters
                            ]
                            []
                        , label [ class "form-check-label" ] [ text <| attributoValueSelector attrId attrValue ]
                        ]
                    ]
                ]

        dataSetMatchesAttributoFilter : JsonDataSet -> ExperimentTypeAttributoFilter -> Bool
        dataSetMatchesAttributoFilter ds { attrId, attrValue } =
            case List.Extra.find (\{ attributoId } -> attributoId == attrId) ds.attributi of
                -- can't find the attributo in the data set
                Nothing ->
                    False

                Just attributoInDs ->
                    attrValue == convertAttributoValueFromApi attributoInDs

        dataSetMatchesAttributiFilters : JsonDataSetWithIndexingResults -> Bool
        dataSetMatchesAttributiFilters { dataSet } =
            -- A little subltety here: we define the _hidden_
            -- attributi, so we want to match all data sets where
            -- _none_ of the filters match for it to be _not_ hidden.
            none (dataSetMatchesAttributoFilter dataSet) model.hiddenExperimentTypeAttributiFilters

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
        [ div
            [ class "pb-3"
            , style "border-bottom" "1pt solid lightgray"
            ]
          <|
            label [ style "width" "10rem" ] [ icon { name = "card-list" }, text " Attributi Filter " ]
                :: attributiFilters
        , table [ class "table amarcord-table-fix-head table-borderless" ]
            [ thead_
                [ tr_
                    [ th [ onClick SortDataSetsAscending ]
                        [ text "ID"
                        , icon
                            { name =
                                if model.dataSetsSortingAscending then
                                    "caret-down-fill"

                                else
                                    "caret-up-fill"
                            }
                        ]
                    , th_ [ text "Attributi" ]
                    , th_ [ text "Runs" ]
                    ]
                ]
            , let
                filteredDataSets : List JsonDataSetWithIndexingResults
                filteredDataSets =
                    List.filter dataSetMatchesAttributiFilters dataSets

                sortedDatasets =
                    List.sortWith (\a b -> compare a.dataSet.id b.dataSet.id) filteredDataSets

                requestedSortedDatasets : List JsonDataSetWithIndexingResults
                requestedSortedDatasets =
                    if model.dataSetsSortingAscending then
                        List.reverse <| sortedDatasets

                    else
                        sortedDatasets
              in
              tbody_ <|
                List.concatMap
                    (viewDataSet
                        model
                        attributi
                        chemicalIdsToName
                    )
                    requestedSortedDatasets
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


modalBodyShells : JsonMergeResultFom -> List JsonMergeResultShell -> List JsonRefinementResult -> Html Msg
modalBodyShells fom shells refinementResults =
    let
        singleShellRow : JsonMergeResultShell -> Html Msg
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
                , div [ class "hstack gap-3 mt-2" ]
                    [ span_ [ text "Refinement files:" ]
                    , span_ [ icon { name = "file-binary" }, a [ href (makeFilesLink pdbFileId) ] [ text "PDB" ] ]
                    , div [ class "vr" ] []
                    , span_ [ icon { name = "file-binary" }, a [ href (makeFilesLink mtzFileId) ] [ text "MTZ" ] ]
                    ]
                , p [ class "text-muted" ] [ text "Note: this MTZ file is different from the one in the overview. It was created during refinement, not by CrystFEL." ]
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


viewExperimentTypeFilter : Maybe Int -> JsonReadExperimentTypes -> List (Html Msg)
viewExperimentTypeFilter selectedExperimentType r =
    let
        makeRadio : JsonExperimentType -> Html Msg
        makeRadio et =
            div_
                [ input_
                    [ type_ "radio"
                    , class "btn-check"
                    , id ("et" ++ String.fromInt et.id)
                    , checked (selectedExperimentType == Just et.id)
                    , onClick (SetFilterExperimentType et.id)
                    ]
                , label
                    [ for ("et" ++ String.fromInt et.id)
                    , class "btn btn-outline-primary"
                    , style "border-color" "black"
                    ]
                    [ text et.name ]
                ]
    in
    [ h5_
        [ icon { name = "clipboard-check" }
        , text " Experiment Type"
        ]
    , div [ class "btn-group" ] (List.map makeRadio r.experimentTypes)
    , hr_
    ]


view : Model -> Html Msg
view model =
    div [ class "container" ] <|
        case model.experimentTypesRequest of
            NotAsked ->
                List.singleton <| text ""

            Loading ->
                List.singleton <| loadingBar "Loading experiment types..."

            Failure e ->
                List.singleton <|
                    viewAlert [ AlertDanger ] <|
                        [ h4 [ class "alert-heading" ]
                            [ text "Failed to retrieve experiment types. Try reloading and if that doesn't work, contact the admins." ]
                        , showError e
                        ]

            Success experimentTypeResult ->
                viewExperimentTypeFilter model.selectedExperimentTypeId experimentTypeResult
                    ++ (case model.analysisRequest of
                            NotAsked ->
                                List.singleton <| text ""

                            Loading ->
                                List.singleton <| loadingBar "Loading analysis results..."

                            Failure e ->
                                List.singleton <|
                                    viewAlert [ AlertDanger ] <|
                                        [ h4 [ class "alert-heading" ]
                                            [ text "Failed to retrieve analysis results. Try reloading and if that doesn't work, contact the admins"
                                            ]
                                        , showError e
                                        ]

                            Success r ->
                                [ modalMergeResultDetail model
                                , viewFilters model.selectMergeStatus
                                , viewInner model r
                                ]
                       )


possiblyRefresh : Model -> Cmd Msg
possiblyRefresh model =
    case model.selectedExperimentTypeId of
        Nothing ->
            Cmd.none

        Just etId ->
            send AnalysisResultsReceived
                (readAnalysisResultsApiAnalysisAnalysisResultsBeamtimeIdExperimentTypeIdGet
                    model.beamtimeId
                    etId
                )


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        ToggleIndexingParameterExpansion ipId ->
            let
                newParameterIds =
                    if Set.member ipId model.expandedIndexingParameterIds then
                        Set.remove ipId model.expandedIndexingParameterIds

                    else
                        Set.insert ipId model.expandedIndexingParameterIds
            in
            ( { model | expandedIndexingParameterIds = newParameterIds }, Cmd.none )

        ScrollDone ->
            ( model, Cmd.none )

        OpenDataSet dataSetId ->
            ( { model | selectedDataSetId = Just dataSetId }, Cmd.none )

        CloseDataSet ->
            ( { model | selectedDataSetId = Nothing }, Cmd.none )

        IndexingParametersMsg paramsMsg ->
            case model.currentProcessingParameters of
                Nothing ->
                    ( model, Cmd.none )

                Just processingParameters ->
                    let
                        ( updatedIndexingParams, cmd ) =
                            IndexingParameters.update paramsMsg processingParameters.indexingParamFormModel

                        updatedProcessingParams =
                            { processingParameters | indexingParamFormModel = updatedIndexingParams }
                    in
                    ( { model | currentProcessingParameters = Just updatedProcessingParams }, Cmd.map IndexingParametersMsg cmd )

        OpenProcessingFormWithExisting parameters dataSetId ->
            ( { model | processingParametersRequest = Loading }
            , send
                (ProcessingParametersWithExistingReceived
                    parameters
                    dataSetId
                )
                (readIndexingParametersApiIndexingParametersDataSetIdGet dataSetId)
            )

        ProcessingParametersWithExistingReceived parameters dataSetId response ->
            case response of
                Err e ->
                    ( { model | processingParametersRequest = Failure e }, Cmd.none )

                Ok { sources } ->
                    case
                        IndexingParameters.convertCommandLineToModel
                            (IndexingParameters.init sources (Maybe.withDefault "" parameters.cellDescription) parameters.geometryFile True)
                            parameters.commandLine
                    of
                        Err e ->
                            ( { model | processingParametersRequest = Failure (BadJson e) }, Cmd.none )

                        Ok ipModel ->
                            ( { model
                                | currentProcessingParameters =
                                    Just
                                        { dataSetId = dataSetId
                                        , indexingParamFormModel = ipModel
                                        }
                              }
                            , Task.attempt (always ScrollDone) <| scrollY "processing-form" 0 0
                            )

        OpenProcessingForm dataSetId ->
            ( { model
                | processingParametersRequest = Loading
              }
            , send
                ProcessingParametersReceived
                (readIndexingParametersApiIndexingParametersDataSetIdGet dataSetId)
            )

        ProcessingParametersReceived processingParameters ->
            case processingParameters of
                Err e ->
                    ( { model | processingParametersRequest = Failure e }, Cmd.none )

                Ok v ->
                    ( { model
                        | processingParametersRequest = Success v
                        , currentProcessingParameters =
                            Just
                                { dataSetId = v.dataSetId
                                , indexingParamFormModel = IndexingParameters.init v.sources v.cellDescription "" True
                                }
                      }
                    , Cmd.none
                    )

        CancelProcessing ->
            ( { model
                | currentProcessingParameters = Nothing
                , submitProcessingRequest = NotAsked
                , processingParametersRequest = NotAsked
              }
            , Cmd.none
            )

        ProcessingSubmitted result ->
            case result of
                Err e ->
                    ( { model | submitProcessingRequest = Failure e }, Cmd.none )

                Ok v ->
                    ( { model
                        | submitProcessingRequest = Success v
                        , currentProcessingParameters = Nothing
                        , processingParametersRequest = NotAsked
                        , expandedIndexingParameterIds =
                            Set.insert v.indexingParametersId model.expandedIndexingParameterIds
                      }
                    , Cmd.batch
                        [ possiblyRefresh model
                        , Task.attempt (always ScrollDone) <| scrollY ("indexing-params" ++ String.fromInt v.indexingParametersId) 0 0
                        ]
                    )

        SubmitProcessing ->
            case model.currentProcessingParameters of
                Nothing ->
                    ( model, Cmd.none )

                Just { dataSetId, indexingParamFormModel } ->
                    case IndexingParameters.toCommandLine indexingParamFormModel of
                        Err _ ->
                            ( model, Cmd.none )

                        Ok commandLine ->
                            ( { model | submitProcessingRequest = Loading }
                            , send
                                ProcessingSubmitted
                                (indexingJobQueueForDataSetApiIndexingPost
                                    { dataSetId = dataSetId
                                    , isOnline = False
                                    , cellDescription = indexingParamFormModel.cellDescription
                                    , geometryFile = indexingParamFormModel.geometryFile
                                    , commandLine = coparseCommandLine commandLine
                                    , source = indexingParamFormModel.source
                                    }
                                )
                            )

        ExperimentTypesReceived experimentTypes ->
            ( { model | experimentTypesRequest = fromResult experimentTypes }
            , possiblyRefresh model
            )

        AnalysisResultsReceived analysisResults ->
            ( { model | analysisRequest = fromResult analysisResults }, Cmd.none )

        OpenMergeForm dataSetId indexingParametersId ->
            ( { model
                | activatedMergeForm =
                    Just
                        { mergeParameters = CrystFELMerge.init dataSetId indexingParametersId
                        }
              }
            , Cmd.none
            )

        SubmitQuickMerge dataSetId indexingParametersId ->
            ( { model
                | activatedMergeForm = Nothing
                , mergeRequest =
                    Just
                        { dataSetId = dataSetId
                        , indexingParametersId = indexingParametersId
                        , request = Loading
                        }
              }
            , send MergeFinished (queueMergeJobApiMergingPost (CrystFELMerge.quickMergeParameters dataSetId indexingParametersId))
            )

        CloseMergeForm ->
            ( { model | activatedMergeForm = Nothing }, Cmd.none )

        SubmitMerge dataSetId mergeParameters ->
            case model.activatedMergeForm of
                Nothing ->
                    ( model, Cmd.none )

                Just _ ->
                    ( { model | mergeRequest = Just { dataSetId = dataSetId, indexingParametersId = mergeParameters.indexingParametersId, request = Loading }, activatedMergeForm = Nothing }
                    , send MergeFinished (queueMergeJobApiMergingPost (modelToMergeParameters mergeParameters))
                    )

        MergeFinished result ->
            case model.mergeRequest of
                Nothing ->
                    ( model, Cmd.none )

                Just { dataSetId, indexingParametersId } ->
                    ( { model
                        | mergeRequest =
                            Just
                                { dataSetId = dataSetId
                                , request = RemoteData.fromResult result
                                , indexingParametersId = indexingParametersId
                                }
                      }
                    , possiblyRefresh model
                    )

        Refresh posix ->
            let
                newHereAndNow =
                    { now = posix, zone = model.hereAndNow.zone }
            in
            ( { model | hereAndNow = newHereAndNow }
            , possiblyRefresh model
            )

        OpenMergeResultDetail mr ->
            ( { model | selectedMergeResult = MergeResultSelected mr }, Cmd.none )

        CloseMergeResultDetail ->
            ( { model | selectedMergeResult = NoMergeResultSelected }, Cmd.none )

        CrystFELMergeMessage cfMsg ->
            case model.activatedMergeForm of
                Nothing ->
                    ( model, Cmd.none )

                Just { mergeParameters } ->
                    let
                        ( cfModel, cfCmd ) =
                            CrystFELMerge.update cfMsg mergeParameters
                    in
                    ( { model
                        | activatedMergeForm =
                            Just
                                { mergeParameters = cfModel
                                }
                      }
                    , Cmd.map CrystFELMergeMessage cfCmd
                    )

        ToggleAccordionShowModelParameters mrid ->
            if Set.member mrid model.expandedMergeResultIds then
                ( { model | expandedMergeResultIds = Set.remove mrid model.expandedMergeResultIds }, Cmd.none )

            else
                ( { model | expandedMergeResultIds = Set.insert mrid model.expandedMergeResultIds }, Cmd.none )

        SortDataSetsAscending ->
            let
                dss =
                    model.dataSetsSortingAscending
            in
            ( { model | dataSetsSortingAscending = not dss }, Cmd.none )

        SetFilterExperimentType experimentTypeId ->
            let
                newModel =
                    { model | selectedExperimentTypeId = Just experimentTypeId }
            in
            ( newModel
            , Cmd.batch
                [ possiblyRefresh newModel
                , Nav.pushUrl
                    model.navKey
                    (Route.makeLink (Route.Analysis (Just experimentTypeId) model.beamtimeId))
                ]
            )

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
            ( { model | hiddenExperimentTypeAttributiFilters = List.Extra.unique <| newHlfs }
            , Cmd.none
            )
