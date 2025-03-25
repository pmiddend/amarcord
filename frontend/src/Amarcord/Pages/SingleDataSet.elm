module Amarcord.Pages.SingleDataSet exposing (Model, Msg(..), init, pageTitle, subscriptions, update, view)

import Amarcord.API.DataSet exposing (DataSetId)
import Amarcord.API.Requests exposing (BeamtimeId, ExperimentTypeId)
import Amarcord.Attributo exposing (Attributo, AttributoType, ChemicalNameDict, convertAttributoFromApi, convertAttributoMapFromApi)
import Amarcord.AttributoHtml exposing (formatFloatHumanFriendly, formatIntHumanFriendly)
import Amarcord.Bootstrap exposing (AlertProperty(..), copyToClipboardButton, icon, loadingBar, viewAlert, viewHelpButton)
import Amarcord.CellDescriptionEdit as CellDescriptionEdit
import Amarcord.CellDescriptionViewer as CellDescriptionViewer
import Amarcord.CommandLineParser exposing (CommandLineOption(..), coparseCommandLine, parseCommandLine)
import Amarcord.CrystFELMerge as CrystFELMerge exposing (mergeModelToString, modelToMergeParameters)
import Amarcord.Crystallography exposing (cellDescriptionToString, cellDescriptionsAlmostEqualStrings)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Html exposing (br_, code_, div_, em_, h5_, p_, small_, span_, strongText, tbody_, td_, th_, thead_, tr_)
import Amarcord.HttpError exposing (HttpError(..), send, showError)
import Amarcord.IndexingParameters as IndexingParameters
import Amarcord.Route exposing (MergeFilter(..), Route(..), RunRange, makeFilesLink, makeIndexingIdErrorLogLink, makeIndexingIdLogLink, makeLink, makeMergeIdLogLink)
import Amarcord.Util exposing (HereAndNow, posixDiffHumanFriendly, posixDiffMinutes, withLeftNeighbor)
import Api.Data exposing (DBJobStatus(..), JsonCreateIndexingForDataSetOutput, JsonDataSet, JsonDataSetWithIndexingResults, JsonExperimentType, JsonIndexingParameters, JsonIndexingParametersWithResults, JsonIndexingResult, JsonMergeParameters, JsonMergeResult, JsonMergeResultStateDone, JsonMergeResultStateError, JsonMergeResultStateQueued, JsonMergeResultStateRunning, JsonPolarisation, JsonQueueMergeJobOutput, JsonReadIndexingParametersOutput, JsonReadSingleDataSetResults, ScaleIntensities(..))
import Api.Request.Analysis exposing (readSingleDataSetResultsApiAnalysisSingleDataSetBeamtimeIdDataSetIdGet)
import Api.Request.Merging exposing (queueMergeJobApiMergingPost)
import Api.Request.Processing exposing (indexingJobQueueForDataSetApiIndexingPost, readIndexingParametersApiIndexingParametersDataSetIdGet)
import Basics.Extra exposing (safeDivide)
import Browser.Navigation as Nav
import Dict exposing (Dict)
import Html exposing (Html, a, button, dd, div, dl, dt, em, figcaption, figure, form, h4, img, li, nav, ol, small, span, sup, table, td, text, th, tr)
import Html.Attributes exposing (class, colspan, disabled, href, id, src, style, type_)
import Html.Events exposing (onClick)
import Html.Extra exposing (nothing, viewIf, viewIfLazy, viewMaybe)
import Maybe
import Maybe.Extra exposing (isJust)
import Ports exposing (copyToClipboard)
import RemoteData exposing (RemoteData(..), fromResult, isLoading)
import Result.Extra
import Scroll exposing (scrollY)
import Set exposing (Set)
import String
import Task
import Time exposing (Posix, millisToPosix, posixToMillis)


subscriptions : Model -> List (Sub Msg)
subscriptions _ =
    [ Time.every 10000 Refresh ]


type alias IndexingParametersId =
    Int


type alias ProcessingParametersInput =
    { dataSetId : DataSetId
    , indexingParamFormModel : IndexingParameters.Model
    }


type Msg
    = AnalysisResultsReceived (Result HttpError JsonReadSingleDataSetResults)
    | CopyToClipboard String
    | OpenMergeForm DataSetId IndexingParametersId String String String
    | OpenProcessingFormWithExisting JsonIndexingParameters DataSetId
    | ToggleIndexingParameterExpansion Int
    | IndexingParametersMsg IndexingParameters.Msg
    | ProcessingParametersWithExistingReceived JsonIndexingParameters DataSetId (Result HttpError JsonReadIndexingParametersOutput)
    | ProcessingParametersReceived (Result HttpError JsonReadIndexingParametersOutput)
    | SubmitQuickMerge DataSetId IndexingParametersId
    | ProcessingSubmitted (Result HttpError JsonCreateIndexingForDataSetOutput)
    | CloseMergeForm
    | SubmitMerge DataSetId CrystFELMerge.Model
    | SubmitProcessing
    | MergeFinished (Result HttpError JsonQueueMergeJobOutput)
    | Refresh Posix
    | ScrollDone
    | CrystFELMergeMessage CrystFELMerge.Msg
    | ToggleAccordionShowModelParameters MergeResultId
    | OpenProcessingForm Int
    | CancelProcessing


type SelectedMergeResult
    = NoMergeResultSelected


type alias MergeResultId =
    Int


type alias MergeResultWrapper =
    { mergeResult : JsonMergeResult, showResults : Bool }


type alias ActivatedMergeForm =
    { mergeParameters : CrystFELMerge.Model
    }


type alias MergeRequest =
    { dataSetId : Int
    , indexingParametersId : Int
    , request : RemoteData HttpError JsonQueueMergeJobOutput
    }


type alias Model =
    { hereAndNow : HereAndNow
    , navKey : Nav.Key
    , processingParametersRequest : RemoteData HttpError JsonReadIndexingParametersOutput
    , submitProcessingRequest : RemoteData HttpError JsonCreateIndexingForDataSetOutput
    , analysisRequest : RemoteData HttpError JsonReadSingleDataSetResults
    , activatedMergeForm : Maybe ActivatedMergeForm
    , mergeRequest : Maybe MergeRequest
    , selectedMergeResult : SelectedMergeResult
    , expandedMergeResultIds : Set MergeResultId
    , beamtimeId : BeamtimeId
    , dataSetId : DataSetId
    , currentProcessingParameters : Maybe ProcessingParametersInput
    , expandedIndexingParameterIds : Set Int
    }


pageTitle : Model -> String
pageTitle { dataSetId, analysisRequest } =
    case analysisRequest of
        Success { experimentType } ->
            "Data Set ID " ++ String.fromInt dataSetId ++ " | " ++ experimentType.name

        _ ->
            "Data Set ID " ++ String.fromInt dataSetId


init : Nav.Key -> HereAndNow -> BeamtimeId -> DataSetId -> ( Model, Cmd Msg )
init navKey hereAndNow beamtimeId dataSetId =
    ( { hereAndNow = hereAndNow
      , navKey = navKey
      , analysisRequest = Loading
      , activatedMergeForm = Nothing
      , processingParametersRequest = NotAsked
      , mergeRequest = Nothing
      , submitProcessingRequest = NotAsked
      , selectedMergeResult = NoMergeResultSelected
      , dataSetId = dataSetId
      , expandedMergeResultIds = Set.empty
      , beamtimeId = beamtimeId
      , currentProcessingParameters = Nothing
      , expandedIndexingParameterIds = Set.empty
      }
    , send AnalysisResultsReceived (readSingleDataSetResultsApiAnalysisSingleDataSetBeamtimeIdDataSetIdGet beamtimeId dataSetId)
    )


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


viewMergeParameters : String -> JsonMergeParameters -> Html msg
viewMergeParameters bgClass { mergeModel, scaleIntensities, postRefinement, iterations, polarisation, startAfter, stopAfter, relB, noPr, noDeltaCcHalf, maxAdu, minMeasurements, logs, minRes, pushRes, w, pointGroup, spaceGroup, cellDescription, ambigatorCommandLine } =
    let
        boolDtDl header b =
            [ tr_
                [ td [ class "text-end p-1" ] [ text header ]
                , td [ class "p-1" ]
                    [ if b then
                        text "✔️"

                      else
                        text "✖️"
                    ]
                ]
            ]

        dtDl dtContent dlContent =
            [ tr_
                [ td [ class "text-end p-1" ] [ text dtContent ]
                , td [ class (bgClass ++ " p-1") ] [ dlContent ]
                ]
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
    table [ class "table" ] <|
        dtDl "Cell Description" (CellDescriptionViewer.view CellDescriptionViewer.MultiLine CellDescriptionViewer.WithErrors cellDescription)
            ++ dtDl "Point Group" (text pointGroup)
            ++ maybeDtDl "Space Group" (Maybe.map text spaceGroup)
            ++ (if ambigatorCommandLine == "" then
                    []

                else
                    dtDl "Ambigator parameters" (text ambigatorCommandLine)
               )
            ++ dtDl "Model" (text <| mergeModelToString mergeModel)
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


viewMergeResultRow : List (Html msg) -> HereAndNow -> BeamtimeId -> ExperimentTypeId -> DataSetId -> MergeResultWrapper -> List (Html Msg)
viewMergeResultRow mergeRowHeaders hereAndNow beamtimeId experimentTypeId dataSetId mrw =
    let
        remainingHeaders =
            List.length mergeRowHeaders - 1

        id =
            mrw.mergeResult.id

        runs =
            mrw.mergeResult.runs

        isShowingMergeParams =
            mrw.showResults

        mergeResultUnion =
            createMergeResultUnion mrw.mergeResult

        rowClass =
            case mergeResultUnion of
                Just (MergeResultStateError _) ->
                    "table-secondary"

                _ ->
                    ""

        firstRow =
            tr [ class rowClass ] <|
                [ td_ [ text (String.fromInt id) ]
                , td_
                    [ button
                        [ class "btn btn-link p-0 text-nowrap"
                        , type_ "button"
                        , onClick (ToggleAccordionShowModelParameters id)
                        ]
                        [ small_
                            [ icon
                                { name =
                                    if isShowingMergeParams then
                                        "arrows-angle-contract"

                                    else
                                        "arrows-angle-expand"
                                }
                            , span_ [ text " Params" ]
                            ]
                        ]
                    ]
                , td [ class "text-nowrap" ] (List.intersperse br_ <| List.map text runs)
                ]
                    ++ (case mergeResultUnion of
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
                                [ td [ colspan remainingHeaders ] [ p_ [ text <| "Error: " ++ error ], a [ href (makeMergeIdLogLink id) ] [ icon { name = "link-45deg" }, text " Job log" ] ] ]

                            Just (MergeResultStateDone { started, stopped, result }) ->
                                let
                                    floatWithShell overall outer =
                                        td_ [ text <| formatFloatHumanFriendly overall ++ " (" ++ formatFloatHumanFriendly outer ++ ")" ]

                                    fom =
                                        result.fom

                                    mtzFileId =
                                        result.mtzFileId
                                in
                                [ td_ [ text <| String.fromInt (posixDiffMinutes (millisToPosix stopped) (millisToPosix started)), span [ class "form-text" ] [ text "min" ] ]
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
                                , td_ [ text <| formatFloatHumanFriendly fom.completeness ++ "% (" ++ formatFloatHumanFriendly fom.outerShell.completeness ++ "%)" ]
                                , floatWithShell fom.redundancy fom.outerShell.redundancy
                                , floatWithShell fom.cc fom.outerShell.cc
                                , floatWithShell fom.ccstar fom.outerShell.ccstar
                                , td_ [ text <| Maybe.withDefault "" <| Maybe.map (\wilsonReal -> formatFloatHumanFriendly wilsonReal ++ " Å²") fom.wilson ]
                                , td_
                                    [ icon { name = "file-binary" }
                                    , a [ href (makeFilesLink mtzFileId (Just ("merge-result-" ++ String.fromInt id ++ ".mtz"))) ] [ text "MTZ" ]
                                    ]
                                , td_
                                    [ icon { name = "card-list" }
                                    , a [ href (makeLink (MergeResult beamtimeId experimentTypeId dataSetId mrw.mergeResult.id)) ] [ text "Details" ]
                                    ]
                                ]

                            _ ->
                                [ td
                                    [ colspan remainingHeaders ]
                                    [ div [ class "spinner-border spinner-border-sm text-secondary" ] [], span_ [ text " In queue..." ] ]
                                ]
                       )
    in
    if isShowingMergeParams then
        let
            parameters =
                mrw.mergeResult.parameters

            secondRow =
                tr [ class rowClass ]
                    [ -- keep space for the ID column
                      td_ []
                    , td [ colspan 11 ] [ viewMergeParameters rowClass parameters ]
                    ]
        in
        [ firstRow, secondRow ]

    else
        [ firstRow ]


viewJobStatus : Bool -> DBJobStatus -> Html msg
viewJobStatus hasError x =
    case x of
        DBJobStatusQueued ->
            span [ class "badge text-bg-secondary d-inline-flex align-items-center gap-2" ] [ span [ class "spinner-border spinner-border-sm" ] [], text " queued" ]

        DBJobStatusRunning ->
            span [ class "badge text-bg-primary d-inline-flex align-items-center gap-2" ] [ span [ class "spinner-border spinner-border-sm" ] [], text " running" ]

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
            [ "IID", "Run", "Status", "Frames", "Hits", "Ixed" ]

        viewHistogram fileId =
            div [ class "col" ]
                [ a
                    [ href (makeFilesLink fileId Nothing)
                    ]
                    [ img [ src (makeFilesLink fileId Nothing), class "img-fluid" ] [] ]
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

        viewIndexingResultRow : JsonIndexingResult -> List (Html Msg)
        viewIndexingResultRow { id, runExternalId, hasError, status, started, stopped, streamFile, programVersion, frames, hits, indexedFrames, detectorShiftXMm, detectorShiftYMm, unitCellHistogramsFileId, generatedGeometryFile } =
            [ tr
                [ class
                    (if hasError then
                        "table-secondary"

                     else
                        ""
                    )
                ]
                [ td_ [ text (String.fromInt id) ]
                , td_ [ text (String.fromInt runExternalId) ]
                , td [ class "text-nowrap" ]
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
                        , viewRate indexedFrames frames
                        ]
                    )
                ]
            , tr
                [ class
                    (if hasError then
                        "table-secondary"

                     else
                        ""
                    )
                ]
                [ td_ []
                , td [ colspan 5 ]
                    [ div_
                        [ span [ class "hstack gap-1" ]
                            [ a [ href (makeIndexingIdLogLink id) ] [ icon { name = "link-45deg" }, text " Job log" ]
                            , if hasError then
                                a [ href (makeIndexingIdErrorLogLink id) ] [ icon { name = "link-45deg" }, text "Error log" ]

                              else
                                text ""
                            ]
                        ]
                    , Maybe.withDefault (text "") <|
                        Maybe.map2
                            (\x y ->
                                div_
                                    [ strongText "Detector shift: "
                                    , text (formatFloatHumanFriendly x ++ "mm, " ++ formatFloatHumanFriendly y ++ "mm")
                                    ]
                            )
                            detectorShiftXMm
                            detectorShiftYMm
                    , if String.isEmpty programVersion then
                        text ""

                      else
                        div_ [ strongText "CrystFEL version: ", text programVersion ]
                    , div_
                        [ strongText "Stream file: "
                        , br_
                        , span [ class "text-break" ] [ text streamFile ]
                        , copyToClipboardButton (CopyToClipboard streamFile)
                        ]
                    , if String.isEmpty generatedGeometryFile then
                        text ""

                      else
                        div_
                            [ strongText "Geometry file: "
                            , br_
                            , span [ class "text-break" ] [ text generatedGeometryFile ]
                            , copyToClipboardButton (CopyToClipboard generatedGeometryFile)
                            ]
                    , case unitCellHistogramsFileId of
                        Nothing ->
                            text ""

                        Just ucFileId ->
                            div_
                                [ strongText "Unit cell histograms"
                                , viewHistogram ucFileId
                                ]
                    ]
                ]
            ]
    in
    table [ class "table table-sm  table-borderless" ]
        [ thead_ [ tr_ (List.map (th_ << List.singleton << text) tableHeaders) ]
        , tbody_ (List.concatMap viewIndexingResultRow results)
        ]


mergeActions : Bool -> JsonDataSet -> JsonIndexingParameters -> Maybe MergeRequest -> String -> String -> String -> IndexingParametersId -> Html Msg
mergeActions isActive dataSet indexingParameters mergeRequest cellDescriptionForDs pointGroupForDs spaceGroupForDs indexingParametersId =
    let
        mergeRequestIsLoading : Maybe MergeRequest -> Bool
        mergeRequestIsLoading x =
            case x of
                Nothing ->
                    False

                Just { request } ->
                    isLoading request

        indexingParametersCellDescription =
            Maybe.withDefault "" indexingParameters.cellDescription

        noCellDescription =
            indexingParametersCellDescription == "" && cellDescriptionForDs == ""
    in
    div_
        [ div [ class "input-group input-group-sm" ]
            [ button
                [ type_ "button"
                , class "btn btn-sm btn-outline-primary"
                , onClick (SubmitQuickMerge dataSet.id indexingParametersId)
                , disabled (mergeRequestIsLoading mergeRequest || isActive || noCellDescription)
                ]
                [ icon { name = "send-exclamation" }, text <| " Quick Merge" ]
            , button
                [ type_ "button"
                , class "btn btn-sm btn-outline-secondary"
                , onClick (OpenMergeForm dataSet.id indexingParametersId cellDescriptionForDs pointGroupForDs spaceGroupForDs)
                , disabled (mergeRequestIsLoading mergeRequest || isActive)
                ]
                [ icon { name = "send" }, text <| " Merge" ]
            , viewIf isActive
                (button [ type_ "button", class "btn btn-sm btn-secondary", onClick CloseMergeForm ]
                    [ icon { name = "x-lg" }, text " Cancel" ]
                )
            ]
        , if indexingParametersCellDescription == "" then
            nothing

          else
            viewIfLazy (not (cellDescriptionsAlmostEqualStrings cellDescriptionForDs indexingParametersCellDescription))
                (\_ ->
                    div_
                        [ em_
                            [ small_
                                [ text "Warning: The indexing job uses cell "
                                , CellDescriptionViewer.viewColor "light" CellDescriptionViewer.SingleLine CellDescriptionViewer.NoErrors indexingParametersCellDescription
                                , text ", the chemicals in the data set have cell description "
                                , CellDescriptionViewer.viewColor "light" CellDescriptionViewer.SingleLine CellDescriptionViewer.NoErrors cellDescriptionForDs
                                , text ", merging will default to the first one."
                                ]
                            ]
                        ]
                )
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


makeRangesLink : BeamtimeId -> List RunRange -> Html msg
makeRangesLink beamtimeId ranges =
    a [ href (makeLink (Runs beamtimeId ranges)) ]
        [ text
            (String.join ", "
                (List.map
                    (\{ runIdFrom, runIdTo } ->
                        if runIdFrom == runIdTo then
                            String.fromInt runIdFrom

                        else
                            String.fromInt runIdFrom ++ "-" ++ String.fromInt runIdTo
                    )
                    ranges
                )
            )
        ]


createRunRanges : BeamtimeId -> List Int -> Html msg
createRunRanges beamtimeId =
    makeRangesLink beamtimeId
        << List.map
            (\( start, end ) -> { runIdFrom = start, runIdTo = end })
        << foldIntervals
        << List.sort


viewCommandLineDiff : String -> String -> Html msg
viewCommandLineDiff priorCmdLine newCmdLine =
    let
        priorParsed =
            parseCommandLine priorCmdLine

        longFolder new oldDict =
            case new of
                LongOption name value ->
                    Dict.insert name value oldDict

                _ ->
                    oldDict

        longSwitchFolder new oldSet =
            case new of
                LongSwitch name ->
                    Set.insert name oldSet

                _ ->
                    oldSet
    in
    case priorParsed of
        Err _ ->
            text ""

        Ok priorParsedReal ->
            let
                newParsed =
                    parseCommandLine newCmdLine
            in
            case newParsed of
                Err _ ->
                    text ""

                Ok newParsedReal ->
                    let
                        priorLongSwitches : Set String
                        priorLongSwitches =
                            List.foldl longSwitchFolder Set.empty priorParsedReal

                        priorLongOptions : Dict String String
                        priorLongOptions =
                            List.foldl longFolder Dict.empty priorParsedReal

                        newLongSwitches : Set String
                        newLongSwitches =
                            List.foldl longSwitchFolder Set.empty newParsedReal

                        newLongOptions : Dict String String
                        newLongOptions =
                            List.foldl longFolder Dict.empty newParsedReal

                        priorLongOptionNames =
                            Set.fromList (Dict.keys priorLongOptions)

                        newLongOptionNames =
                            Set.fromList (Dict.keys newLongOptions)

                        newOptions =
                            Set.diff newLongOptionNames priorLongOptionNames

                        droppedOptions =
                            Set.diff priorLongOptionNames newLongOptionNames

                        newSwitches =
                            Set.diff newLongSwitches priorLongSwitches

                        droppedSwitches =
                            Set.diff priorLongSwitches newLongSwitches

                        changedOptions =
                            Set.filter
                                (\optionName ->
                                    Dict.get optionName priorLongOptions /= Dict.get optionName newLongOptions
                                )
                                (Set.intersect priorLongOptionNames newLongOptionNames)
                    in
                    div_
                        [ if not (Set.isEmpty newOptions) || not (Set.isEmpty newSwitches) then
                            div_
                                [ text
                                    ("New options: "
                                        ++ String.join ", "
                                            (List.map (\optionName -> optionName ++ "=" ++ Maybe.withDefault "" (Dict.get optionName newLongOptions)) (Set.toList newOptions))
                                        ++ (if Set.isEmpty newSwitches then
                                                ""

                                            else
                                                (if Set.isEmpty newOptions then
                                                    ""

                                                 else
                                                    ", "
                                                )
                                                    ++ String.join ", " (Set.toList newSwitches)
                                           )
                                    )
                                ]

                          else
                            text ""
                        , if not (Set.isEmpty droppedOptions) || not (Set.isEmpty droppedSwitches) then
                            div_
                                [ text
                                    ("Dropped options: "
                                        ++ String.join ", "
                                            (List.map (\optionName -> optionName ++ "=" ++ Maybe.withDefault "" (Dict.get optionName priorLongOptions)) (Set.toList droppedOptions))
                                        ++ (if Set.isEmpty droppedSwitches then
                                                ""

                                            else
                                                (if Set.isEmpty droppedOptions then
                                                    ""

                                                 else
                                                    ", "
                                                )
                                                    ++ String.join ", " (Set.toList droppedSwitches)
                                           )
                                    )
                                ]

                          else
                            text ""
                        , if not (Set.isEmpty changedOptions) then
                            div_
                                [ text
                                    ("Changed options: "
                                        ++ String.join ", "
                                            (List.map (\optionName -> optionName ++ " “" ++ Maybe.withDefault "" (Dict.get optionName priorLongOptions) ++ "” → “" ++ Maybe.withDefault "" (Dict.get optionName newLongOptions) ++ "”") (Set.toList changedOptions))
                                    )
                                ]

                          else
                            text ""
                        ]


viewRowDiff : JsonIndexingParameters -> JsonIndexingParameters -> Html msg
viewRowDiff pparams params =
    let
        viewCellDescription d =
            case d of
                Nothing ->
                    em_ [ text "auto-detect" ]

                Just descriptionReal ->
                    if String.trim descriptionReal == "" then
                        em_ [ text "auto-detect" ]

                    else
                        CellDescriptionViewer.view CellDescriptionViewer.SingleLine CellDescriptionViewer.NoErrors descriptionReal
    in
    tr_
        [ td [ colspan 5 ]
            [ div [ class "alert alert-dark" ]
                [ if pparams.geometryFile /= params.geometryFile then
                    div_ [ em_ [ text "Geometry file changed" ] ]

                  else
                    text ""
                , if pparams.cellDescription /= params.cellDescription then
                    div_
                        [ text "Cell description: "
                        , viewCellDescription pparams.cellDescription
                        , span [ class "ms-1 me-1" ] [ text "→" ]
                        , viewCellDescription params.cellDescription
                        ]

                  else
                    text ""
                , if pparams.isOnline /= params.isOnline then
                    div_ [ text "Online → Offline" ]

                  else
                    text ""
                , if pparams.commandLine /= params.commandLine then
                    viewCommandLineDiff pparams.commandLine params.commandLine

                  else
                    text ""
                ]
            ]
        ]


viewSingleIndexingResultRow :
    Model
    -> JsonExperimentType
    -> JsonDataSet
    -> String
    -> String
    -> String
    -> Maybe JsonIndexingParametersWithResults
    -> JsonIndexingParametersWithResults
    -> List (Html Msg)
viewSingleIndexingResultRow model experimentType dataSet cellDescriptionForDs pointGroupForDs spaceGroupForDs priorParametersAndResults ({ parameters, indexingResults, mergeResults } as p) =
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

                priorRowDiff =
                    case priorParametersAndResults of
                        Nothing ->
                            text ""

                        Just priorParametersAndResultsReal ->
                            viewRowDiff priorParametersAndResultsReal.parameters parameters
            in
            [ priorRowDiff
            , tr_
                [ td_ [ text (String.fromInt parametersId) ]
                , td_ [ createRunRanges model.beamtimeId (List.map .runExternalId successfulResults) ]
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
                [ td [ colspan (List.length indexingAndMergeResultHeaders), class "ps-5" ]
                    [ h5_ [ text "Merge Results" ]
                    , viewMergeResults model
                        experimentType
                        dataSet
                        cellDescriptionForDs
                        pointGroupForDs
                        spaceGroupForDs
                        parameters
                        mergeResults
                    ]
                ]
            ]


indexingAndMergeResultHeaders : List (Html msg)
indexingAndMergeResultHeaders =
    List.map text [ "PID", "Runs", "Frames", "Hits", "Ixed" ]


viewIndexingAndMergeResultsTable : Model -> JsonExperimentType -> JsonDataSet -> List JsonIndexingParametersWithResults -> String -> String -> String -> Html Msg
viewIndexingAndMergeResultsTable model experimentType dataSet indexingParametersAndResults cellDescriptionForDs pointGroupForDs spaceGroupForDs =
    table
        [ class "table table-borderless p-3 amarcord-table-fix-head" ]
        [ thead_ <| [ tr_ (List.map (\header -> th_ [ header ]) indexingAndMergeResultHeaders) ]
        , tbody_ <|
            List.concat <|
                List.reverse <|
                    withLeftNeighbor indexingParametersAndResults (viewSingleIndexingResultRow model experimentType dataSet cellDescriptionForDs pointGroupForDs spaceGroupForDs)
        ]


viewMergeResultsTable : Model -> JsonExperimentType -> List JsonMergeResult -> Html Msg
viewMergeResultsTable model experimentType mergeResults =
    if List.isEmpty mergeResults then
        nothing

    else
        let
            mergeRowHeaders : List (Html msg)
            mergeRowHeaders =
                [ text "MRID"
                , text "Parameters"
                , text "Runs"
                , text "Time"
                , text "Resolution (Å)"
                , text "Completeness"
                , text "Multiplicity"
                , span_ [ text "CC", Html.sub [] [ text "1/2" ] ]
                , span_ [ text "CC", sup [] [ text "*" ] ]
                , text "Wilson B"
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
            [ class "table table-sm table-borderless text-muted mt-3", style "font-size" "0.8rem" ]
            [ thead_ <| [ tr_ (List.map (\header -> th [ class "text-nowrap" ] [ header ]) mergeRowHeaders) ]
            , tbody_ <| List.concatMap (viewMergeResultRow mergeRowHeaders model.hereAndNow model.beamtimeId experimentType.id model.dataSetId) mergeResultWrappers
            ]


viewMergeResults : Model -> JsonExperimentType -> JsonDataSet -> String -> String -> String -> JsonIndexingParameters -> List JsonMergeResult -> Html Msg
viewMergeResults model experimentType dataSet cellDescriptionForDs pointGroupForDs spaceGroupForDs indexingParameters mergeResults =
    div [ style "margin-bottom" "4rem" ] <|
        [ viewMaybe
            (mergeActions
                (isJust model.activatedMergeForm)
                dataSet
                indexingParameters
                model.mergeRequest
                cellDescriptionForDs
                pointGroupForDs
                spaceGroupForDs
            )
            indexingParameters.id
        , viewMaybe
            (\{ mergeParameters } ->
                viewIfLazy (Just mergeParameters.indexingParametersId == indexingParameters.id)
                    (\_ ->
                        div_
                            [ Html.map CrystFELMergeMessage (CrystFELMerge.view mergeParameters)
                            , div [ class "mb-3 hstack gap-3" ]
                                [ button
                                    [ type_ "button"
                                    , class "btn btn-primary"
                                    , onClick (SubmitMerge mergeParameters.dataSetId mergeParameters)
                                    ]
                                    [ icon { name = "send" }, text " Start Merge" ]
                                , button [ type_ "button", class "btn btn-secondary", onClick CloseMergeForm ]
                                    [ icon { name = "x-lg" }, text " Cancel" ]
                                ]
                            ]
                    )
            )
            model.activatedMergeForm
        , viewMaybe
            (\{ request, dataSetId, indexingParametersId } ->
                viewIf (dataSetId == dataSet.id && Just indexingParametersId == indexingParameters.id) <|
                    case request of
                        Failure e ->
                            div_ [ viewAlert [ AlertDanger ] [ showError e ] ]

                        _ ->
                            nothing
            )
            model.mergeRequest
        , viewMergeResultsTable model experimentType mergeResults
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
                        span [ class "d-flex" ]
                            [ div_
                                [ CellDescriptionViewer.view
                                    CellDescriptionViewer.MultiLine
                                    CellDescriptionViewer.WithErrors
                                    cellDescription
                                ]
                            , copyToClipboardButton (CopyToClipboard cellDescription)
                            ]
                ]
            , dt [ class "col-3" ] [ text "Geometry file" ]
            , dd [ class "col-9" ]
                [ if String.isEmpty parameters.geometryFile then
                    text "auto-detect"

                  else
                    span [ class "d-flex text-break" ]
                        [ text parameters.geometryFile
                        , copyToClipboardButton (CopyToClipboard parameters.geometryFile)
                        ]
                ]
            ]
        , p_
            [ strongText "Command line: "
            , br_
            , code_ [ text parameters.commandLine ]
            , copyToClipboardButton (CopyToClipboard parameters.commandLine)
            ]
        , div_
            [ button
                [ class "btn btn-sm btn-dark"
                , type_ "button"
                , onClick
                    (OpenProcessingFormWithExisting parameters dataSet.id)
                ]
                [ icon { name = "back" }, text " Open processing form with these parameters" ]
            , div [ class "form-text mb-3" ] [ small [] [ text "If you want to reprocess with slightly different parameters, instead of starting from scratch, this button is the right one for you." ] ]
            ]
        , h5_ [ text "Indexing Results per Run" ]
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
                    [ class "btn btn-primary btn-sm mb-1"
                    , type_ "button"
                    , onClick (OpenProcessingForm dataSet.id)
                    , disabled (isLoading model.processingParametersRequest)
                    ]
                    [ viewSpinnerOrIcon model.processingParametersRequest
                    , text " Start new processing job"
                    ]
                ]

        Just _ ->
            text ""


viewProcessingResultsForDataSet : Model -> JsonExperimentType -> JsonDataSet -> List JsonIndexingParametersWithResults -> String -> String -> String -> Html Msg
viewProcessingResultsForDataSet model experimentType dataSet indexingResults cellDescriptionForDs pointGroupForDs spaceGroupForDs =
    div_
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
                    div [ class "badge text-bg-success" ] [ text "Job submitted!" ]

                else
                    text ""

            _ ->
                text ""
        , case model.currentProcessingParameters of
            Nothing ->
                text ""

            Just currentProcessingParameters ->
                viewProcessingParameterForm model currentProcessingParameters
        , div_ [ viewIndexingAndMergeResultsTable model experimentType dataSet indexingResults cellDescriptionForDs pointGroupForDs spaceGroupForDs ]
        ]


viewDataSet :
    Model
    -> JsonExperimentType
    -> List (Attributo AttributoType)
    -> ChemicalNameDict
    -> JsonDataSetWithIndexingResults
    -> List (Html Msg)
viewDataSet model experimentType attributi chemicalIdsToName { dataSet, runs, indexingResults, cellDescription, pointGroup, spaceGroup } =
    [ h4 [ class "mt-3" ] [ text "Data Set Metadata" ]
    , div [ class "row" ]
        [ div [ class "col-6" ]
            [ viewDataSetTable
                attributi
                model.hereAndNow.zone
                chemicalIdsToName
                (convertAttributoMapFromApi dataSet.attributi)
                False
                False
                Nothing
            ]
        , div [ class "col-6 text-center" ]
            [ h5_ [ text "Runs" ]
            , p_
                [ a
                    [ href
                        (makeLink
                            (Runs model.beamtimeId
                                (List.map (\{ runFrom, runTo } -> { runIdFrom = runFrom, runIdTo = runTo }) runs)
                            )
                        )
                    ]
                    (List.intersperse br_ <|
                        List.map
                            (\{ runFrom, runTo } ->
                                text <|
                                    if runFrom == runTo then
                                        String.fromInt runFrom

                                    else
                                        String.fromInt runFrom ++ "-" ++ String.fromInt runTo
                            )
                            runs
                    )
                ]
            ]
        ]
    , h4 [ class "mt-3" ] [ text "Processing Results", viewHelpButton "help-processing-results" ]
    , div [ id "help-processing-results", class "collapse text-bg-light p-2" ]
        [ p_
            [ text "Below you will find processing results for different parameter combinations. The parameters have a number ", em_ [ text "PID" ], text " to differentiate them in the database." ]
        , p_
            [ text "Indexing is done on a per-run basis, so for each "
            , em_ [ text "Run ID" ]
            , text " and each indexing parameter "
            , em_ [ text "PID" ]
            , text " you have zero or more indexing result IDs: "
            , em_ [ text "IID." ]
            ]
        , p_ [ text "Merge jobs merge indexing results with the same parameters, and are identified by an ID as well: ", em_ [ text "MRID." ] ]
        , figure [ class "figure" ]
            [ img [ src "help-processing-results.svg", class "img-fluid mb-2 mt-2" ] []
            , figcaption [ class "figure-caption" ] [ text "In this sample scenario, we have two runs, which were processed using a parameter set with PID 1, and the result is indexing reults IID 1 and 2. Those were then merged in two different ways, resulting in MRID 1 and 2. Moreover, we tried to reprocess the runs using different parameters (PID 2 and IID 3), but the results were not convincing." ]
            ]
        ]
    , viewProcessingResultsForDataSet model
        experimentType
        dataSet
        indexingResults
        cellDescription
        pointGroup
        spaceGroup
    ]


view : Model -> Html Msg
view model =
    div [ class "container" ] <|
        case model.analysisRequest of
            NotAsked ->
                List.singleton <| text ""

            Loading ->
                List.singleton <| loadingBar "Loading data set..."

            Failure e ->
                List.singleton <|
                    viewAlert [ AlertDanger ] <|
                        [ h4 [ class "alert-heading" ]
                            [ text "Failed to retrieve data set. Try reloading and if that doesn't work, contact the admins"
                            ]
                        , showError e
                        ]

            Success { attributi, dataSet, chemicalIdToName, experimentType } ->
                [ nav []
                    [ ol [ class "breadcrumb" ]
                        [ li [ class "breadcrumb-item active" ]
                            [ text "/ ", a [ href (makeLink (AnalysisOverview model.beamtimeId [] False Both)) ] [ text "Analysis Overview" ] ]
                        , li [ class "breadcrumb-item active" ]
                            [ text experimentType.name
                            ]
                        , li [ class "breadcrumb-item" ] [ text <| "Data Set ID " ++ String.fromInt model.dataSetId ]
                        ]
                    ]
                , div_
                    (viewDataSet
                        model
                        experimentType
                        (List.map convertAttributoFromApi attributi)
                        (List.foldr (\{ chemicalId, name } -> Dict.insert chemicalId name) Dict.empty chemicalIdToName)
                        dataSet
                    )
                ]


possiblyRefresh : Model -> Cmd Msg
possiblyRefresh model =
    send AnalysisResultsReceived
        (readSingleDataSetResultsApiAnalysisSingleDataSetBeamtimeIdDataSetIdGet
            model.beamtimeId
            model.dataSetId
        )


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        CopyToClipboard s ->
            ( model, copyToClipboard s )

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
                            if String.trim (CellDescriptionEdit.modelAsText indexingParamFormModel.cellDescription) == "" then
                                ( { model | submitProcessingRequest = Loading }
                                , send
                                    ProcessingSubmitted
                                    (indexingJobQueueForDataSetApiIndexingPost
                                        { dataSetId = dataSetId
                                        , isOnline = False
                                        , cellDescription = ""
                                        , geometryFile = indexingParamFormModel.geometryFile
                                        , commandLine = coparseCommandLine commandLine
                                        , source = indexingParamFormModel.source
                                        }
                                    )
                                )

                            else
                                case CellDescriptionEdit.parseModel indexingParamFormModel.cellDescription of
                                    Err _ ->
                                        ( model, Cmd.none )

                                    Ok cellDescription ->
                                        ( { model | submitProcessingRequest = Loading }
                                        , send
                                            ProcessingSubmitted
                                            (indexingJobQueueForDataSetApiIndexingPost
                                                { dataSetId = dataSetId
                                                , isOnline = False
                                                , cellDescription = cellDescriptionToString cellDescription
                                                , geometryFile = indexingParamFormModel.geometryFile
                                                , commandLine = coparseCommandLine commandLine
                                                , source = indexingParamFormModel.source
                                                }
                                            )
                                        )

        AnalysisResultsReceived analysisResults ->
            ( { model | analysisRequest = fromResult analysisResults }, Cmd.none )

        OpenMergeForm dataSetId indexingParametersId cellDescriptionForDs pointGroupForDs spaceGroupForDs ->
            ( { model
                | activatedMergeForm =
                    Just
                        { mergeParameters = CrystFELMerge.init cellDescriptionForDs pointGroupForDs spaceGroupForDs dataSetId indexingParametersId
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
