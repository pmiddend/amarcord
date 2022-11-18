module Amarcord.Pages.Analysis exposing (Model, Msg(..), init, update, view)

import Amarcord.API.DataSet exposing (DataSetId)
import Amarcord.API.ExperimentType exposing (ExperimentType)
import Amarcord.API.Requests exposing (AnalysisResultsExperimentType, AnalysisResultsRoot, MergeFom, MergeResult, MergeResultState(..), MergeShellFom, RefinementResult, RequestError, httpGetAnalysisResults, httpStartMergeJobForDataSet)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Attributo exposing (Attributo, AttributoType)
import Amarcord.AttributoHtml exposing (formatFloatHumanFriendly, formatIntHumanFriendly)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert)
import Amarcord.CrystFELMerge as CrystFELMerge exposing (MergeParametersInput, Polarisation, ScaleIntensities(..), mergeModelToString, modelToMergeParameters)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Dialog as Dialog
import Amarcord.Html exposing (br_, div_, h2_, h5_, span_, tbody_, td_, th_, thead_, tr_)
import Amarcord.PointGroupChooser exposing (pointGroupToString)
import Amarcord.Route exposing (makeFilesLink)
import Amarcord.Util exposing (HereAndNow, posixDiffHumanFriendly, posixDiffMinutes)
import Dict exposing (Dict)
import Html exposing (Html, a, abbr, button, dd, div, dl, dt, em, h4, node, p, span, sup, table, td, text, tr)
import Html.Attributes exposing (attribute, class, colspan, disabled, href, style, title, type_)
import Html.Events exposing (onClick)
import Maybe
import Maybe.Extra as MaybeExtra
import RemoteData exposing (RemoteData(..), fromResult, isLoading)
import String
import Time exposing (Posix)


type Msg
    = AnalysisResultsReceived (Result RequestError AnalysisResultsRoot)
    | StartMerge DataSetId
    | QuickStartMerge DataSetId
    | CancelMerge
    | SubmitMerge DataSetId CrystFELMerge.Model
    | MergeFinished (Result RequestError ())
    | Refresh Posix
    | OpenMergeResultDetail DetailMerge
    | CloseMergeResultDetail
    | CrystFELMergeMessage CrystFELMerge.Msg


type SelectedMergeResult
    = NoMergeResultSelected
    | MergeResultSelected DetailMerge


type alias ActivatedMergeForm =
    { dataSetId : Int
    , mergeParameters : CrystFELMerge.Model
    }


type alias DetailMerge =
    { id : Int, fom : MergeFom, shell_foms : List MergeShellFom, refinementResults : List RefinementResult }


type alias MergeRequest =
    { dataSetId : Int
    , request : RemoteData RequestError ()
    }


mergeRequestIsLoading : Maybe MergeRequest -> Bool
mergeRequestIsLoading x =
    case x of
        Nothing ->
            False

        Just { request } ->
            isLoading request


type alias Model =
    { hereAndNow : HereAndNow
    , analysisRequest : RemoteData RequestError AnalysisResultsRoot
    , activatedMergeForm : Maybe ActivatedMergeForm
    , mergeRequest : Maybe MergeRequest
    , selectedMergeResult : SelectedMergeResult
    }


init : HereAndNow -> ( Model, Cmd Msg )
init hereAndNow =
    ( { hereAndNow = hereAndNow
      , analysisRequest = Loading
      , activatedMergeForm = Nothing
      , mergeRequest = Nothing
      , selectedMergeResult = NoMergeResultSelected
      }
    , httpGetAnalysisResults AnalysisResultsReceived
    )


viewInner : HereAndNow -> Maybe MergeRequest -> Maybe ActivatedMergeForm -> AnalysisResultsRoot -> List (Html Msg)
viewInner hereAndNow mergeRequest activatedMergeForm { experimentTypes, attributi, chemicalIdToName, dataSets } =
    List.map
        (\experimentType ->
            viewResultsTableForSingleExperimentType
                attributi
                hereAndNow
                mergeRequest
                activatedMergeForm
                chemicalIdToName
                experimentType
                (Maybe.withDefault [] <| Dict.get experimentType.id dataSets)
        )
        experimentTypes


viewResultsTableForSingleExperimentType :
    List (Attributo AttributoType)
    -> HereAndNow
    -> Maybe MergeRequest
    -> Maybe ActivatedMergeForm
    -> Dict Int String
    -> ExperimentType
    -> List AnalysisResultsExperimentType
    -> Html Msg
viewResultsTableForSingleExperimentType attributi hereAndNow mergeRequest activatedMergeForm chemicalIds experimentType dataSets =
    let
        viewDataSet : AnalysisResultsExperimentType -> List (Html Msg)
        viewDataSet experimentTypeResults =
            let
                dataSet =
                    experimentTypeResults.dataSet

                dataSetRuns =
                    experimentTypeResults.runs

                mergeResults =
                    experimentTypeResults.mergeResults

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
                        Off ->
                            "off"

                        DebyeWaller ->
                            "Debye-Waller"

                        Normal ->
                            "Scale intensities"

                viewMergeParameters : MergeParametersInput -> Html msg
                viewMergeParameters { model, scaleIntensities, postRefinement, iterations, polarisation, startAfter, stopAfter, relB, noPr, noDeltaCcHalf, maxAdu, minMeasurements, logs, minRes, pushRes, w } =
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

                        polarisationToDescription : Polarisation -> String
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
                            (dtDl "Model" (text <| mergeModelToString model)
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
                                ++ maybeDtDl "Indexing assignment refinement" (Maybe.map (text << pointGroupToString) w)
                                ++ dtDl "Reject crystals with absolute B factors ≥ Å²" (text <| String.fromFloat <| relB)
                                ++ boolDtDl "Disable the orientation/physics model part of the refinement calculation" noPr
                                ++ maybeDtDl "Start after crystals" (Maybe.map (text << String.fromInt) startAfter)
                                ++ maybeDtDl "Stop after crystals" (Maybe.map (text << String.fromInt) stopAfter)
                            )
                        ]

                viewMergeResultRow : MergeResult -> Html Msg
                viewMergeResultRow { id, runs, parameters, state, refinementResults } =
                    let
                        remainingHeaders =
                            List.length mergeRowHeaders - 1
                    in
                    tr_ <|
                        [ td_ [ text (String.fromInt id) ]
                        , td_ [ span [ class "text-nowrap" ] [ viewMergeParameters parameters ] ]
                        , td [ class "text-nowrap" ] (List.intersperse br_ <| List.map text runs)
                        ]
                            ++ (case state of
                                    MergeResultQueued ->
                                        [ td
                                            [ colspan remainingHeaders ]
                                            [ div [ class "spinner-border spinner-border-sm text-secondary" ] [], span_ [ text " In queue..." ] ]
                                        ]

                                    MergeResultRunning started _ _ ->
                                        [ td
                                            [ colspan remainingHeaders ]
                                            [ div [ class "spinner-border spinner-border-sm text-primary" ] []
                                            , em [ class "mb-3" ]
                                                [ text " Running for "
                                                , text <|
                                                    posixDiffHumanFriendly hereAndNow.now started
                                                ]
                                            ]
                                        ]

                                    MergeResultError _ _ error _ ->
                                        [ td [ colspan remainingHeaders ] [ span_ [ text <| "Error: " ++ error ++ "." ] ] ]

                                    MergeResultDone started stopped mtzFileId fom shells ->
                                        let
                                            floatWithShell overall outer =
                                                td_ [ text <| formatFloatHumanFriendly overall ++ " (" ++ formatFloatHumanFriendly outer ++ ")" ]
                                        in
                                        [ td_ [ text <| String.fromInt <| posixDiffMinutes stopped started ]
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
                                        , floatWithShell fom.ccStar fom.outerShell.ccStar
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
                               )
            in
            [ tr
                (if List.isEmpty mergeResults && not currentRowIsMerging then
                    [ style "border-bottom" "1pt solid black" ]

                 else
                    []
                )
                [ td_ [ text (String.fromInt dataSet.id) ]
                , td_ [ viewDataSetTable attributi hereAndNow.zone chemicalIds dataSet False Nothing ]
                , td_ (List.intersperse br_ <| List.map text dataSetRuns)
                , td_
                    [ text <|
                        if List.isEmpty dataSetRuns then
                            ""

                        else
                            MaybeExtra.unwrap "" (\summary -> MaybeExtra.unwrap "" (\hr -> formatFloatHumanFriendly hr ++ "%") summary.hitRate) dataSet.summary
                    ]
                , td_
                    [ text <|
                        if List.isEmpty dataSetRuns then
                            ""

                        else
                            MaybeExtra.unwrap "" (\summary -> MaybeExtra.unwrap "" (\hr -> formatFloatHumanFriendly hr ++ "%") summary.indexingRate) dataSet.summary
                    ]
                , td_
                    [ text <|
                        if List.isEmpty dataSetRuns then
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
                                    [ button [ type_ "button", class "btn btn-primary me-3", onClick (SubmitMerge dataSetId mergeParameters) ] [ icon { name = "send" }, text " Start Merge" ]
                                    , button [ type_ "button", class "btn btn-secondary", onClick CancelMerge ] [ icon { name = "x-lg" }, text " Cancel" ]
                                    ]
                                ]
                            ]

                    else
                        text ""

                _ ->
                    text ""
            , if List.isEmpty mergeResults then
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
                                        div_ [ makeAlert [ AlertDanger ] [ showRequestError e ] ]

                                    _ ->
                                        text ""
                        , table
                            [ class "table table-sm text-muted", style "font-size" "0.8rem", style "margin-bottom" "4rem" ]
                            [ thead_ <| [ tr_ (List.map (\header -> th_ [ header ]) mergeRowHeaders) ]
                            , tbody_ <| List.map viewMergeResultRow mergeResults
                            ]
                        ]
                    ]
            ]
    in
    div_
        [ h2_ [ span_ [ text experimentType.name ], text " data sets" ]
        , table [ class "table amarcord-table-fix-head table-borderless" ]
            [ thead_
                [ tr_
                    [ th_ [ text "ID" ]
                    , th_ [ text "Attributi" ]
                    , th_ [ text "Runs" ]
                    , th_ [ abbr [ title "Hit Rate" ] [ text "HR" ] ]
                    , th_ [ abbr [ title "Indexing Rate" ] [ text "IR" ] ]
                    , th_ [ abbr [ title "Indexed Frames" ] [ text "Ind.F." ] ]
                    , th_ [ text "Actions" ]
                    ]
                ]
            , tbody_ (List.concatMap viewDataSet dataSets)
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


modalBodyShells : MergeFom -> List MergeShellFom -> List RefinementResult -> Html Msg
modalBodyShells fom shells refinementResults =
    let
        singleShellRow : MergeShellFom -> Html Msg
        singleShellRow shellRow =
            tr_
                [ td_ [ text <| formatFloatHumanFriendly shellRow.minRes ++ "–" ++ formatFloatHumanFriendly shellRow.maxRes ]
                , td_ [ text <| formatIntHumanFriendly shellRow.nRef ]
                , td_ [ text <| formatIntHumanFriendly shellRow.reflectionsPossible ]
                , td_ [ text <| formatFloatHumanFriendly shellRow.completeness ]
                , td_ [ text <| formatFloatHumanFriendly shellRow.redundancy ]
                , td_ [ text <| formatFloatHumanFriendly shellRow.snr ]
                , td_ [ text <| formatFloatHumanFriendly shellRow.rSplit ]
                , td_ [ text <| formatFloatHumanFriendly shellRow.cc ]
                , td_ [ text <| formatFloatHumanFriendly shellRow.ccStar ]
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
                , td_ [ text <| formatFloatHumanFriendly fom.ccStar ]
                ]

        uglymol : Int -> Int -> String -> Html msg
        uglymol pdbId mtzId prefix =
            node "uglymol-viewer" [ attribute "pdbid" (String.fromInt pdbId), attribute "mtzid" (String.fromInt mtzId), attribute "idprefix" prefix ] []

        viewRefinementResult : RefinementResult -> Html msg
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
                List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve Attributi" ], showRequestError e ]

            Success r ->
                modalMergeResultDetail model :: viewInner model.hereAndNow model.mergeRequest model.activatedMergeForm r


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        AnalysisResultsReceived analysisResults ->
            ( { model | analysisRequest = fromResult analysisResults }, Cmd.none )

        StartMerge dataSetId ->
            ( { model | activatedMergeForm = Just { dataSetId = dataSetId, mergeParameters = CrystFELMerge.init } }, Cmd.none )

        QuickStartMerge dataSetId ->
            ( { model | activatedMergeForm = Nothing, mergeRequest = Just { dataSetId = dataSetId, request = Loading } }, httpStartMergeJobForDataSet MergeFinished dataSetId CrystFELMerge.quickMergeParameters )

        CancelMerge ->
            ( { model | activatedMergeForm = Nothing }, Cmd.none )

        SubmitMerge dataSetId mergeParameters ->
            case model.activatedMergeForm of
                Nothing ->
                    ( model, Cmd.none )

                Just _ ->
                    ( { model | mergeRequest = Just { dataSetId = dataSetId, request = Loading }, activatedMergeForm = Nothing }
                    , httpStartMergeJobForDataSet MergeFinished dataSetId (modelToMergeParameters mergeParameters)
                    )

        MergeFinished result ->
            case model.mergeRequest of
                Nothing ->
                    ( model, Cmd.none )

                Just { dataSetId } ->
                    ( { model | mergeRequest = Just { dataSetId = dataSetId, request = RemoteData.fromResult result } }, httpGetAnalysisResults AnalysisResultsReceived )

        Refresh posix ->
            let
                newHereAndNow =
                    { now = posix, zone = model.hereAndNow.zone }
            in
            ( { model | hereAndNow = newHereAndNow }, httpGetAnalysisResults AnalysisResultsReceived )

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
