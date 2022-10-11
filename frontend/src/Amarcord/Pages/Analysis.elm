module Amarcord.Pages.Analysis exposing (Model, Msg(..), init, update, view)

import Amarcord.API.DataSet exposing (DataSetId)
import Amarcord.API.ExperimentType exposing (ExperimentType)
import Amarcord.API.Requests exposing (AnalysisResultsExperimentType, AnalysisResultsRoot, MergeFom, MergeResult, MergeResultState(..), MergeShellFom, RequestError, httpGetAnalysisResults, httpStartMergeJobForDataSet)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Attributo exposing (Attributo, AttributoType)
import Amarcord.AttributoHtml exposing (formatFloatHumanFriendly, formatIntHumanFriendly)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Dialog as Dialog
import Amarcord.Html exposing (br_, div_, form_, h2_, h5_, input_, span_, tbody_, td_, th_, thead_, tr_)
import Amarcord.Route exposing (makeFilesLink)
import Amarcord.Util exposing (HereAndNow, posixDiffHumanFriendly, posixDiffMinutes)
import Dict exposing (Dict)
import Html exposing (Html, a, abbr, button, div, em, h4, span, sup, table, td, text, tr)
import Html.Attributes exposing (class, colspan, disabled, href, placeholder, style, title, type_, value)
import Html.Events exposing (onClick, onInput)
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
    | SubmitMerge DataSetId PartialatorAdditional
    | MergeFinished (Result RequestError ())
    | Refresh Posix
    | MergePartialatorAdditionalChange String
    | OpenMergeResultDetail DetailMerge
    | CloseMergeResultDetail


type SelectedMergeResult
    = NoMergeResultSelected
    | MergeResultSelected DetailMerge


type alias ActivatedMergeForm =
    { dataSetId : Int
    , partialatorAdditional : PartialatorAdditional
    }


type alias PartialatorAdditional =
    String


type alias DetailMerge =
    { id : Int, fom : MergeFom, shell_foms : List MergeShellFom }


type alias Model =
    { hereAndNow : HereAndNow
    , analysisRequest : RemoteData RequestError AnalysisResultsRoot
    , activatedMergeForm : Maybe ActivatedMergeForm
    , mergeRequest : RemoteData RequestError ()
    , selectedMergeResult : SelectedMergeResult
    }


init : HereAndNow -> ( Model, Cmd Msg )
init hereAndNow =
    ( { hereAndNow = hereAndNow
      , analysisRequest = Loading
      , activatedMergeForm = Nothing
      , mergeRequest = NotAsked
      , selectedMergeResult = NoMergeResultSelected
      }
    , httpGetAnalysisResults AnalysisResultsReceived
    )


viewInner : HereAndNow -> RemoteData RequestError () -> Maybe ActivatedMergeForm -> AnalysisResultsRoot -> List (Html Msg)
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
    -> RemoteData RequestError ()
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
                                , disabled (isLoading mergeRequest)
                                ]
                                [ icon { name = "send-exclamation" }, text <| " Quick Merge" ]
                            , button
                                [ type_ "button"
                                , class "btn btn-sm btn-outline-secondary"
                                , onClick (StartMerge dataSet.id)
                                , disabled (isLoading mergeRequest)
                                ]
                                [ icon { name = "send" }, text <| " Merge" ]
                            ]

                actions =
                    case activatedMergeForm of
                        Just { dataSetId, partialatorAdditional } ->
                            if dataSetId == dataSet.id then
                                form_
                                    [ div [ class "input-group" ]
                                        [ input_
                                            [ type_ "text"
                                            , class "form-control"
                                            , placeholder "Parameters"
                                            , value partialatorAdditional
                                            , onInput MergePartialatorAdditionalChange
                                            ]
                                        , button
                                            [ type_ "button"
                                            , class "btn btn-sm btn-success"
                                            , onClick (SubmitMerge dataSet.id partialatorAdditional)
                                            , disabled (isLoading mergeRequest)
                                            ]
                                            [ icon { name = "send" } ]
                                        , button
                                            [ type_ "button"
                                            , class "btn btn-sm btn-warning"
                                            , onClick CancelMerge
                                            , disabled (isLoading mergeRequest)
                                            ]
                                            [ icon { name = "x" } ]
                                        ]
                                    ]

                            else
                                mergeButtons

                        _ ->
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

                viewMergeResultRow : MergeResult -> Html Msg
                viewMergeResultRow { id, runs, partialatorAdditional, state } =
                    let
                        remainingHeaders =
                            List.length mergeRowHeaders - 1
                    in
                    tr_ <|
                        [ td_ [ text (String.fromInt id) ]
                        , td_ [ span [ class "font-monospace text-nowrap" ] [ text partialatorAdditional ] ]
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
                                        [ td [ colspan remainingHeaders ] [ span_ [ text <| "Error: " ++ error ++ ". Latest log: " ] ] ]

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
                                        , td_ [ text <| formatFloatHumanFriendly fom.wilson ]
                                        , td_
                                            [ icon { name = "file-binary" }
                                            , a [ href (makeFilesLink mtzFileId) ] [ text "MTZ" ]
                                            ]
                                        , td_
                                            [ icon { name = "card-list" }
                                            , a
                                                [ class "link-primary"
                                                , onClick (OpenMergeResultDetail { id = id, fom = fom, shell_foms = shells })
                                                ]
                                                [ text "Details" ]
                                            ]
                                        ]
                               )
            in
            [ tr
                (if List.isEmpty mergeResults then
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
            , if List.isEmpty mergeResults then
                text ""

              else
                tr [ style "border-bottom" "1pt solid black" ]
                    [ td [ colspan 7, style "padding-left" "2em", class "text-muted" ]
                        [ h5_ [ icon { name = "diagram-2" }, text "Merge results" ]
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
                    , body = Just <| span_ [ modalBodyShells mr.fom mr.shell_foms ]
                    , closeMessage = Just CloseMergeResultDetail
                    , containerClass = Nothing
                    , modalDialogClass = Just "modal-dialog modal-xl"
                    , footer = Nothing
                    }
                )

        NoMergeResultSelected ->
            text ""


modalBodyShells : MergeFom -> List MergeShellFom -> Html Msg
modalBodyShells fom shells =
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
    in
    div_
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
            ( { model | activatedMergeForm = Just { dataSetId = dataSetId, partialatorAdditional = "" } }, Cmd.none )

        QuickStartMerge dataSetId ->
            ( { model | activatedMergeForm = Nothing, mergeRequest = Loading }, httpStartMergeJobForDataSet MergeFinished dataSetId "" )

        CancelMerge ->
            ( { model | activatedMergeForm = Nothing }, Cmd.none )

        SubmitMerge dataSetId partialatorAdditional ->
            case model.activatedMergeForm of
                Nothing ->
                    ( model, Cmd.none )

                Just _ ->
                    ( { model | mergeRequest = Loading }, httpStartMergeJobForDataSet MergeFinished dataSetId partialatorAdditional )

        MergeFinished _ ->
            ( { model | mergeRequest = Success (), activatedMergeForm = Nothing }, httpGetAnalysisResults AnalysisResultsReceived )

        Refresh posix ->
            let
                newHereAndNow =
                    { now = posix, zone = model.hereAndNow.zone }
            in
            ( { model | hereAndNow = newHereAndNow }, httpGetAnalysisResults AnalysisResultsReceived )

        MergePartialatorAdditionalChange newPartialatorAdditional ->
            ( case model.activatedMergeForm of
                Nothing ->
                    model

                Just { dataSetId } ->
                    { model | activatedMergeForm = Just { dataSetId = dataSetId, partialatorAdditional = newPartialatorAdditional } }
            , Cmd.none
            )

        OpenMergeResultDetail mr ->
            ( { model | selectedMergeResult = MergeResultSelected mr }, Cmd.none )

        CloseMergeResultDetail ->
            ( { model | selectedMergeResult = NoMergeResultSelected }, Cmd.none )
