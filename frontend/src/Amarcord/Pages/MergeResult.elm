module Amarcord.Pages.MergeResult exposing (Model, Msg(..), init, pageTitle, update, view)

import Amarcord.API.DataSet exposing (DataSetId)
import Amarcord.API.ExperimentType exposing (ExperimentTypeId)
import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.AttributoHtml exposing (formatFloatHumanFriendly, formatIntHumanFriendly)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, viewAlert)
import Amarcord.Html exposing (div_, h5_, span_, tbody_, td_, th_, thead_, tr_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.Route exposing (MergeFilter(..), Route(..), makeFilesLink, makeLink)
import Amarcord.Util exposing (HereAndNow)
import Api.Data exposing (JsonExperimentType, JsonMergeResultFom, JsonMergeResultShell, JsonReadSingleMergeResult, JsonRefinementResult)
import Api.Request.Analysis exposing (readSingleMergeResultApiAnalysisMergeResultBeamtimeIdExperimentTypeIdMergeResultIdGet)
import Browser.Navigation as Nav
import Html exposing (Html, a, div, h4, li, nav, node, ol, p, sup, table, text, tr)
import Html.Attributes exposing (attribute, class, href)
import RemoteData exposing (RemoteData(..), fromResult)
import String


type Msg
    = AnalysisResultsReceived (Result HttpError JsonReadSingleMergeResult)


type alias MergeResultId =
    Int


type alias Model =
    { hereAndNow : HereAndNow
    , navKey : Nav.Key
    , analysisRequest : RemoteData HttpError JsonReadSingleMergeResult
    , beamtimeId : BeamtimeId
    , experimentTypeId : ExperimentTypeId
    , dataSetId : DataSetId
    , mergeResultId : MergeResultId
    }


pageTitle : Model -> String
pageTitle { mergeResultId, analysisRequest, dataSetId } =
    case analysisRequest of
        Success { experimentType } ->
            "Merge Result ID " ++ String.fromInt mergeResultId ++ " | Data Set ID " ++ String.fromInt dataSetId ++ " | " ++ experimentType.name

        _ ->
            "Merge Result ID " ++ String.fromInt mergeResultId


init : Nav.Key -> HereAndNow -> BeamtimeId -> ExperimentTypeId -> DataSetId -> MergeResultId -> ( Model, Cmd Msg )
init navKey hereAndNow beamtimeId experimentTypeId dataSetId mergeResultId =
    ( { hereAndNow = hereAndNow
      , navKey = navKey
      , analysisRequest = NotAsked
      , beamtimeId = beamtimeId
      , dataSetId = dataSetId
      , experimentTypeId = experimentTypeId
      , mergeResultId = mergeResultId
      }
    , send
        AnalysisResultsReceived
        (readSingleMergeResultApiAnalysisMergeResultBeamtimeIdExperimentTypeIdMergeResultIdGet beamtimeId experimentTypeId mergeResultId)
    )


modalMergeResultDetail : Model -> JsonReadSingleMergeResult -> Html Msg
modalMergeResultDetail m { experimentType, result } =
    case result.stateDone of
        Nothing ->
            text "Waiting for results..."

        Just mr ->
            modalBodyShells
                m.beamtimeId
                experimentType
                m.dataSetId
                m.mergeResultId
                mr.result.fom
                mr.result.detailedFoms
                result.refinementResults


modalBodyShells : BeamtimeId -> JsonExperimentType -> DataSetId -> MergeResultId -> JsonMergeResultFom -> List JsonMergeResultShell -> List JsonRefinementResult -> Html Msg
modalBodyShells beamtimeId experimentType dataSetId mergeResultId fom shells refinementResults =
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
                    , span_ [ icon { name = "file-binary" }, a [ href (makeFilesLink pdbFileId (Just ("merge-result-" ++ String.fromInt id ++ "-refined.pdb"))) ] [ text "PDB" ] ]
                    , div [ class "vr" ] []
                    , span_ [ icon { name = "file-binary" }, a [ href (makeFilesLink mtzFileId (Just ("merge-result-" ++ String.fromInt mergeResultId ++ "-refined.mtz"))) ] [ text "MTZ" ] ]
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
        [ nav []
            [ ol [ class "breadcrumb" ]
                [ li [ class "breadcrumb-item active" ]
                    [ text "/ ", a [ href (makeLink (AnalysisOverview beamtimeId [] False Both)) ] [ text "Analysis Overview" ] ]
                , li [ class "breadcrumb-item active" ]
                    [ text experimentType.name
                    ]
                , li [ class "breadcrumb-item active" ]
                    [ a [ href (makeLink (AnalysisDataSet beamtimeId dataSetId)) ] [ text ("Data Set ID " ++ String.fromInt dataSetId) ]
                    ]
                , li [ class "breadcrumb-item" ] [ text ("Merge Result ID " ++ String.fromInt mergeResultId) ]
                ]
            ]
        , h5_ [ text "Figures of merit" ]
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
                List.singleton <|
                    viewAlert [ AlertDanger ] <|
                        [ h4 [ class "alert-heading" ]
                            [ text "Failed to retrieve analysis results. Try reloading and if that doesn't work, contact the admins"
                            ]
                        , showError e
                        ]

            Success r ->
                [ modalMergeResultDetail model r
                ]


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        AnalysisResultsReceived analysisResults ->
            ( { model | analysisRequest = fromResult analysisResults }, Cmd.none )
