module Amarcord.Pages.Analysis exposing (Model, Msg, init, update, view)

import Amarcord.API.Requests exposing (AnalysisResultsExperimentType, AnalysisResultsRoot, CfelAnalysisResult, RequestError, httpGetAnalysisResults)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoName, AttributoType, AttributoValue)
import Amarcord.AttributoHtml exposing (formatFloatHumanFriendly)
import Amarcord.Bootstrap exposing (AlertProperty(..), loadingBar, makeAlert)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Html exposing (div_, h2_, tbody_, td_, th_, thead_, tr_)
import Amarcord.Pages.DataSets exposing (DataSetModel, DataSetMsg, initDataSet, updateDataSet, viewDataSet)
import Amarcord.Pages.ExperimentTypes exposing (ExperimentTypeModel, ExperimentTypeMsg, initExperimentType, updateExperimentType, viewExperimentType)
import Amarcord.Util exposing (HereAndNow)
import Dict exposing (Dict)
import Html exposing (Html, div, h4, table, td, text)
import Html.Attributes exposing (class, colspan, style)
import Maybe.Extra as MaybeExtra
import RemoteData exposing (RemoteData(..), fromResult)
import String
import Time exposing (Zone)
import Tuple exposing (first, mapBoth, second)


type Msg
    = AnalysisResultsReceived (Result RequestError AnalysisResultsRoot)
    | ExperimentTypeMsg ExperimentTypeMsg
    | DataSetMsg DataSetMsg


type alias Model =
    { hereAndNow : HereAndNow
    , analysisRequest : RemoteData RequestError AnalysisResultsRoot
    , experimentTypeModel : ExperimentTypeModel
    , dataSetModel : DataSetModel
    }


init : HereAndNow -> ( Model, Cmd Msg )
init hereAndNow =
    ( { hereAndNow = hereAndNow
      , analysisRequest = Loading
      , experimentTypeModel = first initExperimentType
      , dataSetModel = first (initDataSet hereAndNow.zone)
      }
    , Cmd.batch
        [ httpGetAnalysisResults AnalysisResultsReceived
        , Cmd.map ExperimentTypeMsg <| second initExperimentType
        , Cmd.map DataSetMsg <| second (initDataSet hereAndNow.zone)
        ]
    )


viewInner : Zone -> AnalysisResultsRoot -> List (Html Msg)
viewInner zone { experimentTypes, attributi, sampleIdToName } =
    List.map
        (viewResultsTableForSingleExperimentType attributi zone sampleIdToName)
        (Dict.toList experimentTypes)


viewResultsTableForSingleExperimentType :
    List (Attributo AttributoType)
    -> Zone
    -> Dict Int String
    -> ( String, List AnalysisResultsExperimentType )
    -> Html msg
viewResultsTableForSingleExperimentType attributi zone sampleIds experimentTypeAndDataSets =
    let
        headerNamesAnalysisResults =
            [ "Runs"

            --, "Directory"
            , "Resolution"
            , "RSplit"
            , "CCHalf"
            , "CC*"
            , "SNR"
            , "Completeness"
            , "Multiplicity"
            , "Total Measurements"
            , "Unique Reflections"
            , "Wilson B"
            , "Outer Shell"
            , "Num. patterns"
            , "Num Hits"
            , "Indexed Patterns"
            , "Indexed Crystals"

            --, "Comment"
            ]

        textTd =
            td_ << List.singleton << text

        floatTd =
            textTd << formatFloatHumanFriendly

        intTd =
            textTd << String.fromInt

        viewCfelAnalysisResultRow : CfelAnalysisResult -> Html msg
        viewCfelAnalysisResultRow { directoryName, runFrom, runTo, resolution, rsplit, cchalf, ccstar, snr, completeness, multiplicity, totalMeasurements, uniqueReflections, wilsonB, outerShell, numPatterns, numHits, indexedPatterns, indexedCrystals, comment } =
            tr_
                [ td [ style "white-space" "nowrap" ] [ text <| String.fromInt runFrom ++ "-" ++ String.fromInt runTo ]

                --, textTd directoryName
                , textTd resolution
                , floatTd rsplit
                , floatTd cchalf
                , floatTd ccstar
                , floatTd snr
                , floatTd completeness
                , floatTd multiplicity
                , intTd totalMeasurements
                , intTd uniqueReflections
                , floatTd wilsonB
                , textTd outerShell
                , intTd numPatterns
                , intTd numHits
                , intTd indexedPatterns
                , intTd indexedCrystals

                --, textTd comment
                ]

        viewResultRow : AnalysisResultsExperimentType -> List (Html msg)
        viewResultRow { dataSet, analysisResults } =
            [ tr_
                [ td_ [ text (String.fromInt dataSet.id) ]
                , td_ [ viewDataSetTable attributi zone sampleIds dataSet Nothing ]
                , td_ [ MaybeExtra.unwrap (text "") (\summary -> text (String.fromInt summary.numberOfRuns)) dataSet.summary ]
                , td_ [ MaybeExtra.unwrap (text "") (\summary -> text (String.fromInt summary.frames)) dataSet.summary ]
                , td_ [ MaybeExtra.unwrap (text "") (\summary -> text (String.fromInt summary.hits)) dataSet.summary ]
                ]
            ]
                ++ (if List.isEmpty analysisResults then
                        []

                    else
                        [ tr_
                            [ td [ colspan 2 ]
                                [ table [ class "table table-sm" ]
                                    [ thead_
                                        [ tr_
                                            (List.map (\header -> th_ [ text header ]) headerNamesAnalysisResults)
                                        ]
                                    , tbody_ (List.map viewCfelAnalysisResultRow analysisResults)
                                    ]
                                ]
                            ]
                        ]
                   )
    in
    div_
        [ h2_ [ text (first experimentTypeAndDataSets) ]
        , table [ class "table table-striped" ]
            [ thead_
                [ tr_
                    [ td_ [ text "Data Set ID" ]
                    , td_ [ text "Attributi" ]
                    , td_ [ text "Number of Runs" ]
                    , td_ [ text "Frames" ]
                    , td_ [ text "Hits" ]
                    ]
                ]
            , tbody_ (List.concatMap viewResultRow (second experimentTypeAndDataSets))
            ]
        ]


view : Model -> Html Msg
view model =
    div [ class "container" ] <|
        (List.map (Html.map ExperimentTypeMsg) <| viewExperimentType model.experimentTypeModel)
            ++ (List.map (Html.map DataSetMsg) <| viewDataSet model.dataSetModel)
            ++ (case model.analysisRequest of
                    NotAsked ->
                        List.singleton <| text ""

                    Loading ->
                        List.singleton <| loadingBar "Loading analysis results..."

                    Failure e ->
                        List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve Attributi" ] ] ++ [ showRequestError e ]

                    Success r ->
                        viewInner model.hereAndNow.zone r
               )


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        AnalysisResultsReceived analysisResults ->
            ( { model | analysisRequest = fromResult analysisResults }, Cmd.none )

        ExperimentTypeMsg experimentTypeMsg ->
            mapBoth (\newModel -> { model | experimentTypeModel = newModel }) (Cmd.map ExperimentTypeMsg) <| updateExperimentType experimentTypeMsg model.experimentTypeModel

        DataSetMsg dataSetMsg ->
            mapBoth (\newModel -> { model | dataSetModel = newModel }) (Cmd.map DataSetMsg) <| updateDataSet dataSetMsg model.dataSetModel
