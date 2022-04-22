module Amarcord.Pages.Analysis exposing (Model, Msg, init, update, view)

import Amarcord.API.Requests exposing (AnalysisResultsExperimentType, AnalysisResultsRoot, CfelAnalysisResult, RequestError, httpGetAnalysisResults)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoName, AttributoType, AttributoValue)
import Amarcord.AttributoHtml exposing (formatFloatHumanFriendly, formatIntHumanFriendly)
import Amarcord.Bootstrap exposing (AlertProperty(..), loadingBar, makeAlert)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.File as Amarcord
import Amarcord.Html exposing (br_, div_, h2_, img_, p_, tbody_, td_, th_, thead_, tr_)
import Amarcord.Route exposing (makeFilesLink)
import Amarcord.Util exposing (HereAndNow)
import Dict exposing (Dict)
import Html exposing (Html, a, div, figcaption, figure, h4, small, table, td, text, tr)
import Html.Attributes exposing (class, colspan, href, src)
import Maybe.Extra as MaybeExtra
import RemoteData exposing (RemoteData(..), fromResult)
import String
import Time exposing (Zone)
import Tuple exposing (first, second)


type Msg
    = AnalysisResultsReceived (Result RequestError AnalysisResultsRoot)


type alias Model =
    { hereAndNow : HereAndNow
    , analysisRequest : RemoteData RequestError AnalysisResultsRoot
    }


init : HereAndNow -> ( Model, Cmd Msg )
init hereAndNow =
    ( { hereAndNow = hereAndNow
      , analysisRequest = Loading
      }
    , httpGetAnalysisResults AnalysisResultsReceived
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
            [ "Resolution"
            , "RSplit"
            , "CCHalf"
            , "CC*"
            , "SNR"
            , "Completeness"
            , "Multiplicity"
            , "Total Measurements"
            , "Unique Reflections"
            , "Num. patterns"
            , "Num Hits"
            , "Indexed Patterns"
            , "Indexed Crystals"
            , "CC*-Rsplit"
            , "CrystFEL version"
            ]

        textTd =
            td_ << List.singleton << text

        floatTd =
            textTd << formatFloatHumanFriendly

        intTd =
            textTd << String.fromInt

        viewFile : Amarcord.File -> Html msg
        viewFile { id, type_, description, fileName, sizeInBytes, originalPath } =
            if String.startsWith "image/" type_ then
                div_ [ figure [ class "figure" ] [ img_ [ src ("api/files/" ++ String.fromInt id), class "figure-img img-fluid rounded" ], figcaption [ class "figure-caption" ] [ text description ] ] ]

            else
                let
                    originalPathSuffix =
                        case originalPath of
                            Nothing ->
                                []

                            Just op ->
                                [ br_, small [ class "text-muted" ] [ text <| "Original path: " ], small [ class "text-muted font-monospace" ] [ text op ] ]
                in
                p_
                    ([ a [ href (makeFilesLink id) ] [ text ("Download \"" ++ fileName ++ "\"") ], text (" (" ++ String.fromInt (sizeInBytes // 1024) ++ "KiB)") ] ++ originalPathSuffix)

        viewCfelAnalysisResultRows : CfelAnalysisResult -> List (Html msg)
        viewCfelAnalysisResultRows { directoryName, dataSetId, resolution, rsplit, cchalf, ccstar, snr, completeness, multiplicity, totalMeasurements, uniqueReflections, numPatterns, numHits, indexedPatterns, indexedCrystals, created, crystfelVersion, ccstarRSplit, files } =
            let
                dataRow =
                    tr_
                        [ textTd resolution
                        , floatTd rsplit
                        , floatTd cchalf
                        , floatTd ccstar
                        , floatTd snr
                        , floatTd completeness
                        , floatTd multiplicity
                        , intTd totalMeasurements
                        , intTd uniqueReflections
                        , intTd numPatterns
                        , intTd numHits
                        , intTd indexedPatterns
                        , intTd indexedCrystals
                        , floatTd ccstarRSplit
                        , textTd crystfelVersion
                        ]
            in
            if List.isEmpty files then
                [ dataRow ]

            else
                dataRow :: [ tr_ [ td [ colspan (List.length headerNamesAnalysisResults) ] (List.map viewFile files) ] ]

        viewResultRow : AnalysisResultsExperimentType -> List (Html msg)
        viewResultRow { dataSet, analysisResults, runs } =
            [ tr_
                [ td_ [ text (String.fromInt dataSet.id) ]
                , td_ [ viewDataSetTable attributi zone sampleIds dataSet False Nothing ]
                , td_ [ MaybeExtra.unwrap (text "") (\summary -> text (String.fromInt summary.numberOfRuns)) dataSet.summary ]
                , td_ [ text <| String.join ", " runs ]
                , td_ [ text <| MaybeExtra.unwrap "" (\summary -> formatIntHumanFriendly summary.frames) dataSet.summary ]
                , td_ [ text <| MaybeExtra.unwrap "" (\summary -> formatIntHumanFriendly summary.hits) dataSet.summary ]
                , td_
                    [ text <|
                        MaybeExtra.unwrap ""
                            (\summary ->
                                if summary.frames > 0 then
                                    formatFloatHumanFriendly (toFloat summary.hits / toFloat summary.frames * 100.0) ++ "%"

                                else
                                    ""
                            )
                            dataSet.summary
                    ]
                ]
            ]
                ++ (if List.isEmpty analysisResults then
                        []

                    else
                        [ tr [ class "table-secondary" ]
                            [ td [ colspan 7 ]
                                [ table [ class "table table-sm" ]
                                    [ thead_
                                        [ tr_
                                            (List.map (\header -> th_ [ text header ]) headerNamesAnalysisResults)
                                        ]
                                    , tbody_ (List.concatMap viewCfelAnalysisResultRows analysisResults)
                                    ]
                                ]
                            ]
                        ]
                   )
    in
    div_
        [ h2_ [ text (first experimentTypeAndDataSets) ]
        , table [ class "table" ]
            [ thead_
                [ tr_
                    [ th_ [ text "Data Set ID" ]
                    , th_ [ text "Attributi" ]
                    , th_ [ text "Number of Runs" ]
                    , th_ [ text "Runs" ]
                    , th_ [ text "Frames" ]
                    , th_ [ text "Hits" ]
                    , th_ [ text "Hit Rate" ]
                    ]
                ]
            , tbody_ (List.concatMap viewResultRow (second experimentTypeAndDataSets))
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
                List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve Attributi" ] ] ++ [ showRequestError e ]

            Success r ->
                viewInner model.hereAndNow.zone r


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        AnalysisResultsReceived analysisResults ->
            ( { model | analysisRequest = fromResult analysisResults }, Cmd.none )
