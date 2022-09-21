module Amarcord.Pages.Analysis exposing (Model, Msg, init, update, view)

import Amarcord.API.Requests exposing (AnalysisResultsExperimentType, AnalysisResultsRoot, RequestError, httpGetAnalysisResults)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoName, AttributoType, AttributoValue)
import Amarcord.AttributoHtml exposing (formatFloatHumanFriendly, formatIntHumanFriendly)
import Amarcord.Bootstrap exposing (AlertProperty(..), loadingBar, makeAlert)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Html exposing (div_, h2_, tbody_, td_, th_, thead_, tr_)
import Amarcord.Util exposing (HereAndNow)
import Dict exposing (Dict)
import Html exposing (Html, div, h4, table, text)
import Html.Attributes exposing (class)
import Maybe
import Maybe.Extra as MaybeExtra
import RemoteData exposing (RemoteData(..), fromResult)
import String exposing (join)
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
        viewResultRow : AnalysisResultsExperimentType -> List (Html msg)
        viewResultRow { dataSet, runs } =
            [ tr_
                [ td_ [ text (String.fromInt dataSet.id) ]
                , td_ [ viewDataSetTable attributi zone sampleIds dataSet False Nothing ]
                , td_ [ text <| join ", " runs ]
                , td_
                    [ text <|
                        if List.isEmpty runs then
                            ""

                        else
                            MaybeExtra.unwrap "" (\summary -> MaybeExtra.unwrap "" (\hr -> formatFloatHumanFriendly hr ++ "%") summary.hitRate) dataSet.summary
                    ]
                , td_
                    [ text <|
                        if List.isEmpty runs then
                            ""

                        else
                            MaybeExtra.unwrap "" (\summary -> MaybeExtra.unwrap "" (\hr -> formatFloatHumanFriendly hr ++ "%") summary.indexingRate) dataSet.summary
                    ]
                , td_
                    [ text <|
                        if List.isEmpty runs then
                            ""

                        else
                            MaybeExtra.unwrap "" (\summary -> formatIntHumanFriendly summary.indexedFrames) dataSet.summary
                    ]
                ]
            ]
    in
    div_
        [ h2_ [ text (first experimentTypeAndDataSets) ]
        , table [ class "table" ]
            [ thead_
                [ tr_
                    [ th_ [ text "Data Set ID" ]
                    , th_ [ text "Attributi" ]
                    , th_ [ text "Run IDs" ]
                    , th_ [ text "Hit Rate" ]
                    , th_ [ text "Indexing Rate" ]
                    , th_ [ text "Indexed Frames" ]
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
