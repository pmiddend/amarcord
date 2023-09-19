module Amarcord.Pages.RunAnalysis exposing (Model, Msg(..), init, update, view)

import Amarcord.API.DataSet exposing (DataSetSummary)
import Amarcord.API.Requests exposing (IndexingStatistic, RequestError, RunAnalysisResult, RunAnalysisResultsRoot, SimpleRun, httpGetRunAnalysisResults)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue)
import Amarcord.Bootstrap exposing (AlertProperty(..), loadingBar, makeAlert)
import Amarcord.Chemical exposing (Chemical, ChemicalId, chemicalIdDict)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.File exposing (File)
import Amarcord.Html exposing (h1_)
import Amarcord.Util exposing (HereAndNow)
import Axis
import Color
import Html exposing (Html, br, div, h4, span, table, tbody, td, text, th, thead, tr)
import Html.Attributes exposing (class, style)
import List.Extra as ListExtra
import Maybe
import Path exposing (Path)
import RemoteData exposing (RemoteData(..), fromResult)
import Scale exposing (ContinuousScale)
import Segment
import Shape
import Stat
import Statistics
import SubPath
import Time exposing (posixToMillis)
import Tuple exposing (second)
import TypedSvg exposing (g, svg)
import TypedSvg.Attributes exposing (fill, stroke, strokeDasharray, transform, viewBox)
import TypedSvg.Attributes.InPx exposing (strokeWidth)
import TypedSvg.Types exposing (Paint(..), Transform(..))


type Msg
    = RunAnalysisResultsReceived (Result RequestError RunAnalysisResultsRoot)


type alias Model =
    { hereAndNow : HereAndNow
    , analysisRequest : RemoteData RequestError RunAnalysisResultsRoot
    }


init : HereAndNow -> ( Model, Cmd Msg )
init hereAndNow =
    ( { hereAndNow = hereAndNow
      , analysisRequest = Loading
      }
    , httpGetRunAnalysisResults RunAnalysisResultsReceived
    )


xShiftColor : Color.Color
xShiftColor =
    Color.red


yShiftColor : Color.Color
yShiftColor =
    Color.green


viewDetectorShifts : List RunAnalysisResult -> Html msg
viewDetectorShifts r =
    let
        w : Float
        w =
            900

        h : Float
        h =
            450

        padding : Float
        padding =
            60

        runIdScale : ContinuousScale Float
        runIdScale =
            r
                |> List.map (\rr -> toFloat rr.runId)
                |> Statistics.extent
                |> Maybe.withDefault ( 1, 2 )
                |> Scale.linear ( 0.0, w - 2 * padding )

        xShiftScale : ContinuousScale Float
        xShiftScale =
            r
                |> List.concatMap (\rr -> rr.foms)
                |> List.filterMap (\fom -> fom.detectorShiftX)
                |> Statistics.extent
                |> Maybe.withDefault ( 0, 0 )
                |> Scale.linear ( h - 2 * padding, 0 )
                |> Scale.nice 4

        yShiftScale : ContinuousScale Float
        yShiftScale =
            r
                |> List.concatMap (\rr -> rr.foms)
                |> List.filterMap (\fom -> fom.detectorShiftY)
                |> Statistics.extent
                |> Maybe.withDefault ( 0, 0 )
                |> Scale.linear ( h - 2 * padding, 0 )
                |> Scale.nice 4

        runIdLineGenerator : ContinuousScale Float -> ( Int, Float ) -> Maybe ( Float, Float )
        runIdLineGenerator scale ( runId, amount ) =
            Just ( Scale.convert runIdScale (toFloat runId), Scale.convert scale amount )

        line : ContinuousScale Float -> (DataSetSummary -> Maybe Float) -> Path
        line scale accessor =
            List.filterMap
                (\rr ->
                    Maybe.map
                        (\sx -> ( rr.runId, sx ))
                    <|
                        List.head <|
                            List.filterMap
                                accessor
                                rr.foms
                )
                r
                |> List.map (runIdLineGenerator scale)
                |> Shape.line Shape.monotoneInXCurve
    in
    svg [ viewBox 0 0 w h ]
        [ g [ transform [ Translate (padding - 1) (h - padding) ] ]
            [ Axis.bottom [ Axis.tickCount 10 ] runIdScale ]
        , g [ transform [ Translate (padding - 1) padding ] ]
            [ Axis.left [] xShiftScale
            ]
        , g [ transform [ Translate (padding + second (Scale.range runIdScale) - 1.0) padding ] ]
            [ Axis.right [] yShiftScale
            ]
        , g [ transform [ Translate padding padding ] ]
            [ Path.element (line xShiftScale .detectorShiftX)
                [ stroke <| Paint <| xShiftColor
                , strokeWidth 3
                , fill PaintNone
                ]
            , Path.element (line yShiftScale .detectorShiftY)
                [ stroke <| Paint <| yShiftColor
                , strokeWidth 3
                , fill PaintNone
                ]
            ]
        ]


indexingRateColor : Color.Color
indexingRateColor =
    Color.green


hitRateColor : Color.Color
hitRateColor =
    Color.red


viewRunStatistics : IndexingStatistic -> List IndexingStatistic -> Html msg
viewRunStatistics firstStat stats =
    let
        w : Float
        w =
            900

        h : Float
        h =
            450

        padding : Float
        padding =
            60

        statTime stat =
            posixToMillis stat.time // 1000

        relativeStatTime stat =
            statTime stat - statTime firstStat

        runTimeScale : ContinuousScale Float
        runTimeScale =
            stats
                |> List.map (toFloat << relativeStatTime)
                |> Statistics.extent
                |> Maybe.withDefault ( 1, 2 )
                |> Scale.linear ( 0.0, w - 2 * padding )

        hitRateAccessor stat =
            toFloat stat.hits / toFloat stat.frames * 100.0

        avgHitRate =
            Maybe.withDefault 0.0 <| Stat.mean hitRateAccessor stats

        hitRateScale : ContinuousScale Float
        hitRateScale =
            stats
                |> List.map hitRateAccessor
                |> Statistics.extent
                |> Maybe.withDefault ( 0, 0 )
                |> Scale.linear ( h - 2 * padding, 0 )
                |> Scale.nice 4

        indexingRateAccessor stat =
            toFloat stat.indexedFrames / toFloat stat.hits * 100.0

        avgIndexingRate =
            Maybe.withDefault 0.0 <| Stat.mean indexingRateAccessor stats

        indexingRateScale : ContinuousScale Float
        indexingRateScale =
            stats
                |> List.map indexingRateAccessor
                |> Statistics.extent
                |> Maybe.withDefault ( 0, 0 )
                |> Scale.linear ( h - 2 * padding, 0 )
                |> Scale.nice 4

        line : ContinuousScale Float -> (IndexingStatistic -> Float) -> Path
        line scale accessor =
            List.map
                (\stat ->
                    Just
                        ( Scale.convert runTimeScale (toFloat (relativeStatTime stat))
                        , Scale.convert scale (accessor stat)
                        )
                )
                stats
                |> Shape.line Shape.monotoneInXCurve

        rightSide =
            padding + second (Scale.range runTimeScale) - 1.0
    in
    svg [ viewBox 0 0 w h ]
        [ g [ transform [ Translate (padding - 1) (h - padding) ] ]
            [ Axis.bottom [ Axis.tickCount 10 ] runTimeScale ]
        , g [ transform [ Translate (padding - 1) padding ] ]
            [ Axis.left [] hitRateScale
            ]
        , g [ transform [ Translate rightSide padding ] ]
            [ Axis.right [] indexingRateScale
            ]
        , g [ transform [ Translate padding padding ] ]
            [ Path.element (line hitRateScale hitRateAccessor)
                [ stroke <| Paint <| hitRateColor
                , strokeWidth 3
                , fill PaintNone
                ]
            , Path.element (line indexingRateScale indexingRateAccessor)
                [ stroke <| Paint <| indexingRateColor
                , strokeWidth 3
                , fill PaintNone
                ]
            ]
        , SubPath.element
            (SubPath.fromSegments
                [ Segment.line ( padding, Scale.convert indexingRateScale avgIndexingRate ) ( rightSide, Scale.convert indexingRateScale avgIndexingRate )
                ]
            )
            [ stroke <| Paint <| indexingRateColor
            , strokeDasharray "10,10"
            , strokeWidth 2
            , fill PaintNone
            ]
        , SubPath.element
            (SubPath.fromSegments
                [ Segment.line ( padding, Scale.convert hitRateScale avgHitRate ) ( rightSide, Scale.convert hitRateScale avgHitRate )
                ]
            )
            [ stroke <| Paint <| hitRateColor
            , strokeDasharray "5,5"
            , strokeWidth 2
            , fill PaintNone
            ]
        ]


viewRunTableRow :
    HereAndNow
    -> List (Attributo AttributoType)
    -> List (Chemical ChemicalId (AttributoMap AttributoValue) File)
    -> List SimpleRun
    -> RunAnalysisResult
    -> Html msg
viewRunTableRow hereAndNow attributi chemicals runs rar =
    let
        foundRun : Maybe SimpleRun
        foundRun =
            ListExtra.find (\r -> r.id == rar.runId) runs

        viewRun r =
            viewDataSetTable attributi hereAndNow.zone (chemicalIdDict chemicals) r.attributi False Nothing
    in
    tr []
        [ td []
            [ text (String.fromInt rar.runId)
            , br [] []
            , Maybe.withDefault (text "") <| Maybe.map viewRun foundRun
            ]
        , td []
            [ case List.head rar.indexingStatistics of
                Nothing ->
                    text ""

                Just first ->
                    viewRunStatistics first rar.indexingStatistics
            ]
        ]


viewRunGraphs :
    HereAndNow
    -> List (Attributo AttributoType)
    -> List (Chemical ChemicalId (AttributoMap AttributoValue) File)
    -> List SimpleRun
    -> List RunAnalysisResult
    -> Html msg
viewRunGraphs hereAndNow attributi chemicals runs rars =
    table [ class "table table-striped" ]
        [ thead [] [ tr [] [ th [] [ text "Run ID" ], th [ style "width" "100%" ] [ text "Statistics" ] ] ]
        , tbody [] (List.map (viewRunTableRow hereAndNow attributi chemicals runs) rars)
        ]


viewInner : HereAndNow -> RunAnalysisResultsRoot -> List (Html Msg)
viewInner hereAndNow { runs, attributi, chemicals, indexingResultsByRunId } =
    [ h1_ [ text "Detector Shifts" ]
    , div [ class "hstack gap-1" ]
        [ span [] [ span [ style "color" (Color.toCssString xShiftColor) ] [ text "■" ], text " X direction" ]
        , div [ class "vr" ] []
        , span [] [ span [ style "color" (Color.toCssString yShiftColor) ] [ text "■" ], text " Y direction" ]
        ]
    , viewDetectorShifts indexingResultsByRunId
    , h1_ [ text "Online Indexing Statistics" ]
    , div [ class "hstack gap-1" ]
        [ span [] [ span [ style "color" (Color.toCssString indexingRateColor) ] [ text "■" ], text " Indexing rate" ]
        , div [ class "vr" ] []
        , span [] [ span [ style "color" (Color.toCssString hitRateColor) ] [ text "■" ], text " Hit rate" ]
        ]
    , viewRunGraphs hereAndNow attributi chemicals runs indexingResultsByRunId
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
                viewInner model.hereAndNow r


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        RunAnalysisResultsReceived analysisResults ->
            ( { model | analysisRequest = fromResult analysisResults }, Cmd.none )
