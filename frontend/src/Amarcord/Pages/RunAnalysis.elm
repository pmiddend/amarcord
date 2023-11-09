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
import Html exposing (Html, br, div, h4, input, label, span, table, tbody, td, text, th, thead, tr)
import Html.Attributes exposing (class, for, id, style, type_)
import Html.Events exposing (onClick)
import List exposing (head)
import List.Extra as ListExtra
import Maybe
import Maybe.Extra as MaybeExtra
import Path exposing (Path)
import RemoteData exposing (RemoteData(..), fromResult)
import Scale exposing (ContinuousScale)
import Scale.Color
import Segment
import Shape
import Stat
import Statistics
import SubPath
import Time exposing (posixToMillis)
import Tuple exposing (second)
import TypedSvg exposing (g, line, svg, text_)
import TypedSvg.Attributes exposing (dominantBaseline, fill, stroke, strokeDasharray, textAnchor, transform, viewBox)
import TypedSvg.Attributes.InPx exposing (fontSize, strokeWidth, x, x1, x2, y, y1, y2)
import TypedSvg.Types exposing (AnchorAlignment(..), DominantBaseline(..), Paint(..), Transform(..))


type Msg
    = RunAnalysisResultsReceived (Result RequestError RunAnalysisResultsRoot)
    | BinningPeriodChange (Maybe Int)


type alias Model =
    { hereAndNow : HereAndNow
    , analysisRequest : RemoteData RequestError RunAnalysisResultsRoot
    , binningPeriod : Maybe Int
    }


init : HereAndNow -> ( Model, Cmd Msg )
init hereAndNow =
    ( { hereAndNow = hereAndNow
      , analysisRequest = Loading
      , binningPeriod = Nothing
      }
    , httpGetRunAnalysisResults RunAnalysisResultsReceived
    )


xShiftColor : Color.Color
xShiftColor =
    Maybe.withDefault Color.red <| head Scale.Color.colorblind


yShiftColor : Color.Color
yShiftColor =
    Maybe.withDefault Color.red <| ListExtra.last Scale.Color.colorblind


viewDetectorShifts : List RunAnalysisResult -> Html msg
viewDetectorShifts r =
    let
        w : Float
        w =
            900

        h : Float
        h =
            450

        xPadding : Float
        xPadding =
            70

        topPadding : Float
        topPadding =
            30

        bottomPadding : Float
        bottomPadding =
            70

        largeFontSize =
            16

        smallFontSize =
            14

        yPadding : Float
        yPadding =
            topPadding + bottomPadding

        runIdScale : ContinuousScale Float
        runIdScale =
            r
                |> List.map (\rr -> toFloat rr.runId)
                |> Statistics.extent
                |> Maybe.withDefault ( 1, 2 )
                |> Scale.linear ( 0.0, w - 2 * xPadding )

        shiftScale : ContinuousScale Float
        shiftScale =
            r
                |> List.concatMap (\rr -> rr.foms)
                |> List.concatMap (\fom -> MaybeExtra.toList fom.detectorShiftX ++ MaybeExtra.toList fom.detectorShiftY)
                |> Statistics.extent
                |> Maybe.withDefault ( 0, 0 )
                |> Scale.linear ( h - yPadding, 0 )
                |> Scale.nice 4

        runIdLineGenerator : ContinuousScale Float -> ( Int, Float ) -> Maybe ( Float, Float )
        runIdLineGenerator scale ( runId, amount ) =
            Just ( Scale.convert runIdScale (toFloat runId), Scale.convert scale amount )

        plotLine : ContinuousScale Float -> (DataSetSummary -> Maybe Float) -> Path
        plotLine scale accessor =
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
                |> Shape.line Shape.linearCurve

        xAxisLabel =
            text_
                [ y (h - bottomPadding / 2)
                , x (xPadding + (w - xPadding) / 2)
                , textAnchor AnchorMiddle
                , fontSize largeFontSize
                ]
                [ text "Run ID"
                ]

        yAxisLabel =
            text_
                [ textAnchor AnchorMiddle
                , dominantBaseline DominantBaselineMiddle
                , transform [ Rotate 270 (xPadding * 0.25) (h / 2.0), Translate (xPadding * 0.25) (h / 2.0) ]
                , fontSize largeFontSize
                ]
                [ text "Shift (mm)"
                ]

        xAxisLegend =
            g []
                [ line [ x1 xPadding, x2 (xPadding + smallFontSize + 2), y1 (topPadding / 2.0), y2 (topPadding / 2.0), stroke (Paint xShiftColor), strokeWidth 2 ] []
                , text_
                    [ dominantBaseline DominantBaselineMiddle
                    , y (topPadding / 2.0)
                    , x (xPadding + smallFontSize + 6)
                    , fontSize smallFontSize
                    ]
                    [ text "X direction" ]
                ]

        yAxisLegend =
            let
                xBasePoint =
                    100
            in
            g []
                [ line
                    [ x1 (xPadding + xBasePoint)
                    , x2 (xPadding + xBasePoint + smallFontSize + 2)
                    , y1 (topPadding / 2.0)
                    , y2 (topPadding / 2.0)
                    , stroke (Paint yShiftColor)
                    , strokeWidth 2
                    ]
                    []
                , text_
                    [ dominantBaseline DominantBaselineMiddle
                    , y (topPadding / 2.0)
                    , x (xPadding + xBasePoint + smallFontSize + 6)
                    , fontSize smallFontSize
                    ]
                    [ text "Y direction" ]
                ]
    in
    svg [ viewBox 0 0 w h ]
        [ g [ transform [ Translate (xPadding - 1) (h - bottomPadding) ] ]
            [ Axis.bottom [ Axis.tickCount 10 ] runIdScale ]
        , g [ transform [ Translate (xPadding - 1) topPadding ] ]
            [ Axis.left [] shiftScale
            ]
        , xAxisLabel
        , yAxisLabel
        , xAxisLegend
        , yAxisLegend
        , g [ transform [ Translate xPadding topPadding ] ]
            [ Path.element (plotLine shiftScale .detectorShiftX)
                [ stroke <| Paint <| xShiftColor
                , strokeWidth 2
                , fill PaintNone
                ]
            , Path.element (plotLine shiftScale .detectorShiftY)
                [ stroke <| Paint <| yShiftColor
                , strokeWidth 2
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


foldNeighbors : (Maybe a -> a -> b) -> List a -> List b
foldNeighbors f xs =
    let
        transducer : a -> ( Maybe a, List b ) -> ( Maybe a, List b )
        transducer newElement ( priorElement, priorList ) =
            ( Just newElement, f priorElement newElement :: priorList )
    in
    second <| List.foldl transducer ( Nothing, [] ) xs


viewRunStatistics : Maybe Int -> IndexingStatistic -> List IndexingStatistic -> Html msg
viewRunStatistics binningPeriod firstStat originalStats =
    let
        convertStat : Maybe IndexingStatistic -> IndexingStatistic -> IndexingStatistic
        convertStat leftNeighbor element =
            case leftNeighbor of
                Nothing ->
                    element

                Just ln ->
                    { element
                        | frames = element.frames - ln.frames
                        , hits = element.hits - ln.hits
                        , indexedFrames = element.indexedFrames - ln.indexedFrames
                        , indexedCrystals = element.indexedCrystals - ln.indexedCrystals
                    }

        binStats : Int -> List IndexingStatistic -> List IndexingStatistic
        binStats binSeconds =
            let
                transducer newElement ( oldBatch, oldList ) =
                    case ListExtra.last oldBatch of
                        Nothing ->
                            ( [ newElement ], oldList )

                        Just oldestElement ->
                            if posixToMillis oldestElement.time - posixToMillis newElement.time > binSeconds * 1000 then
                                let
                                    compressedBatch =
                                        { oldestElement
                                            | frames = List.sum (List.map .frames oldBatch)
                                            , hits = List.sum (List.map .hits oldBatch)
                                            , indexedFrames = List.sum (List.map .indexedFrames oldBatch)
                                            , indexedCrystals = List.sum (List.map .indexedCrystals oldBatch)
                                        }
                                in
                                ( [ newElement ], compressedBatch :: oldList )

                            else
                                ( newElement :: oldBatch, oldList )
            in
            second << List.foldl transducer ( [], [] )

        stats =
            case binningPeriod of
                Just binSeconds ->
                    binStats binSeconds (foldNeighbors convertStat originalStats)

                Nothing ->
                    originalStats

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
            if stat.frames > 0 then
                toFloat stat.hits / toFloat stat.frames * 100.0

            else
                0

        avgHitRate =
            Maybe.withDefault 0.0 <| Stat.mean hitRateAccessor stats

        hitRateScale : ContinuousScale Float
        hitRateScale =
            let
                hitRates =
                    avgHitRate :: List.map hitRateAccessor stats
            in
            hitRates
                |> Statistics.extent
                |> Maybe.map (\( _, maxHr ) -> ( 0, maxHr ))
                |> Maybe.withDefault ( 0, 0 )
                |> Scale.linear ( h - 2 * padding, 0 )

        indexingRateAccessor stat =
            if stat.hits > 0 then
                toFloat stat.indexedFrames / toFloat stat.hits * 100.0

            else
                0

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
                |> Shape.line Shape.linearCurve

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
        , g [ transform [ Translate 0 padding ] ]
            [ SubPath.element
                (SubPath.fromSegments
                    [ Segment.line ( padding, Scale.convert hitRateScale avgHitRate ) ( rightSide, Scale.convert hitRateScale avgHitRate ) ]
                )
                [ stroke <| Paint <| hitRateColor
                , strokeDasharray "5,5"
                , strokeWidth 2
                , fill PaintNone
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
            ]
        ]


viewRunTableRow :
    HereAndNow
    -> Maybe Int
    -> List (Attributo AttributoType)
    -> List (Chemical ChemicalId (AttributoMap AttributoValue) File)
    -> List SimpleRun
    -> RunAnalysisResult
    -> Html msg
viewRunTableRow hereAndNow binningPeriod attributi chemicals runs rar =
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
                    viewRunStatistics binningPeriod first rar.indexingStatistics
            ]
        ]


viewRunGraphs :
    HereAndNow
    -> Maybe Int
    -> List (Attributo AttributoType)
    -> List (Chemical ChemicalId (AttributoMap AttributoValue) File)
    -> List SimpleRun
    -> List RunAnalysisResult
    -> Html msg
viewRunGraphs hereAndNow binningPeriod attributi chemicals runs rars =
    table [ class "table table-striped" ]
        [ thead [] [ tr [] [ th [] [ text "Run ID" ], th [ style "width" "100%" ] [ text "Statistics" ] ] ]
        , tbody [] (List.map (viewRunTableRow hereAndNow binningPeriod attributi chemicals runs) rars)
        ]


viewInner : HereAndNow -> Maybe Int -> RunAnalysisResultsRoot -> List (Html Msg)
viewInner hereAndNow binningPeriod { runs, attributi, chemicals, indexingResultsByRunId } =
    [ h1_ [ text "Detector Shifts" ]
    , viewDetectorShifts indexingResultsByRunId
    , h1_ [ text "Online Indexing Statistics" ]
    , div [ class "form-check mb-3" ]
        [ input
            [ class "form-check-input"
            , type_ "checkbox"
            , id "use-binning"
            , onClick
                (BinningPeriodChange <|
                    MaybeExtra.unwrap (Just 20) (always Nothing) binningPeriod
                )
            ]
            []
        , label [ class "form-check-label", for "use-binning" ] [ text "Use binning" ]
        , div [ class "form-text" ] [ text "This will show a binned (windowed) version of the indexing and hit rate graphs." ]
        ]
    , div [ class "hstack gap-1" ]
        [ span [] [ span [ style "color" (Color.toCssString indexingRateColor) ] [ text "■" ], text " Indexing rate" ]
        , div [ class "vr" ] []
        , span [] [ span [ style "color" (Color.toCssString hitRateColor) ] [ text "■" ], text " Hit rate" ]
        ]
    , viewRunGraphs hereAndNow binningPeriod attributi chemicals runs indexingResultsByRunId
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
                viewInner model.hereAndNow model.binningPeriod r


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        RunAnalysisResultsReceived analysisResults ->
            ( { model | analysisRequest = fromResult analysisResults }, Cmd.none )

        BinningPeriodChange newBinningPeriod ->
            ( { model | binningPeriod = newBinningPeriod }, Cmd.none )
