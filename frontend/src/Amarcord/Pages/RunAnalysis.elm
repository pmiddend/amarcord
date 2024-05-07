module Amarcord.Pages.RunAnalysis exposing (Model, Msg(..), init, update, view)

import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.API.RequestsHtml exposing (showHttpError)
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue, convertAttributoFromApi, convertAttributoMapFromApi)
import Amarcord.Bootstrap exposing (AlertProperty(..), loadingBar, makeAlert)
import Amarcord.Chemical exposing (Chemical, ChemicalId, chemicalIdDict, convertChemicalFromApi)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Html exposing (h1_, h5_)
import Amarcord.RunStatistics exposing (viewHitRateAndIndexingGraphs)
import Amarcord.Util exposing (HereAndNow)
import Api exposing (send)
import Api.Data exposing (JsonAnalysisRun, JsonFileOutput, JsonIndexingFom, JsonIndexingStatistic, JsonReadRunAnalysis, JsonRunAnalysisIndexingResult)
import Api.Request.Analysis exposing (readRunAnalysisApiRunAnalysisBeamtimeIdGet)
import Axis
import Color
import Html exposing (Html, br, div, h4, span, table, tbody, td, text, th, thead, tr)
import Html.Attributes exposing (class, style)
import Http
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
import Statistics
import SubPath
import Tuple
import TypedSvg exposing (g, line, svg, text_)
import TypedSvg.Attributes exposing (dominantBaseline, fill, stroke, strokeDasharray, textAnchor, transform, viewBox)
import TypedSvg.Attributes.InPx exposing (fontSize, strokeWidth, x, x1, x2, y, y1, y2)
import TypedSvg.Types exposing (AnchorAlignment(..), DominantBaseline(..), Paint(..), Transform(..))


type Msg
    = RunAnalysisResultsReceived (Result Http.Error JsonReadRunAnalysis)


type alias Model =
    { hereAndNow : HereAndNow
    , analysisRequest : RemoteData Http.Error JsonReadRunAnalysis
    , beamtimeId : BeamtimeId
    }


init : HereAndNow -> BeamtimeId -> ( Model, Cmd Msg )
init hereAndNow beamtimeId =
    ( { hereAndNow = hereAndNow
      , analysisRequest = Loading
      , beamtimeId = beamtimeId
      }
    , send RunAnalysisResultsReceived (readRunAnalysisApiRunAnalysisBeamtimeIdGet beamtimeId)
    )


xShiftColor : Color.Color
xShiftColor =
    Maybe.withDefault Color.red <| head Scale.Color.colorblind


yShiftColor : Color.Color
yShiftColor =
    Maybe.withDefault Color.red <| ListExtra.last Scale.Color.colorblind


viewDetectorShifts : List JsonRunAnalysisIndexingResult -> Html msg
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
                |> List.concatMap (\fom -> MaybeExtra.toList fom.detectorShiftXMm ++ MaybeExtra.toList fom.detectorShiftYMm)
                |> Statistics.extent
                |> Maybe.withDefault ( 0, 0 )
                |> Scale.linear ( h - yPadding, 0 )
                |> Scale.nice 4

        runIdLineGenerator : ContinuousScale Float -> ( Int, Float ) -> Maybe ( Float, Float )
        runIdLineGenerator scale ( runId, amount ) =
            Just ( Scale.convert runIdScale (toFloat runId), Scale.convert scale amount )

        plotLine : ContinuousScale Float -> (JsonIndexingFom -> Maybe Float) -> Path
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
        , g [ transform [ Translate (xPadding - 1) 0 ] ]
            [ SubPath.element
                (SubPath.fromSegments
                    [ Segment.line
                        ( 0, topPadding + Scale.convert shiftScale 0 )
                        ( Tuple.second (Scale.range runIdScale), topPadding + Scale.convert shiftScale 0 )
                    ]
                )
                [ stroke <| Paint <| Color.black
                , strokeDasharray "5,5"
                , strokeWidth 2
                , fill PaintNone
                ]
            ]
        , g [ transform [ Translate xPadding topPadding ] ]
            [ Path.element (plotLine shiftScale .detectorShiftXMm)
                [ stroke <| Paint <| xShiftColor
                , strokeWidth 2
                , fill PaintNone
                ]
            , Path.element (plotLine shiftScale .detectorShiftYMm)
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


viewRunStatistics : List JsonIndexingStatistic -> Html msg
viewRunStatistics originalStats =
    viewHitRateAndIndexingGraphs originalStats


viewRunTableRow :
    HereAndNow
    -> List (Attributo AttributoType)
    -> List (Chemical ChemicalId (AttributoMap AttributoValue) JsonFileOutput)
    -> List JsonAnalysisRun
    -> JsonRunAnalysisIndexingResult
    -> Html msg
viewRunTableRow hereAndNow attributi chemicals runs rar =
    let
        foundRun : Maybe JsonAnalysisRun
        foundRun =
            ListExtra.find (\r -> r.id == rar.runId) runs

        viewRun r =
            viewDataSetTable attributi hereAndNow.zone (chemicalIdDict chemicals) (convertAttributoMapFromApi r.attributi) False Nothing
    in
    tr []
        [ td []
            [ h5_ [ text <| "Run " ++ String.fromInt rar.runId ]
            , br [] []
            , Maybe.withDefault (text "") <| Maybe.map viewRun foundRun
            ]
        , td []
            [ viewRunStatistics rar.indexingStatistics
            ]
        ]


viewRunGraphs :
    HereAndNow
    -> List (Attributo AttributoType)
    -> List (Chemical ChemicalId (AttributoMap AttributoValue) JsonFileOutput)
    -> List JsonAnalysisRun
    -> List JsonRunAnalysisIndexingResult
    -> Html msg
viewRunGraphs hereAndNow attributi chemicals runs rars =
    table [ class "table table-striped" ]
        [ thead [] [ tr [] [ th [] [ text "Run Information" ], th [ style "width" "100%" ] [ text "Statistics" ] ] ]
        , tbody [] (List.map (viewRunTableRow hereAndNow attributi chemicals runs) rars)
        ]


viewInner : HereAndNow -> JsonReadRunAnalysis -> List (Html Msg)
viewInner hereAndNow { runs, attributi, chemicals, indexingResultsByRunId } =
    [ h1_ [ text "Detector Shifts" ]
    , viewDetectorShifts indexingResultsByRunId
    , h1_ [ text "Online Indexing Statistics" ]
    , div [ class "hstack gap-1" ]
        [ span [] [ span [ style "color" (Color.toCssString indexingRateColor) ] [ text "■" ], text " Indexing rate" ]
        , div [ class "vr" ] []
        , span [] [ span [ style "color" (Color.toCssString hitRateColor) ] [ text "■" ], text " Hit rate" ]
        ]
    , viewRunGraphs
        hereAndNow
        (List.map convertAttributoFromApi attributi)
        (List.map convertChemicalFromApi chemicals)
        runs
        indexingResultsByRunId
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
                List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve Attributi" ], showHttpError e ]

            Success r ->
                viewInner model.hereAndNow r


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        RunAnalysisResultsReceived analysisResults ->
            ( { model | analysisRequest = fromResult analysisResults }, Cmd.none )
