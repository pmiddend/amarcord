module Amarcord.Pages.Geometry exposing (Model, Msg(..), init, pageTitle, update, view)

import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.AttributoHtml exposing (formatFloatHumanFriendly)
import Amarcord.Bootstrap exposing (AlertProperty(..), loadingBar, makeAlert)
import Amarcord.Html exposing (div_, h1_, input_, tbody_, td_, th_, thead_, tr_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.Util exposing (foldPairs)
import Api.Data exposing (JsonAlignDetectorGroup, JsonDetectorShift, JsonReadBeamtimeGeometryDetails)
import Api.Request.Analysis exposing (readBeamtimeGeometryDetailsApiRunAnalysisBeamtimeIdGeometryGet)
import Axis
import Color
import Curve
import Html exposing (Html, div, h4, label, table, text)
import Html.Attributes exposing (checked, class, for, id, name, type_)
import Html.Events exposing (onInput)
import List
import List.Extra
import Maybe
import Maybe.Extra
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
import TypedSvg.Core exposing (Svg)
import TypedSvg.Types exposing (AnchorAlignment(..), DominantBaseline(..), Paint(..), Transform(..))


type DetectorShiftMode
    = ByTime
    | ByRunId


type Msg
    = GeometryDetailsReceived (Result HttpError JsonReadBeamtimeGeometryDetails)
    | DetectorShiftModeChange DetectorShiftMode


type alias Model =
    { geometryDetailsRequest : RemoteData HttpError JsonReadBeamtimeGeometryDetails
    , beamtimeId : BeamtimeId
    , detectorShiftMode : DetectorShiftMode
    }


pageTitle : Model -> String
pageTitle _ =
    "Geometry"


init : BeamtimeId -> ( Model, Cmd Msg )
init beamtimeId =
    ( { geometryDetailsRequest = Loading
      , beamtimeId = beamtimeId
      , detectorShiftMode = ByRunId
      }
    , send GeometryDetailsReceived (readBeamtimeGeometryDetailsApiRunAnalysisBeamtimeIdGeometryGet beamtimeId)
      -- ,
    )


xShiftColor : Color.Color
xShiftColor =
    Maybe.withDefault Color.red <| List.Extra.getAt 0 Scale.Color.colorblind


yShiftColor : Color.Color
yShiftColor =
    Maybe.withDefault Color.red <| List.Extra.getAt 2 Scale.Color.colorblind


zShiftColor : Color.Color
zShiftColor =
    Maybe.withDefault Color.red <| List.Extra.getAt 5 Scale.Color.colorblind


detectorShiftWidth : Float
detectorShiftWidth =
    800


detectorShiftHeight : Float
detectorShiftHeight =
    350


retrieveGroup : String -> JsonDetectorShift -> Maybe JsonAlignDetectorGroup
retrieveGroup desiredGroup =
    List.Extra.find (\{ group } -> group == desiredGroup) << .alignDetectorGroups


w : Float
w =
    detectorShiftWidth


h : Float
h =
    detectorShiftHeight


xPadding : Float
xPadding =
    70


topPadding : Float
topPadding =
    30


bottomPadding : Float
bottomPadding =
    70


largeFontSize : Float
largeFontSize =
    16


smallFontSize : Float
smallFontSize =
    14


yPadding : Float
yPadding =
    topPadding + bottomPadding


geometryChangeExternalRunIds : List JsonDetectorShift -> List Int
geometryChangeExternalRunIds shifts =
    let
        folder ( currentRun, nextRun ) =
            if currentRun.geometryHash /= nextRun.geometryHash then
                Just <| nextRun.runExternalId

            else
                Nothing
    in
    List.filterMap identity (foldPairs shifts folder)


runIdScale : List JsonDetectorShift -> ContinuousScale Float
runIdScale shifts =
    shifts
        |> List.map (\rr -> toFloat rr.runExternalId)
        |> Statistics.extent
        |> Maybe.withDefault ( 1, 2 )
        |> Scale.linear ( 0.0, w - 2 * xPadding )


shiftScale : List JsonDetectorShift -> ContinuousScale Float
shiftScale shifts =
    shifts
        |> List.concatMap .alignDetectorGroups
        |> List.concatMap
            (\{ xTranslationMm, yTranslationMm, zTranslationMm } ->
                [ xTranslationMm, yTranslationMm ] ++ Maybe.withDefault [] (Maybe.map (\x -> [ x ]) zTranslationMm)
            )
        |> Statistics.extent
        |> Maybe.withDefault ( 0, 0 )
        |> Scale.linear ( h - yPadding, 0 )
        |> Scale.nice 4


runIdAxisLabel : Svg msg
runIdAxisLabel =
    text_
        [ y (h - bottomPadding / 2)
        , x (xPadding + (w - xPadding) / 2)
        , textAnchor AnchorMiddle
        , fontSize largeFontSize
        ]
        [ text "Run ID"
        ]


shiftAxisLabel : Svg msg
shiftAxisLabel =
    text_
        [ textAnchor AnchorMiddle
        , dominantBaseline DominantBaselineMiddle
        , transform [ Rotate 270 (xPadding * 0.25) (h / 2.0), Translate (xPadding * 0.25) (h / 2.0) ]
        , fontSize largeFontSize
        ]
        [ text "Shift (mm)"
        ]


rotationScale : List JsonDetectorShift -> ContinuousScale Float
rotationScale r =
    r
        |> List.concatMap .alignDetectorGroups
        |> List.concatMap
            (\{ xRotationDeg, yRotationDeg } ->
                Maybe.Extra.values [ xRotationDeg, yRotationDeg ]
            )
        |> Statistics.extent
        |> Maybe.withDefault ( 0, 0 )
        |> Scale.linear ( h - yPadding, 0 )
        |> Scale.nice 4


viewDetectorShiftsByRunId : List JsonDetectorShift -> Html msg
viewDetectorShiftsByRunId r =
    let
        runIdLineGenerator : ContinuousScale Float -> ( Int, Float ) -> Maybe ( Float, Float )
        runIdLineGenerator scale ( runId, amount ) =
            Just ( Scale.convert (runIdScale r) (toFloat runId), Scale.convert scale amount )

        plotLine : ContinuousScale Float -> (JsonDetectorShift -> Maybe Float) -> Path
        plotLine scale accessor =
            List.filterMap (\rr -> accessor rr |> Maybe.map (\floatValue -> ( rr.runExternalId, floatValue ))) r
                |> List.map (runIdLineGenerator scale)
                |> Shape.line Shape.linearCurve

        xAxisLegend =
            g []
                [ line
                    [ x1 xPadding
                    , x2 (xPadding + smallFontSize + 2)
                    , y1 (topPadding / 2.0)
                    , y2 (topPadding / 2.0)
                    , stroke (Paint xShiftColor)
                    , strokeWidth 2
                    ]
                    []
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

        zAxisLegend =
            let
                xBasePoint =
                    200
            in
            g []
                [ line
                    [ x1 (xPadding + xBasePoint)
                    , x2 (xPadding + xBasePoint + smallFontSize + 2)
                    , y1 (topPadding / 2.0)
                    , y2 (topPadding / 2.0)
                    , stroke (Paint zShiftColor)
                    , strokeWidth 2
                    ]
                    []
                , text_
                    [ dominantBaseline DominantBaselineMiddle
                    , y (topPadding / 2.0)
                    , x (xPadding + xBasePoint + smallFontSize + 6)
                    , fontSize smallFontSize
                    ]
                    [ text "Z direction" ]
                ]

        geometryChangeLegend =
            let
                xBasePoint =
                    300
            in
            g []
                [ line
                    [ x1 (xPadding + xBasePoint)
                    , x2 (xPadding + xBasePoint + smallFontSize + 2)
                    , y1 (topPadding / 2.0)
                    , y2 (topPadding / 2.0)
                    , strokeDasharray "5,5"
                    , strokeWidth 1
                    , stroke (Paint Color.black)
                    , strokeWidth 2
                    ]
                    []
                , text_
                    [ dominantBaseline DominantBaselineMiddle
                    , y (topPadding / 2.0)
                    , x (xPadding + xBasePoint + smallFontSize + 6)
                    , fontSize smallFontSize
                    ]
                    [ text "geometry change" ]
                ]

        shiftPlot =
            svg [ viewBox 0 0 w h ]
                [ g [ transform [ Translate (xPadding - 1) (h - bottomPadding) ] ]
                    [ Axis.bottom [ Axis.tickCount 10 ] (runIdScale r) ]
                , g [ transform [ Translate (xPadding - 1) topPadding ] ]
                    [ Axis.left [] (shiftScale r)
                    ]
                , runIdAxisLabel
                , shiftAxisLabel
                , xAxisLegend
                , yAxisLegend
                , zAxisLegend
                , geometryChangeLegend
                , g [ transform [ Translate (xPadding - 1) 0 ] ]
                    [ SubPath.element
                        (SubPath.fromSegments
                            [ Segment.line
                                ( 0, topPadding + Scale.convert (shiftScale r) 0 )
                                ( Tuple.second (Scale.range (runIdScale r)), topPadding + Scale.convert (shiftScale r) 0 )
                            ]
                        )
                        [ stroke <| Paint <| Color.lightCharcoal
                        , strokeWidth 0.5
                        , fill PaintNone
                        ]
                    ]
                , g [ transform [ Translate xPadding topPadding ] ]
                    (List.map
                        (\geometryChangeRunId ->
                            let
                                xValueOfVerticalLine =
                                    Scale.convert (runIdScale r) (toFloat geometryChangeRunId)
                            in
                            SubPath.element
                                (SubPath.fromSegments
                                    [ Segment.line
                                        ( xValueOfVerticalLine, Tuple.first (Scale.range (shiftScale r)) )
                                        ( xValueOfVerticalLine, Tuple.second (Scale.range (shiftScale r)) )
                                    ]
                                )
                                [ stroke <| Paint <| Color.black
                                , strokeDasharray "5,5"
                                , strokeWidth 1
                                , fill PaintNone
                                ]
                        )
                        (geometryChangeExternalRunIds r)
                    )
                , g [ transform [ Translate xPadding topPadding ] ]
                    [ Path.element (plotLine (shiftScale r) (Maybe.map .xTranslationMm << retrieveGroup "all"))
                        [ stroke <| Paint <| xShiftColor
                        , strokeWidth 1
                        , fill PaintNone
                        ]
                    , Path.element (plotLine (shiftScale r) (Maybe.map .yTranslationMm << retrieveGroup "all"))
                        [ stroke <| Paint <| yShiftColor
                        , strokeWidth 1
                        , fill PaintNone
                        ]
                    , Path.element (plotLine (shiftScale r) (Maybe.andThen .zTranslationMm << retrieveGroup "all"))
                        [ stroke <| Paint <| zShiftColor
                        , strokeWidth 1
                        , fill PaintNone
                        ]
                    ]
                ]

        yRotationAxisLabel =
            text_
                [ textAnchor AnchorMiddle
                , dominantBaseline DominantBaselineMiddle
                , transform [ Rotate 270 (xPadding * 0.25) (h / 2.0), Translate (xPadding * 0.25) (h / 2.0) ]
                , fontSize largeFontSize
                ]
                [ text "Rotation [°]"
                ]

        xRotationAxisLegend =
            g []
                [ line [ x1 xPadding, x2 (xPadding + smallFontSize + 2), y1 (topPadding / 2.0), y2 (topPadding / 2.0), stroke (Paint xShiftColor), strokeWidth 2 ] []
                , text_
                    [ dominantBaseline DominantBaselineMiddle
                    , y (topPadding / 2.0)
                    , x (xPadding + smallFontSize + 6)
                    , fontSize smallFontSize
                    ]
                    [ text "X rotation" ]
                ]

        yRotationAxisLegend =
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
                    [ text "Y rotation" ]
                ]

        rotationPlot =
            svg [ viewBox 0 0 w h ]
                [ g [ transform [ Translate (xPadding - 1) (h - bottomPadding) ] ]
                    [ Axis.bottom [ Axis.tickCount 10 ] (runIdScale r) ]
                , g [ transform [ Translate (xPadding - 1) topPadding ] ]
                    [ Axis.left [] (rotationScale r)
                    ]
                , runIdAxisLabel
                , yRotationAxisLabel
                , xRotationAxisLegend
                , yRotationAxisLegend
                , geometryChangeLegend
                , g [ transform [ Translate (xPadding - 1) 0 ] ]
                    [ SubPath.element
                        (SubPath.fromSegments
                            [ Segment.line
                                ( 0, topPadding + Scale.convert (rotationScale r) 0 )
                                ( Tuple.second (Scale.range (runIdScale r)), topPadding + Scale.convert (rotationScale r) 0 )
                            ]
                        )
                        [ stroke <| Paint <| Color.lightCharcoal
                        , strokeWidth 0.5
                        , fill PaintNone
                        ]
                    ]
                , g [ transform [ Translate xPadding topPadding ] ]
                    (List.map
                        (\geometryChangeRunId ->
                            let
                                xValueOfVerticalLine =
                                    Scale.convert (runIdScale r) (toFloat geometryChangeRunId)
                            in
                            SubPath.element
                                (SubPath.fromSegments
                                    [ Segment.line
                                        ( xValueOfVerticalLine, Tuple.first (Scale.range (rotationScale r)) )
                                        ( xValueOfVerticalLine, Tuple.second (Scale.range (rotationScale r)) )
                                    ]
                                )
                                [ stroke <| Paint <| Color.black
                                , strokeDasharray "5,5"
                                , strokeWidth 1
                                , fill PaintNone
                                ]
                        )
                        (geometryChangeExternalRunIds r)
                    )
                , g [ transform [ Translate xPadding topPadding ] ]
                    [ Path.element (plotLine (rotationScale r) (Maybe.andThen .xRotationDeg << retrieveGroup "all"))
                        [ stroke <| Paint <| xShiftColor
                        , strokeWidth 1
                        , fill PaintNone
                        ]
                    , Path.element (plotLine (rotationScale r) (Maybe.andThen .yRotationDeg << retrieveGroup "all"))
                        [ stroke <| Paint <| yShiftColor
                        , strokeWidth 1
                        , fill PaintNone
                        ]
                    ]
                ]
    in
    div_
        [ shiftPlot
        , rotationPlot
        ]


viewDetectorShiftsByAbsoluteTime : List JsonDetectorShift -> Html msg
viewDetectorShiftsByAbsoluteTime r =
    let
        convertDuration x =
            toFloat (x // 1000 // 60)

        runDuration rr =
            convertDuration <| (Maybe.withDefault rr.runStart rr.runEnd - rr.runStart)

        firstRunStart =
            List.head r |> Maybe.map .runStart |> Maybe.withDefault 0

        runDurationSpan : Float
        runDurationSpan =
            case List.Extra.last r of
                Nothing ->
                    0.0

                Just lastRun ->
                    convertDuration <| Maybe.withDefault 0 lastRun.runEnd - firstRunStart

        runTimeScale : ContinuousScale Float
        runTimeScale =
            Scale.linear ( 0.0, w - 2 * xPadding ) ( 0, runDurationSpan )

        geometryChanges : List Float
        geometryChanges =
            let
                folder ( currentRun, nextRun ) =
                    if currentRun.geometryHash /= nextRun.geometryHash then
                        Just <| convertDuration <| nextRun.runStart - firstRunStart

                    else
                        Nothing
            in
            List.filterMap identity (foldPairs r folder)

        runsWithStartAndDurations : List ( JsonDetectorShift, Float, Float )
        runsWithStartAndDurations =
            let
                allStartAndDurations =
                    List.foldl
                        (\rr oldStartAndDurations ->
                            ( rr, convertDuration (rr.runStart - firstRunStart), runDuration rr ) :: oldStartAndDurations
                        )
                        []
                        r
            in
            List.reverse allStartAndDurations

        plotLine : ContinuousScale Float -> (JsonDetectorShift -> Maybe Float) -> SubPath.SubPath
        plotLine scale accessor =
            List.filterMap
                (\( run, start, duration ) ->
                    accessor run
                        |> Maybe.map (\value -> ( Scale.convert runTimeScale start, Scale.convert runTimeScale (start + duration), Scale.convert scale value ))
                )
                runsWithStartAndDurations
                |> List.concatMap (\( start, end, amount ) -> [ ( start, amount ), ( end, amount ) ])
                |> Curve.linear

        xAxisLabel =
            text_
                [ y (h - bottomPadding / 2)
                , x (xPadding + (w - xPadding) / 2)
                , textAnchor AnchorMiddle
                , fontSize largeFontSize
                ]
                [ text "Run Duration"
                ]

        yRotationAxisLabel =
            text_
                [ textAnchor AnchorMiddle
                , dominantBaseline DominantBaselineMiddle
                , transform [ Rotate 270 (xPadding * 0.25) (h / 2.0), Translate (xPadding * 0.25) (h / 2.0) ]
                , fontSize largeFontSize
                ]
                [ text "Rotation [°]"
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

        xRotationAxisLegend =
            g []
                [ line [ x1 xPadding, x2 (xPadding + smallFontSize + 2), y1 (topPadding / 2.0), y2 (topPadding / 2.0), stroke (Paint xShiftColor), strokeWidth 2 ] []
                , text_
                    [ dominantBaseline DominantBaselineMiddle
                    , y (topPadding / 2.0)
                    , x (xPadding + smallFontSize + 6)
                    , fontSize smallFontSize
                    ]
                    [ text "X rotation" ]
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

        yRotationAxisLegend =
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
                    [ text "Y rotation" ]
                ]

        zAxisLegend =
            let
                xBasePoint =
                    200
            in
            g []
                [ line
                    [ x1 (xPadding + xBasePoint)
                    , x2 (xPadding + xBasePoint + smallFontSize + 2)
                    , y1 (topPadding / 2.0)
                    , y2 (topPadding / 2.0)
                    , stroke (Paint zShiftColor)
                    , strokeWidth 2
                    ]
                    []
                , text_
                    [ dominantBaseline DominantBaselineMiddle
                    , y (topPadding / 2.0)
                    , x (xPadding + xBasePoint + smallFontSize + 6)
                    , fontSize smallFontSize
                    ]
                    [ text "Z direction" ]
                ]

        geometryChangeLegend =
            let
                xBasePoint =
                    300
            in
            g []
                [ line
                    [ x1 (xPadding + xBasePoint)
                    , x2 (xPadding + xBasePoint + smallFontSize + 2)
                    , y1 (topPadding / 2.0)
                    , y2 (topPadding / 2.0)
                    , strokeDasharray "5,5"
                    , strokeWidth 1
                    , stroke (Paint Color.black)
                    , strokeWidth 2
                    ]
                    []
                , text_
                    [ dominantBaseline DominantBaselineMiddle
                    , y (topPadding / 2.0)
                    , x (xPadding + xBasePoint + smallFontSize + 6)
                    , fontSize smallFontSize
                    ]
                    [ text "geometry change" ]
                ]

        shiftPlot =
            svg [ viewBox 0 0 w h ]
                [ g [ transform [ Translate (xPadding - 1) (h - bottomPadding) ] ]
                    [ Axis.bottom [ Axis.tickCount 10 ] runTimeScale ]
                , g [ transform [ Translate (xPadding - 1) topPadding ] ]
                    [ Axis.left [] (shiftScale r)
                    ]
                , xAxisLabel
                , shiftAxisLabel
                , xAxisLegend
                , yAxisLegend
                , zAxisLegend
                , geometryChangeLegend
                , g [ transform [ Translate (xPadding - 1) 0 ] ]
                    [ SubPath.element
                        (SubPath.fromSegments
                            [ Segment.line
                                ( 0, topPadding + Scale.convert (shiftScale r) 0 )
                                ( Tuple.second (Scale.range runTimeScale), topPadding + Scale.convert (shiftScale r) 0 )
                            ]
                        )
                        [ stroke <| Paint <| Color.lightCharcoal
                        , fill PaintNone
                        ]
                    ]
                , g [ transform [ Translate xPadding topPadding ] ]
                    (List.map
                        (\geometryChangeTime ->
                            let
                                xValueOfVerticalLine =
                                    Scale.convert runTimeScale geometryChangeTime
                            in
                            SubPath.element
                                (SubPath.fromSegments
                                    [ Segment.line
                                        ( xValueOfVerticalLine, Tuple.first (Scale.range (shiftScale r)) )
                                        ( xValueOfVerticalLine, Tuple.second (Scale.range (shiftScale r)) )
                                    ]
                                )
                                [ stroke <| Paint <| Color.black
                                , strokeDasharray "5,5"
                                , strokeWidth 1
                                , fill PaintNone
                                ]
                        )
                        geometryChanges
                    )
                , g [ transform [ Translate xPadding topPadding ] ]
                    [ Path.element [ plotLine (shiftScale r) (Maybe.map .xTranslationMm << retrieveGroup "all") ]
                        [ stroke <| Paint <| xShiftColor
                        , strokeWidth 1
                        , fill PaintNone
                        ]
                    , Path.element [ plotLine (shiftScale r) (Maybe.map .yTranslationMm << retrieveGroup "all") ]
                        [ stroke <| Paint <| yShiftColor
                        , strokeWidth 1
                        , fill PaintNone
                        ]
                    , Path.element [ plotLine (shiftScale r) (Maybe.andThen .zTranslationMm << retrieveGroup "all") ]
                        [ stroke <| Paint <| zShiftColor
                        , strokeWidth 1
                        , fill PaintNone
                        ]
                    ]
                ]

        rotationPlot =
            svg [ viewBox 0 0 w h ]
                [ g [ transform [ Translate (xPadding - 1) (h - bottomPadding) ] ]
                    [ Axis.bottom [ Axis.tickCount 10 ] runTimeScale ]
                , g [ transform [ Translate (xPadding - 1) topPadding ] ]
                    [ Axis.left [] (shiftScale r)
                    ]
                , xAxisLabel
                , yRotationAxisLabel
                , xRotationAxisLegend
                , yRotationAxisLegend
                , geometryChangeLegend
                , g [ transform [ Translate (xPadding - 1) 0 ] ]
                    [ SubPath.element
                        (SubPath.fromSegments
                            [ Segment.line
                                ( 0, topPadding + Scale.convert (rotationScale r) 0 )
                                ( Tuple.second (Scale.range runTimeScale), topPadding + Scale.convert (rotationScale r) 0 )
                            ]
                        )
                        [ stroke <| Paint <| Color.lightCharcoal
                        , fill PaintNone
                        ]
                    ]
                , g [ transform [ Translate xPadding topPadding ] ]
                    (List.map
                        (\geometryChangeTime ->
                            let
                                xValueOfVerticalLine =
                                    Scale.convert runTimeScale geometryChangeTime
                            in
                            SubPath.element
                                (SubPath.fromSegments
                                    [ Segment.line
                                        ( xValueOfVerticalLine, Tuple.first (Scale.range (rotationScale r)) )
                                        ( xValueOfVerticalLine, Tuple.second (Scale.range (rotationScale r)) )
                                    ]
                                )
                                [ stroke <| Paint <| Color.black
                                , strokeDasharray "5,5"
                                , strokeWidth 1
                                , fill PaintNone
                                ]
                        )
                        geometryChanges
                    )
                , g [ transform [ Translate xPadding topPadding ] ]
                    [ Path.element [ plotLine (rotationScale r) (Maybe.andThen .xRotationDeg << retrieveGroup "all") ]
                        [ stroke <| Paint <| xShiftColor
                        , strokeWidth 1
                        , fill PaintNone
                        ]
                    , Path.element [ plotLine (rotationScale r) (Maybe.andThen .yRotationDeg << retrieveGroup "all") ]
                        [ stroke <| Paint <| yShiftColor
                        , strokeWidth 1
                        , fill PaintNone
                        ]
                    ]
                ]
    in
    div_
        [ shiftPlot
        , rotationPlot
        ]


viewShiftTable : List JsonDetectorShift -> Html msg
viewShiftTable shifts =
    let
        viewShift { runExternalId, alignDetectorGroups } =
            List.Extra.find (\{ group } -> group == "all") alignDetectorGroups
                |> Maybe.map
                    (\{ xTranslationMm, yTranslationMm, zTranslationMm, xRotationDeg, yRotationDeg } ->
                        tr_
                            [ td_ [ text (String.fromInt runExternalId) ]
                            , td_ [ text (formatFloatHumanFriendly xTranslationMm) ]
                            , td_ [ text (formatFloatHumanFriendly yTranslationMm) ]
                            , td_
                                [ Maybe.withDefault (text "") (Maybe.map (text << formatFloatHumanFriendly) zTranslationMm)
                                ]
                            , td_ [ Maybe.withDefault (text "") (Maybe.map (text << formatFloatHumanFriendly) xRotationDeg) ]
                            , td_ [ Maybe.withDefault (text "") (Maybe.map (text << formatFloatHumanFriendly) yRotationDeg) ]
                            ]
                    )
    in
    table [ class "table table-striped amarcord-table-fix-head" ]
        [ thead_
            [ tr_
                [ th_ [ text "Run ID" ]
                , th_ [ text "X translation" ]
                , th_ [ text "Y translation" ]
                , th_ [ text "Z translation" ]
                , th_ [ text "X rotation" ]
                , th_ [ text "Y rotation" ]
                ]
            ]
        , tbody_ (List.filterMap viewShift shifts)
        ]


viewInner : JsonReadBeamtimeGeometryDetails -> Model -> List (Html Msg)
viewInner { detectorShifts } model =
    [ h1_ [ text "Detector Shifts" ]
    , div [ class "form-text" ]
        [ text "Shifts are displayed only for online indexing results for now. The group \"all\" is used to show the total shift and rotation."
        ]
    , div [ class "mb-3" ]
        [ div [ class "form-check form-check-inline" ]
            [ input_
                [ id "detector-shift-mode-by-id"
                , type_ "radio"
                , name "detector-shift-mode"
                , class "form-check-input"
                , checked (model.detectorShiftMode == ByRunId)
                , onInput (always <| DetectorShiftModeChange ByRunId)
                ]
            , label [ for "detector-shift-mode-by-id" ] [ text "Display by ID" ]
            ]
        , div [ class "form-check form-check-inline" ]
            [ input_
                [ id "detector-shift-mode-by-time"
                , type_ "radio"
                , name "detector-shift-mode"
                , class "form-check-input"
                , checked (model.detectorShiftMode == ByTime)
                , onInput (always <| DetectorShiftModeChange ByTime)
                ]
            , label [ for "detector-shift-mode-by-time" ] [ text "Display by absolute time" ]
            ]
        ]
    , case model.detectorShiftMode of
        ByTime ->
            viewDetectorShiftsByAbsoluteTime detectorShifts

        ByRunId ->
            viewDetectorShiftsByRunId detectorShifts
    , viewShiftTable detectorShifts
    ]


view : Model -> Html Msg
view model =
    div [ class "container" ] <|
        case model.geometryDetailsRequest of
            NotAsked ->
                List.singleton <| text ""

            Loading ->
                List.singleton <| loadingBar "Loading analysis results..."

            Failure e ->
                List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve Attributi" ], showError e ]

            Success r ->
                viewInner r model


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        DetectorShiftModeChange newMode ->
            ( { model | detectorShiftMode = newMode }, Cmd.none )

        GeometryDetailsReceived analysisResults ->
            ( { model | geometryDetailsRequest = fromResult analysisResults }, Cmd.none )
