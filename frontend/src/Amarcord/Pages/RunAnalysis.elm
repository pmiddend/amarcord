module Amarcord.Pages.RunAnalysis exposing (Model, Msg(..), init, pageTitle, update, view)

import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue, convertAttributoFromApi, convertAttributoMapFromApi)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert)
import Amarcord.Chemical exposing (Chemical, ChemicalId, chemicalIdDict, convertChemicalFromApi)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Html exposing (div_, h1_, h5_, input_, p_, tbody_, td_, th_, thead_, tr_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.RunStatistics exposing (viewHitRateAndIndexingGraphs)
import Amarcord.Util exposing (foldPairs)
import Api.Data exposing (JsonAnalysisRun, JsonDetectorShift, JsonFileOutput, JsonIndexingStatistic, JsonReadBeamtimeGeometryDetails, JsonReadRunAnalysis, JsonRunAnalysisIndexingResult, JsonRunFile)
import Api.Request.Analysis exposing (readBeamtimeGeometryDetailsApiRunAnalysisBeamtimeIdGeometryGet, readRunAnalysisApiRunAnalysisBeamtimeIdGet)
import Axis
import Color
import Curve
import Html exposing (Html, br, button, div, h4, label, span, table, tbody, td, text, th, thead, tr)
import Html.Attributes exposing (checked, class, for, id, name, style, type_, value)
import Html.Events exposing (onClick, onInput)
import Html.Events.Extra exposing (onEnter)
import List exposing (head)
import List.Extra as ListExtra
import Maybe
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


type DetectorShiftMode
    = ByTime
    | ByRunId


type Msg
    = GeometryDetailsReceived (Result HttpError JsonReadBeamtimeGeometryDetails)
    | RunAnalysisResultsReceived (Result HttpError JsonReadRunAnalysis)
    | ChangeRunId (Int -> Int)
    | ChangeRunIdText String
    | ConfirmRunIdText
    | DetectorShiftModeChange DetectorShiftMode


type alias Model =
    { runIdInput : String
    , runAnalysisRequest : RemoteData HttpError JsonReadRunAnalysis
    , geometryDetailsRequest : RemoteData HttpError JsonReadBeamtimeGeometryDetails
    , beamtimeId : BeamtimeId
    , detectorShiftMode : DetectorShiftMode
    }


pageTitle : Model -> String
pageTitle _ =
    "Run Analysis"


init : BeamtimeId -> ( Model, Cmd Msg )
init beamtimeId =
    ( { runIdInput = ""
      , geometryDetailsRequest = Loading
      , runAnalysisRequest = Loading
      , beamtimeId = beamtimeId
      , detectorShiftMode = ByRunId
      }
    , Cmd.batch
        [ send GeometryDetailsReceived (readBeamtimeGeometryDetailsApiRunAnalysisBeamtimeIdGeometryGet beamtimeId)
        , send RunAnalysisResultsReceived (readRunAnalysisApiRunAnalysisBeamtimeIdGet beamtimeId Nothing)
        ]
      -- ,
    )


xShiftColor : Color.Color
xShiftColor =
    Maybe.withDefault Color.red <| head Scale.Color.colorblind


yShiftColor : Color.Color
yShiftColor =
    Maybe.withDefault Color.red <| ListExtra.last Scale.Color.colorblind


detectorShiftWidth : Float
detectorShiftWidth =
    800


detectorShiftHeight : Float
detectorShiftHeight =
    350


viewDetectorShiftsByRunId : List JsonDetectorShift -> Html msg
viewDetectorShiftsByRunId r =
    let
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

        largeFontSize =
            16

        smallFontSize =
            14

        yPadding : Float
        yPadding =
            topPadding + bottomPadding

        geometryChanges : List Int
        geometryChanges =
            let
                folder ( currentRun, nextRun ) =
                    if currentRun.geometryHash /= nextRun.geometryHash then
                        Just <| nextRun.runExternalId

                    else
                        Nothing
            in
            List.filterMap identity (foldPairs r folder)

        runIdScale : ContinuousScale Float
        runIdScale =
            r
                |> List.map (\rr -> toFloat rr.runExternalId)
                |> Statistics.extent
                |> Maybe.withDefault ( 1, 2 )
                |> Scale.linear ( 0.0, w - 2 * xPadding )

        shiftScale : ContinuousScale Float
        shiftScale =
            r
                |> List.concatMap (\fom -> [ fom.shiftXMm, fom.shiftYMm ])
                |> Statistics.extent
                |> Maybe.withDefault ( 0, 0 )
                |> Scale.linear ( h - yPadding, 0 )
                |> Scale.nice 4

        runIdLineGenerator : ContinuousScale Float -> ( Int, Float ) -> Maybe ( Float, Float )
        runIdLineGenerator scale ( runId, amount ) =
            Just ( Scale.convert runIdScale (toFloat runId), Scale.convert scale amount )

        plotLine : ContinuousScale Float -> (JsonDetectorShift -> Float) -> Path
        plotLine scale accessor =
            List.map (\rr -> ( rr.runExternalId, accessor rr )) r
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

        geometryChangeLegend =
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
        , geometryChangeLegend
        , g [ transform [ Translate (xPadding - 1) 0 ] ]
            [ SubPath.element
                (SubPath.fromSegments
                    [ Segment.line
                        ( 0, topPadding + Scale.convert shiftScale 0 )
                        ( Tuple.second (Scale.range runIdScale), topPadding + Scale.convert shiftScale 0 )
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
                            Scale.convert runIdScale (toFloat geometryChangeRunId)
                    in
                    SubPath.element
                        (SubPath.fromSegments
                            [ Segment.line
                                ( xValueOfVerticalLine, Tuple.first (Scale.range shiftScale) )
                                ( xValueOfVerticalLine, Tuple.second (Scale.range shiftScale) )
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
            [ Path.element (plotLine shiftScale .shiftXMm)
                [ stroke <| Paint <| xShiftColor
                , strokeWidth 1
                , fill PaintNone
                ]
            , Path.element (plotLine shiftScale .shiftYMm)
                [ stroke <| Paint <| yShiftColor
                , strokeWidth 1
                , fill PaintNone
                ]
            ]
        ]


viewDetectorShiftsByAbsoluteTime : List JsonDetectorShift -> Html msg
viewDetectorShiftsByAbsoluteTime r =
    let
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

        largeFontSize =
            16

        smallFontSize =
            14

        yPadding : Float
        yPadding =
            topPadding + bottomPadding

        convertDuration x =
            toFloat (x // 1000 // 60)

        runDuration rr =
            convertDuration <| (Maybe.withDefault rr.runStart rr.runEnd - rr.runStart)

        firstTime =
            List.head r |> Maybe.map .runStart |> Maybe.withDefault 0

        runDurationSpan : Float
        runDurationSpan =
            case ListExtra.last r of
                Nothing ->
                    0.0

                Just lastRun ->
                    convertDuration <| Maybe.withDefault 0 lastRun.runEnd - firstTime

        runTimeScale : ContinuousScale Float
        runTimeScale =
            Scale.linear ( 0.0, w - 2 * xPadding ) ( 0, runDurationSpan )

        geometryChanges : List Float
        geometryChanges =
            let
                folder ( currentRun, nextRun ) =
                    if currentRun.geometryHash /= nextRun.geometryHash then
                        Just <| convertDuration <| nextRun.runStart - firstTime

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
                            ( rr, convertDuration (rr.runStart - firstTime), runDuration rr ) :: oldStartAndDurations
                        )
                        []
                        r
            in
            List.reverse allStartAndDurations

        shiftScale : ContinuousScale Float
        shiftScale =
            r
                |> List.concatMap (\fom -> [ fom.shiftXMm, fom.shiftYMm ])
                |> Statistics.extent
                |> Maybe.withDefault ( 0, 0 )
                |> Scale.linear ( h - yPadding, 0 )
                |> Scale.nice 4

        plotLine : ContinuousScale Float -> (JsonDetectorShift -> Float) -> SubPath.SubPath
        plotLine scale accessor =
            List.map
                (\( run, start, duration ) -> ( Scale.convert runTimeScale start, Scale.convert runTimeScale (start + duration), Scale.convert scale (accessor run) ))
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

        yAxisLabel =
            text_
                [ textAnchor AnchorMiddle
                , dominantBaseline DominantBaselineMiddle
                , transform [ Rotate 270 (xPadding * 0.25) (h / 2.0), Translate (xPadding * 0.25) (h / 2.0) ]
                , fontSize largeFontSize
                ]
                [ text "Shift [mm]"
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

        geometryChangeLegend =
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
    in
    svg [ viewBox 0 0 w h ]
        [ g [ transform [ Translate (xPadding - 1) (h - bottomPadding) ] ]
            [ Axis.bottom [ Axis.tickCount 10 ] runTimeScale ]
        , g [ transform [ Translate (xPadding - 1) topPadding ] ]
            [ Axis.left [] shiftScale
            ]
        , xAxisLabel
        , yAxisLabel
        , xAxisLegend
        , yAxisLegend
        , geometryChangeLegend
        , g [ transform [ Translate (xPadding - 1) 0 ] ]
            [ SubPath.element
                (SubPath.fromSegments
                    [ Segment.line
                        ( 0, topPadding + Scale.convert shiftScale 0 )
                        ( Tuple.second (Scale.range runTimeScale), topPadding + Scale.convert shiftScale 0 )
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
                                ( xValueOfVerticalLine, Tuple.first (Scale.range shiftScale) )
                                ( xValueOfVerticalLine, Tuple.second (Scale.range shiftScale) )
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
            [ Path.element [ plotLine shiftScale .shiftXMm ]
                [ stroke <| Paint <| xShiftColor
                , strokeWidth 1
                , fill PaintNone
                ]
            , Path.element [ plotLine shiftScale .shiftYMm ]
                [ stroke <| Paint <| yShiftColor
                , strokeWidth 1
                , fill PaintNone
                ]
            ]
        ]


viewRunStatistics : List JsonIndexingStatistic -> Html msg
viewRunStatistics originalStats =
    viewHitRateAndIndexingGraphs originalStats


viewRunFiles : List JsonRunFile -> Html msg
viewRunFiles files =
    case files of
        [] ->
            text ""

        _ ->
            div_
                [ h5_ [ text "Files" ]
                , table [ class "table table-sm table-striped" ]
                    [ thead_ [ tr_ [ th_ [ text "Source" ], th_ [ text "Glob" ] ] ]
                    , tbody_ (List.map (\{ glob, source } -> tr_ [ td_ [ text source ], td_ [ text glob ] ]) files)
                    ]
                ]


viewRunTableRow :
    List (Attributo AttributoType)
    -> List (Chemical ChemicalId (AttributoMap AttributoValue) JsonFileOutput)
    -> JsonAnalysisRun
    -> JsonRunAnalysisIndexingResult
    -> Html msg
viewRunTableRow attributi chemicals run rar =
    let
        viewRun r =
            div_
                [ viewDataSetTable
                    attributi
                    (chemicalIdDict chemicals)
                    (convertAttributoMapFromApi r.attributi)
                    False
                    False
                    Nothing
                , viewRunFiles run.filePaths
                ]
    in
    tr []
        [ td []
            [ h5_ [ text <| "Run " ++ String.fromInt rar.runId ]
            , br [] []
            , viewRun run
            ]
        , td []
            [ viewRunStatistics rar.indexingStatistics
            ]
        ]


viewRunGraphs :
    String
    -> List (Attributo AttributoType)
    -> List (Chemical ChemicalId (AttributoMap AttributoValue) JsonFileOutput)
    -> Maybe JsonAnalysisRun
    -> List JsonRunAnalysisIndexingResult
    -> Html Msg
viewRunGraphs runIdInput attributi chemicals run rars =
    div_
        [ div [ class "hstack gap-3" ]
            [ button [ class "btn btn-outline-secondary", onClick (ChangeRunId (\r -> r - 1)) ] [ icon { name = "arrow-left" } ]
            , span [ class "form-text text-nowrap" ] [ text "Run ID" ]
            , input_
                [ type_ "text"
                , value runIdInput
                , class "form-control"
                , onInput ChangeRunIdText
                , onEnter ConfirmRunIdText
                ]
            , button [ class "btn btn-outline-secondary", onClick (ChangeRunId (\r -> r + 1)) ] [ icon { name = "arrow-right" } ]
            ]
        , case run of
            Nothing ->
                text ""

            Just runReal ->
                table [ class "table table-striped" ]
                    [ thead []
                        [ tr []
                            [ th [] [ text "Run Information" ]
                            , th [ style "width" "100%" ] [ text "Statistics" ]
                            ]
                        ]
                    , tbody [] (List.map (viewRunTableRow attributi chemicals runReal) rars)
                    ]
        ]


viewInner : JsonReadBeamtimeGeometryDetails -> Model -> List (Html Msg)
viewInner { detectorShifts } model =
    [ h1_ [ text "Detector Shifts" ]
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
    , h1_ [ text "Indexing Details" ]
    , p_ [ text "Enter a run ID to see details. Press Return to update." ]
    , case model.runAnalysisRequest of
        NotAsked ->
            text ""

        Loading ->
            loadingBar "Loading analysis results..."

        Failure e ->
            makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve run analysis" ], showError e ]

        Success { attributi, chemicals, run, indexingResults } ->
            viewRunGraphs
                model.runIdInput
                (List.map convertAttributoFromApi attributi)
                (List.map convertChemicalFromApi chemicals)
                run
                indexingResults
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

        RunAnalysisResultsReceived analysisResults ->
            ( { model | runAnalysisRequest = fromResult analysisResults }, Cmd.none )

        ChangeRunId changer ->
            case model.runAnalysisRequest of
                Success { runIds } ->
                    case String.toInt model.runIdInput of
                        Nothing ->
                            ( model, Cmd.none )

                        Just realRunId ->
                            let
                                newRunId =
                                    changer realRunId
                            in
                            case ListExtra.find (\runIdIntExt -> runIdIntExt.externalRunId == newRunId) runIds of
                                Just { internalRunId } ->
                                    ( { model | runIdInput = String.fromInt newRunId }, send RunAnalysisResultsReceived (readRunAnalysisApiRunAnalysisBeamtimeIdGet model.beamtimeId (Just internalRunId)) )

                                Nothing ->
                                    ( model, Cmd.none )

                _ ->
                    ( model, Cmd.none )

        ChangeRunIdText newText ->
            ( { model | runIdInput = newText }, Cmd.none )

        ConfirmRunIdText ->
            case model.runAnalysisRequest of
                Success { runIds } ->
                    case String.toInt model.runIdInput of
                        Nothing ->
                            ( model, Cmd.none )

                        Just realRunId ->
                            case ListExtra.find (\runIdIntExt -> runIdIntExt.externalRunId == realRunId) runIds of
                                Nothing ->
                                    ( model, Cmd.none )

                                Just { internalRunId } ->
                                    ( model, send RunAnalysisResultsReceived (readRunAnalysisApiRunAnalysisBeamtimeIdGet model.beamtimeId (Just internalRunId)) )

                _ ->
                    ( model, Cmd.none )
