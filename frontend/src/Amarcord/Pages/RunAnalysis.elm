module Amarcord.Pages.RunAnalysis exposing (Model, Msg(..), init, update, view)

import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue, convertAttributoFromApi, convertAttributoMapFromApi)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert)
import Amarcord.Chemical exposing (Chemical, ChemicalId, chemicalIdDict, convertChemicalFromApi)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Html exposing (div_, h1_, h5_, input_)
import Amarcord.HttpError as HttpError
import Amarcord.RunStatistics exposing (viewHitRateAndIndexingGraphs)
import Amarcord.Util exposing (HereAndNow)
import Api.Data exposing (JsonAnalysisRun, JsonDetectorShift, JsonFileOutput, JsonIndexingStatistic, JsonReadBeamtimeGeometryDetails, JsonReadRunAnalysis, JsonRunAnalysisIndexingResult)
import Api.Request.Analysis exposing (readBeamtimeGeometryDetailsApiRunAnalysisBeamtimeIdGeometryGet, readRunAnalysisApiRunAnalysisBeamtimeIdGet)
import Axis
import Color
import Html exposing (Html, br, button, div, h4, span, table, tbody, td, text, th, thead, tr)
import Html.Attributes exposing (class, style, type_, value)
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


type Msg
    = GeometryDetailsReceived (Result HttpError.HttpError JsonReadBeamtimeGeometryDetails)
    | RunAnalysisResultsReceived (Result HttpError.HttpError JsonReadRunAnalysis)
    | ChangeRunId (Int -> Int)
    | ChangeRunIdText String
    | ConfirmRunIdText


type alias Model =
    { hereAndNow : HereAndNow
    , runIdInput : String
    , runAnalysisRequest : RemoteData HttpError.HttpError JsonReadRunAnalysis
    , geometryDetailsRequest : RemoteData HttpError.HttpError JsonReadBeamtimeGeometryDetails
    , beamtimeId : BeamtimeId
    }


init : HereAndNow -> BeamtimeId -> ( Model, Cmd Msg )
init hereAndNow beamtimeId =
    ( { hereAndNow = hereAndNow
      , runIdInput = ""
      , geometryDetailsRequest = Loading
      , runAnalysisRequest = Loading
      , beamtimeId = beamtimeId
      }
    , Cmd.batch
        [ HttpError.send GeometryDetailsReceived (readBeamtimeGeometryDetailsApiRunAnalysisBeamtimeIdGeometryGet beamtimeId)
        , HttpError.send RunAnalysisResultsReceived (readRunAnalysisApiRunAnalysisBeamtimeIdGet beamtimeId Nothing)
        ]
      -- ,
    )


xShiftColor : Color.Color
xShiftColor =
    Maybe.withDefault Color.red <| head Scale.Color.colorblind


yShiftColor : Color.Color
yShiftColor =
    Maybe.withDefault Color.red <| ListExtra.last Scale.Color.colorblind


viewDetectorShifts : List JsonDetectorShift -> Html msg
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
            [ Path.element (plotLine shiftScale .shiftXMm)
                [ stroke <| Paint <| xShiftColor
                , strokeWidth 2
                , fill PaintNone
                ]
            , Path.element (plotLine shiftScale .shiftYMm)
                [ stroke <| Paint <| yShiftColor
                , strokeWidth 2
                , fill PaintNone
                ]
            ]
        ]


viewRunStatistics : List JsonIndexingStatistic -> Html msg
viewRunStatistics originalStats =
    viewHitRateAndIndexingGraphs originalStats


viewRunTableRow :
    HereAndNow
    -> List (Attributo AttributoType)
    -> List (Chemical ChemicalId (AttributoMap AttributoValue) JsonFileOutput)
    -> JsonAnalysisRun
    -> JsonRunAnalysisIndexingResult
    -> Html msg
viewRunTableRow hereAndNow attributi chemicals run rar =
    let
        viewRun r =
            viewDataSetTable attributi hereAndNow.zone (chemicalIdDict chemicals) (convertAttributoMapFromApi r.attributi) False False Nothing
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
    HereAndNow
    -> String
    -> List (Attributo AttributoType)
    -> List (Chemical ChemicalId (AttributoMap AttributoValue) JsonFileOutput)
    -> Maybe JsonAnalysisRun
    -> List JsonRunAnalysisIndexingResult
    -> Html Msg
viewRunGraphs hereAndNow runIdInput attributi chemicals run rars =
    div_
        [ div [ class "hstack gap-3" ]
            [ button [ class "btn btn-outline-secondary", onClick (ChangeRunId (\r -> r - 1)) ] [ icon { name = "arrow-left" } ]
            , span [ class "form-text text-nowrap" ] [ text "Run ID" ]
            , input_ [ type_ "text", value runIdInput, class "form-control", onInput ChangeRunIdText, onEnter ConfirmRunIdText ]
            , button [ class "btn btn-outline-secondary", onClick (ChangeRunId (\r -> r + 1)) ] [ icon { name = "arrow-right" } ]
            ]
        , case run of
            Nothing ->
                text ""

            Just runReal ->
                table [ class "table table-striped" ]
                    [ thead [] [ tr [] [ th [] [ text "Run Information" ], th [ style "width" "100%" ] [ text "Statistics" ] ] ]
                    , tbody [] (List.map (viewRunTableRow hereAndNow attributi chemicals runReal) rars)
                    ]
        ]


viewInner : HereAndNow -> JsonReadBeamtimeGeometryDetails -> Model -> List (Html Msg)
viewInner hereAndNow { detectorShifts } model =
    [ h1_ [ text "Detector Shifts" ]
    , viewDetectorShifts detectorShifts
    , h1_ [ text "Indexing Details" ]
    , case model.runAnalysisRequest of
        NotAsked ->
            text ""

        Loading ->
            loadingBar "Loading analysis results..."

        Failure e ->
            makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve run analysis" ], HttpError.showError e ]

        Success { attributi, chemicals, run, indexingResults } ->
            viewRunGraphs
                hereAndNow
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
                List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve Attributi" ], HttpError.showError e ]

            Success r ->
                viewInner model.hereAndNow r model


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
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
                                    ( { model | runIdInput = String.fromInt newRunId }, HttpError.send RunAnalysisResultsReceived (readRunAnalysisApiRunAnalysisBeamtimeIdGet model.beamtimeId (Just internalRunId)) )

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
                                    ( model, HttpError.send RunAnalysisResultsReceived (readRunAnalysisApiRunAnalysisBeamtimeIdGet model.beamtimeId (Just internalRunId)) )

                _ ->
                    ( model, Cmd.none )
