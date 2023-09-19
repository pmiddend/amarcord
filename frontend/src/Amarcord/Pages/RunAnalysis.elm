module Amarcord.Pages.RunAnalysis exposing (Model, Msg(..), init, update, view)

import Amarcord.API.DataSet exposing (DataSetSummary)
import Amarcord.API.Requests exposing (RequestError, RunAnalysisResult, RunAnalysisResultsRoot, httpGetRunAnalysisResults)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Bootstrap exposing (AlertProperty(..), loadingBar, makeAlert)
import Amarcord.Html exposing (h1_)
import Amarcord.Util exposing (HereAndNow)
import Axis
import Color
import Html exposing (Html, div, h4, text)
import Html.Attributes exposing (class)
import Maybe
import Path exposing (Path)
import RemoteData exposing (RemoteData(..), fromResult)
import Scale exposing (ContinuousScale)
import Shape
import Statistics
import Tuple exposing (second)
import TypedSvg exposing (g, svg)
import TypedSvg.Attributes exposing (fill, stroke, transform, viewBox)
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


minMax : List comparable -> Maybe ( comparable, comparable )
minMax l =
    Maybe.map2 (\mymin mymax -> ( mymin, mymax )) (List.minimum l) (List.maximum l)


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
                |> minMax
                |> Maybe.withDefault ( 0, 0 )
                |> Scale.linear ( h - 2 * padding, 0 )
                |> Scale.nice 4

        yShiftScale : ContinuousScale Float
        yShiftScale =
            r
                |> List.concatMap (\rr -> rr.foms)
                |> List.filterMap (\fom -> fom.detectorShiftY)
                |> minMax
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
        , g [ transform [ Translate (padding + second (Scale.range runIdScale)) padding ] ]
            [ Axis.right [] yShiftScale

            -- , text_ [ fontFamily [ "sans-serif" ], fontSize 10, x 5, y 5 ] [ text "Y shift amount" ]
            ]
        , g [ transform [ Translate padding padding ] ]
            [ Path.element (line xShiftScale .detectorShiftX)
                [ stroke <| Paint <| Color.red
                , strokeWidth 3
                , fill PaintNone
                ]
            , Path.element (line yShiftScale .detectorShiftY)
                [ stroke <| Paint <| Color.green
                , strokeWidth 3
                , fill PaintNone
                ]
            ]
        ]


viewInner : HereAndNow -> RunAnalysisResultsRoot -> List (Html Msg)
viewInner _ { indexingResultsByRunId } =
    [ h1_ [ text "Detector Shifts" ]
    , viewDetectorShifts indexingResultsByRunId
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
