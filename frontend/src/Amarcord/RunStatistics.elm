module Amarcord.RunStatistics exposing (..)

import Amarcord.Bootstrap exposing (viewCloseHelpButton, viewHelpButton)
import Amarcord.Html exposing (h5_, p_)
import Amarcord.Util exposing (foldPairs)
import Api.Data exposing (JsonIndexingStatistic)
import Chart as C
import Chart.Attributes as CA
import Html exposing (Html, div, em, img, text)
import Html.Attributes exposing (class, id, src)
import Time exposing (utc)


viewHitRateAndIndexingGraphs : List JsonIndexingStatistic -> Html msg
viewHitRateAndIndexingGraphs stats =
    let
        graphMarginMagnitude =
            30

        graphMargins =
            { top = graphMarginMagnitude, bottom = graphMarginMagnitude, left = graphMarginMagnitude, right = graphMarginMagnitude }

        fontSize =
            10

        makeRate accessor =
            foldPairs
                stats
                (\( prior, next ) ->
                    { time = next.time
                    , rate =
                        if next.frames == prior.frames then
                            0.0

                        else
                            (toFloat <| accessor next - accessor prior) / (toFloat <| next.frames - prior.frames) * 100.0
                    }
                )
    in
    div []
        [ h5_
            [ text "Hit Rate [%]"
            , viewHelpButton "help-hit-rate"
            ]
        , div
            [ class "collapse text-bg-light p-2"
            , id "help-hit-rate"
            ]
            [ p_ [ text "The “hit rate” is defined as the fraction of total frames that are considered “hits”:  What consitutes a “hit” depends on a lot of configuration parameters, but generally is a frame that has enough “peaks” (bright spots that might be Bragg peaks). " ]
            , p_ [ text "The following example with 100,000 images illustrates the CrystFEL data flow:" ]
            , img [ src "hits-and-indexing.svg", class "img-fluid mb-2 mt-2" ] []
            , p_ [ text "Here, a run was taken with 100,000 frames total. Of that, only 10,000 frames are considered “hits”. Of those 10,000 frames, 3,000 are considered “indexable”. Thus, we get a hit rate of 10%, and an indexing rate of 30% (because we take the hits as a reference, not the total number of frames)." ]
            , p_ [ text "Since only indexed frames are useful for merging (and thus for the electron density), you can see that not every hit is useful in the end. Consequently, when you are acquiring data, the ", em [] [ text "number of indexed frames" ], text " is the more important quantity to shoot for." ]
            , viewCloseHelpButton "help-hit-rate"
            ]
        , C.chart
            [ CA.height 200, CA.width 400, CA.margin graphMargins ]
            [ C.xAxis []
            , C.yAxis []
            , C.xLabels [ CA.fontSize fontSize, CA.times utc ]
            , C.yLabels [ CA.withGrid, CA.fontSize fontSize ]
            , C.series (toFloat << .time)
                [ C.interpolated .rate [] [] ]
                (makeRate .hits)
            ]
        , h5_ [ text "Indexing Rate [%]" ]
        , C.chart
            [ CA.height 200, CA.width 400, CA.margin graphMargins ]
            [ C.xAxis []
            , C.yAxis []
            , C.xLabels [ CA.fontSize fontSize, CA.times utc ]
            , C.yLabels [ CA.withGrid, CA.fontSize fontSize ]
            , C.series (toFloat << .time)
                [ C.interpolated .rate [ CA.color CA.red ] [] ]
                (makeRate .indexed)
            ]
        ]
