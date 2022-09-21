module Amarcord.Gauge exposing (gauge, thisFillColor, totalFillColor)

import Html exposing (text)
import Html.Attributes exposing (style)
import Svg exposing (g, line, path, polygon, rect, svg)
import Svg.Attributes exposing (d, dominantBaseline, height, points, rx, ry, textAnchor, viewBox, width, x, x1, x2, y, y1, y2)


thisFillColor : String
thisFillColor =
    "#f18f1f"


totalFillColor : String
totalFillColor =
    "#f1c490"


arcFillColor : String
arcFillColor =
    "#3183fd"


gauge : Maybe Float -> Maybe Float -> Html.Html msg
gauge this total =
    let
        w =
            250.0

        h =
            125.0

        cx =
            w / 2.0

        cy =
            h - 5.0

        offset =
            40.0

        r1 =
            cx - offset

        r2 delta =
            r1 - delta

        roundStr =
            String.fromInt << round

        str =
            String.fromFloat

        drawNeedle percent =
            let
                a =
                    180.0 + 180.0 * percent / 100

                nw =
                    4.0

                nx1 =
                    cx + nw * cos (degrees (a - 90))

                ny1 =
                    cy + nw * sin (degrees (a - 90))

                nx2 =
                    cx + (r1 + 15.0) * cos (degrees a)

                ny2 =
                    cy + (r1 + 15.0) * sin (degrees a)

                nx3 =
                    cx + nw * cos (degrees (a + 90))

                ny3 =
                    cy + nw * sin (degrees (a + 90))
            in
            str nx1 ++ "," ++ str ny1 ++ " " ++ str nx2 ++ "," ++ str ny2 ++ " " ++ str nx3 ++ "," ++ str ny3

        drawArc delta =
            let
                x1 =
                    cx + r1

                y1 =
                    cy

                x2 =
                    offset

                y2 =
                    cy

                x3 =
                    x1 - delta

                y3 =
                    cy
            in
            "M "
                ++ str x1
                ++ ", "
                ++ str y1
                ++ " A"
                ++ str r1
                ++ ","
                ++ str r1
                ++ " 0 0 0 "
                ++ str x2
                ++ ","
                ++ str y2
                ++ " H"
                ++ (str <| offset + delta)
                ++ " A"
                ++ str (r2 delta)
                ++ ","
                ++ str (r2 delta)
                ++ " 0 0 1 "
                ++ str x3
                ++ ","
                ++ str y3
                ++ " z"

        drawScale delta =
            let
                sr1 =
                    r1 + 2.0

                sr2 =
                    r2 delta + 7.0

                srT =
                    r1 + 15.0

                makeTick idx =
                    let
                        angle =
                            -180.0 + toFloat idx * 18.0

                        sx1 =
                            cx + sr1 * cos (degrees angle)

                        sy1 =
                            cy + sr1 * sin (degrees angle)

                        sx2 =
                            cx + sr2 * cos (degrees angle)

                        sy2 =
                            cy + sr2 * sin (degrees angle)

                        sxT =
                            cx + srT * cos (degrees angle)

                        syT =
                            cy + srT * sin (degrees angle)
                    in
                    [ line
                        [ x1 (str sx1)
                        , x2 (str sx2)
                        , y1 (str sy1)
                        , y2 (str sy2)
                        ]
                        []
                    , Svg.text_
                        [ x (str sxT)
                        , y (str syT)
                        , textAnchor "middle"
                        , dominantBaseline "alphabetic"
                        , style "stroke" "none"
                        , style "font-size" "0.75rem"
                        ]
                        [ text (String.fromInt <| idx * 10) ]
                    ]

                ticks =
                    List.concatMap makeTick <| List.range 0 10
            in
            g [ style "stroke" "black", style "color" "#212529" ] ticks
    in
    svg
        [ viewBox ("0 0 " ++ roundStr w ++ " " ++ roundStr h)
        , width (roundStr w)
        , height (roundStr h)
        ]
        [ drawScale 20.0
        , path
            [ d (drawArc 10.0)
            , style "fill" arcFillColor
            ]
            []
        , case this of
            Nothing ->
                text ""

            Just thisReal ->
                polygon [ style "fill" thisFillColor, style "stroke" "#686868", points (drawNeedle thisReal) ] []
        , case total of
            Nothing ->
                text ""

            Just totalReal ->
                polygon [ style "fill" totalFillColor, style "stroke" "#686868", points (drawNeedle totalReal) ] []
        , rect
            [ x (str <| cx - 10)
            , y (str <| cy - 10)
            , width (str <| 20)
            , height (str <| 20)
            , rx "2.5"
            , ry "2.5"
            , style "fill" "#909090"
            , style "stroke" "black"
            ]
            []
        ]
