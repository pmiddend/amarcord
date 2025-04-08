module Amarcord.CellDescriptionViewer exposing (CellDescriptionViewerErrors(..), CellDescriptionViewerMultiline(..), view, viewColor)

import Amarcord.Crystallography exposing (bravaisLatticeToStringNoUnknownAxis, parseCellDescription)
import Amarcord.Html exposing (br_, div_, span_)
import Html exposing (Html, div, small, span, text)
import Html.Attributes exposing (class)
import Html.Extra exposing (nothing)


type CellDescriptionViewerMultiline
    = SingleLine
    | MultiLine


type CellDescriptionViewerErrors
    = WithErrors
    | NoErrors


view : CellDescriptionViewerMultiline -> CellDescriptionViewerErrors -> String -> Html msg
view =
    viewColor "secondary"


viewColor : String -> CellDescriptionViewerMultiline -> CellDescriptionViewerErrors -> String -> Html msg
viewColor badgeColor lines errors cd =
    if String.trim cd == "" then
        nothing

    else
        case parseCellDescription cd of
            Err e ->
                if errors == WithErrors then
                    if lines == MultiLine then
                        div_
                            [ span_ [ text cd ]
                            , br_
                            , small [ class "text-danger" ] [ text e ]
                            ]

                    else
                        span_ [ text (cd ++ " ("), span [ class "text-danger" ] [ text e ], text ")" ]

                else
                    span [ class "text-danger" ] [ text cd ]

            Ok { bravaisLattice, cellA, cellB, cellC, cellAlpha, cellBeta, cellGamma } ->
                div [ class "d-inline-flex gap-1" ]
                    [ span [ class ("badge text-bg-" ++ badgeColor) ] [ text (bravaisLatticeToStringNoUnknownAxis bravaisLattice) ]
                    , span [ class ("badge text-bg-" ++ badgeColor ++ " text-nowrap") ]
                        [ text (String.fromFloat cellA ++ " " ++ String.fromFloat cellB ++ " " ++ String.fromFloat cellC ++ " Å")
                        ]
                    , span [ class ("badge text-bg-" ++ badgeColor ++ " text-nowrap") ]
                        [ text (String.fromFloat cellAlpha ++ " " ++ String.fromFloat cellBeta ++ " " ++ String.fromFloat cellGamma ++ " °")
                        ]
                    ]
