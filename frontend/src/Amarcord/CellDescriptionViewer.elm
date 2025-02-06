module Amarcord.CellDescriptionViewer exposing (view)

import Amarcord.Crystallography exposing (bravaisLatticeToStringNoUnknownAxis, parseCellDescription)
import Amarcord.Html exposing (br_, div_, span_)
import Html exposing (Html, div, small, span, text)
import Html.Attributes exposing (class)


view : String -> Html msg
view cd =
    case parseCellDescription cd of
        Err e ->
            div_
                [ span_ [ text cd ]
                , br_
                , small [ class "text-danger" ] [ text e ]
                ]

        Ok { bravaisLattice, cellA, cellB, cellC, cellAlpha, cellBeta, cellGamma } ->
            let
                badgeColor =
                    "dark"
            in
            div [ class "hstack gap-1" ]
                [ span [ class ("badge text-bg-" ++ badgeColor) ] [ text (bravaisLatticeToStringNoUnknownAxis bravaisLattice) ]
                , span [ class ("badge text-bg-" ++ badgeColor ++ " text-nowrap") ]
                    [ text (String.fromFloat cellA ++ " " ++ String.fromFloat cellB ++ " " ++ String.fromFloat cellC ++ " Å")
                    ]
                , span [ class ("badge text-bg-" ++ badgeColor ++ " text-nowrap") ]
                    [ text (String.fromFloat cellAlpha ++ " " ++ String.fromFloat cellBeta ++ " " ++ String.fromFloat cellGamma ++ " °")
                    ]
                ]
