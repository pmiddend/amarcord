module Amarcord.DataSetHtml exposing (..)

import Amarcord.Attributo exposing (Attributo, AttributoId, AttributoMap, AttributoType, AttributoValue, attributoMapIds)
import Amarcord.AttributoHtml exposing (viewAttributoCell)
import Amarcord.Html exposing (tbody_, th_, thead_, tr_)
import Dict
import Html exposing (Html, table, td, text, tr)
import Html.Attributes exposing (class, colspan, style)
import List.Extra exposing (find)
import Time exposing (Zone)


viewDataSetTable : List (Attributo AttributoType) -> Zone -> Dict.Dict Int String -> AttributoMap AttributoValue -> Bool -> Bool -> Maybe (Html msg) -> Html msg
viewDataSetTable attributi zone chemicalIdToName attributoMap withHeader withTolerance footer =
    let
        viewAttributiValueRow : Int -> AttributoMap AttributoValue -> Int -> AttributoId -> Html msg
        viewAttributiValueRow maxIdx attributoValues idx attributoId =
            case find (\a -> a.id == attributoId) attributi of
                Nothing ->
                    tr_ [ td [ colspan 2 ] [ text ("Attributo " ++ String.fromInt attributoId ++ " not found") ] ]

                Just attributo ->
                    tr
                        [ style "border-bottom"
                            (if idx == maxIdx then
                                "1px solid white"

                             else
                                ""
                            )
                        ]
                        [ td [ style "width" "50%" ] [ text attributo.name ]
                        , td [ style "width" "50%" ]
                            [ viewAttributoCell
                                { shortDateTime = False
                                , colorize = False
                                , withUnit = True
                                , withTolerance = withTolerance
                                }
                                zone
                                chemicalIdToName
                                attributoValues
                                attributo
                            ]
                        ]
    in
    table
        [ class "table table-sm", style "margin-bottom" "0" ]
        [ if withHeader then
            thead_ [ tr_ [ th_ [ text "Attributo" ], th_ [ text "Value" ] ] ]

          else
            text ""
        , tbody_
            (List.indexedMap
                (viewAttributiValueRow
                    (Dict.size attributoMap - 1)
                    attributoMap
                )
                (attributoMapIds attributoMap)
            )
        , case footer of
            Nothing ->
                text ""

            Just footerHtml ->
                footerHtml
        ]
