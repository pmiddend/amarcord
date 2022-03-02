module Amarcord.DataSetHtml exposing (..)

import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue, attributoMapNames)
import Amarcord.AttributoHtml exposing (viewAttributoCell)
import Amarcord.DataSet exposing (DataSet)
import Amarcord.Html exposing (tbody_, td_, th_, thead_, tr_)
import Dict
import Html exposing (Html, table, text)
import Html.Attributes exposing (class)
import List.Extra exposing (find)
import Time exposing (Zone)


viewDataSetTable : List (Attributo AttributoType) -> Zone -> Dict.Dict Int String -> DataSet -> Bool -> Maybe (Html msg) -> Html msg
viewDataSetTable attributi zone sampleIdToName ds withHeader footer =
    let
        viewAttributiValueRow : AttributoMap AttributoValue -> String -> Html msg
        viewAttributiValueRow attributoValues name =
            case find (\a -> a.name == name) attributi of
                Nothing ->
                    tr_ []

                Just attributo ->
                    tr_
                        [ td_ [ text attributo.name ]
                        , td_ [ viewAttributoCell { shortDateTime = False } zone sampleIdToName attributoValues attributo ]
                        ]
    in
    table
        [ class "table table-sm" ]
        [ if withHeader then
            thead_ [ tr_ [ th_ [ text "Name" ], th_ [ text "Value" ] ] ]

          else
            text ""
        , tbody_ (List.map (viewAttributiValueRow ds.attributi) (attributoMapNames ds.attributi))
        , case footer of
            Nothing ->
                text ""

            Just footerHtml ->
                footerHtml
        ]
