module Amarcord.DataSetHtml exposing (..)

import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue, attributoMapNames)
import Amarcord.AttributoHtml exposing (viewAttributoCell)
import Amarcord.DataSet exposing (DataSet)
import Amarcord.Html exposing (tbody_, td_, th_, thead_, tr_)
import Amarcord.Sample exposing (Sample, sampleIdDict)
import Html exposing (Html, table, text)
import Html.Attributes exposing (class)
import List.Extra exposing (find)
import Time exposing (Zone)


viewDataSetTable : List (Attributo AttributoType) -> Zone -> List (Sample Int b c) -> DataSet -> Maybe (Html msg) -> Html msg
viewDataSetTable attributi zone samples ds footer =
    let
        viewAttributiValueRow : AttributoMap AttributoValue -> String -> Html msg
        viewAttributiValueRow attributoValues name =
            case find (\a -> a.name == name) attributi of
                Nothing ->
                    tr_ []

                Just attributo ->
                    tr_ [ td_ [ text attributo.name ], td_ [ viewAttributoCell { shortDateTime = False } zone (sampleIdDict samples) attributoValues attributo ] ]
    in
    table [ class "table table-sm" ]
        [ thead_ [ tr_ [ th_ [ text "Name" ], th_ [ text "Value" ] ] ]
        , tbody_ (List.map (viewAttributiValueRow ds.attributi) (attributoMapNames ds.attributi))
        , case footer of
            Nothing ->
                text ""

            Just footerHtml ->
                footerHtml
        ]
