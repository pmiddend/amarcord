module Amarcord.API.RequestsHtml exposing (showHttpError)

import Amarcord.Html exposing (div_)
import Html exposing (Html, h5, pre, text)
import Html.Attributes exposing (class)
import Http


showHttpError : Http.Error -> Html msg
showHttpError x =
    case x of
        Http.BadUrl url ->
            h5 [ class "alert-heading" ] [ text ("Bad URL: " ++ url) ]

        Http.Timeout ->
            h5 [ class "alert-heading" ] [ text "Timeout" ]

        Http.NetworkError ->
            h5 [ class "alert-heading" ] [ text "Network error" ]

        Http.BadStatus int ->
            h5 [ class "alert-heading" ] [ text <| "Bad status: " ++ String.fromInt int ]

        Http.BadBody errorMessage ->
            div_ [ h5 [ class "alert-heading" ] [ text "Bad Request Body" ], pre [] [ text errorMessage ] ]
