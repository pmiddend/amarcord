module Amarcord.API.RequestsHtml exposing (showRequestError)

import Amarcord.API.Requests exposing (RequestError(..))
import Amarcord.Html exposing (div_)
import Amarcord.UserError exposing (CustomError)
import Html exposing (Html, h5, pre, text)
import Html.Attributes exposing (class)
import Http
import Maybe.Extra as MaybeExtra


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


showUserError : CustomError -> Html msg
showUserError { title, description } =
    div_
        [ h5 [ class "alert-heading" ] [ text title ]
        , MaybeExtra.unwrap (text "") (\descriptionReal -> pre [] [ text descriptionReal ]) description
        ]


showRequestError : RequestError -> Html msg
showRequestError x =
    case x of
        HttpError error ->
            showHttpError error

        UserError userError ->
            showUserError userError
