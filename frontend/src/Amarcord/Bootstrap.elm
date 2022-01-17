module Amarcord.Bootstrap exposing (AlertType(..), Button, ButtonType, Icon, button, icon, makeAlert, showHttpError)

import Html as Html exposing (Html, div, h5, i, pre, text)
import Html.Attributes exposing (class)
import Html.Events exposing (onClick)
import Http
import List exposing (singleton)
import Maybe.Extra exposing (unwrap)
import String exposing (fromInt)


type alias Icon =
    { name : String }


type alias Button msg =
    { type_ : ButtonType, icon : Maybe Icon, content : List (Html msg), onClick : msg }


type ButtonType
    = ButtonPrimary


buttonTypeToString : ButtonType -> String
buttonTypeToString x =
    case x of
        ButtonPrimary ->
            "primary"


icon : Icon -> Html msg
icon { name } =
    i [ class ("bi-" ++ name) ] []


button : Button msg -> Html msg
button b =
    Html.button
        [ class ("btn btn-" ++ buttonTypeToString b.type_)
        , onClick b.onClick
        ]
        (unwrap [] (\x -> [ icon x ]) b.icon ++ b.content)


type AlertType
    = AlertPrimary
    | AlertSecondary
    | AlertSuccess
    | AlertDanger
    | AlertWarning
    | AlertInfo
    | AlertLight
    | AlertDark


alertClass : AlertType -> String
alertClass x =
    case x of
        AlertPrimary ->
            "alert-primary"

        AlertSecondary ->
            "alert-secondary"

        AlertSuccess ->
            "alert-success"

        AlertDanger ->
            "alert-danger"

        AlertWarning ->
            "alert-warning"

        AlertInfo ->
            "alert-info"

        AlertLight ->
            "alert-light"

        AlertDark ->
            "alert-dark"


makeAlert : AlertType -> List (Html msg) -> Html msg
makeAlert at content =
    div [ class <| "alert " ++ alertClass at ] content


showHttpError : Http.Error -> List (Html msg)
showHttpError x =
    case x of
        Http.BadUrl url ->
            singleton <| h5 [ class "alert-heading" ] [ text ("Bad URL: " ++ url) ]

        Http.Timeout ->
            singleton <| h5 [ class "alert-heading" ] [ text "Timeout" ]

        Http.NetworkError ->
            singleton <| h5 [ class "alert-heading" ] [ text "Network error" ]

        Http.BadStatus int ->
            singleton <| h5 [ class "alert-heading" ] [ text <| "Bad status: " ++ fromInt int ]

        Http.BadBody errorMessage ->
            [ h5 [ class "alert-heading" ] [ text "Bad Request Body" ], pre [] [ text errorMessage ] ]
