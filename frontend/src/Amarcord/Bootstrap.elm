module Amarcord.Bootstrap exposing (AlertProperty(..), Button, ButtonType, Icon, icon, loadingBar, makeAlert, showHttpError, spinner, viewRemoteData)

import Amarcord.Html exposing (div_, h4_, p_)
import Html as Html exposing (Html, div, h5, i, pre, span, text)
import Html.Attributes exposing (attribute, class, classList)
import Html.Events exposing (onClick)
import Http
import List exposing (singleton)
import Maybe.Extra exposing (unwrap)
import RemoteData exposing (RemoteData(..))
import String exposing (fromInt)


type alias Icon =
    { name : String }


type alias Button msg =
    { type_ : ButtonType, icon : Maybe Icon, content : List (Html msg), onClick : msg }


type ButtonType
    = ButtonPrimary


spinner : Html msg
spinner =
    div [ class "spinner-border", attribute "role" "status" ] [ span [ class "visually-hidden" ] [] ]


loadingBar : String.String -> Html msg
loadingBar message =
    div [ class "d-flex align-items-center justify-content-center" ] [ div [ class "text-end me-3" ] [ spinner ], div [] [ h4_ [ text message ] ] ]


buttonTypeToString : ButtonType -> String
buttonTypeToString x =
    case x of
        ButtonPrimary ->
            "primary"


icon : Icon -> Html msg
icon { name } =
    i [ class ("bi-" ++ name) ] []


type AlertProperty
    = AlertPrimary
    | AlertSecondary
    | AlertSuccess
    | AlertDanger
    | AlertWarning
    | AlertInfo
    | AlertLight
    | AlertDark
    | AlertSmall
    | AlertDismissible


alertPropToCss : AlertProperty -> String
alertPropToCss x =
    case x of
        AlertSmall ->
            "amarcord-alert-small"

        AlertDismissible ->
            "alert-dismissible"

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


makeAlert : List AlertProperty -> List (Html msg) -> Html msg
makeAlert props content =
    div [ classList <| ( "alert", True ) :: List.map (\x -> ( alertPropToCss x, True )) props ] content


viewRemoteData : String.String -> RemoteData Http.Error a -> Html msg
viewRemoteData message x =
    case x of
        NotAsked ->
            text ""

        Loading ->
            p_ [ text "Request in progress..." ]

        Failure e ->
            div_ [ makeAlert [ AlertDanger ] (showHttpError e) ]

        Success _ ->
            div [ class "mt-3" ]
                [ makeAlert [ AlertSuccess ] [ text message ]
                ]


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
