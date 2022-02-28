module Amarcord.Bootstrap exposing (AlertProperty(..), Icon, icon, loadingBar, makeAlert, spinner, viewRemoteData)

import Amarcord.API.Requests exposing (RequestError)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Html exposing (div_, h4_, p_)
import Html exposing (Html, div, i, span, text)
import Html.Attributes exposing (attribute, class, classList)
import List
import RemoteData exposing (RemoteData(..))
import String


type alias Icon =
    { name : String }


spinner : Html msg
spinner =
    div [ class "spinner-border", attribute "role" "status" ] [ span [ class "visually-hidden" ] [] ]


loadingBar : String.String -> Html msg
loadingBar message =
    div [ class "d-flex align-items-center justify-content-center" ] [ div [ class "text-end me-3" ] [ spinner ], div [] [ h4_ [ text message ] ] ]


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


viewRemoteData : String.String -> RemoteData RequestError a -> Html msg
viewRemoteData message x =
    case x of
        NotAsked ->
            text ""

        Loading ->
            p_ [ text "Request in progress..." ]

        Failure e ->
            div_ [ makeAlert [ AlertDanger ] [ showRequestError e ] ]

        Success _ ->
            div [ class "mt-3" ]
                [ makeAlert [ AlertSuccess ] [ text message ]
                ]
