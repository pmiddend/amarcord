module Amarcord.Bootstrap exposing (AlertProperty(..), Icon, icon, loadingBar, makeAlert, mimeTypeToIcon, spinner, viewRemoteData)

import Amarcord.API.Requests exposing (RequestError)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Html exposing (div_, h4_, p_)
import Html exposing (Html, div, i, span, text)
import Html.Attributes exposing (attribute, class, classList, style)
import List
import RemoteData exposing (RemoteData(..))
import String


type alias Icon =
    { name : String }


spinner : Int -> Html msg
spinner scaleFactor =
    div [ class "spinner-border", attribute "role" "status", style "scale" (String.fromInt scaleFactor ++ "%") ] [ span [ class "visually-hidden" ] [] ]


loadingBar : String.String -> Html msg
loadingBar message =
    div [ class "d-flex align-items-center justify-content-center" ] [ div [ class "text-end me-3" ] [ spinner 100 ], div [] [ h4_ [ text message ] ] ]


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


mimeTypeToIcon : String -> Html msg
mimeTypeToIcon type_ =
    icon
        { name =
            case String.split "/" type_ of
                "text" :: "x-shellscript" :: [] ->
                    "file-code"

                "application" :: "pdf" :: [] ->
                    "file-pdf"

                prefix :: _ ->
                    case prefix of
                        "image" ->
                            "file-image"

                        "text" ->
                            "file-text"

                        _ ->
                            "question-diamond"

                _ ->
                    "question-diamond"
        }
