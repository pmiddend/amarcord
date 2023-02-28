module Amarcord.Bootstrap exposing (AlertProperty(..), Icon, icon, loadingBar, makeAlert, mimeTypeToIcon, spinner, viewRemoteData)

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


spinner : Bool -> Html msg
spinner small =
    div
        [ classList [ ( "spinner-border", True ), ( "spinner-border-sm", small ) ]
        , attribute "role" "status"
        ]
        [ span [ class "visually-hidden" ] [] ]


loadingBar : String.String -> Html msg
loadingBar message =
    div [ class "d-flex align-items-center justify-content-center" ] [ div [ class "text-end me-3" ] [ spinner False ], div [] [ h4_ [ text message ] ] ]


icon : Icon -> Html msg
icon { name } =
    i [ class ("bi-" ++ name) ] []


type AlertProperty
    = AlertSuccess
    | AlertDanger


alertPropToCss : AlertProperty -> String
alertPropToCss x =
    case x of
        AlertSuccess ->
            "alert-success"

        AlertDanger ->
            "alert-danger"


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

                "application" :: "vnd.openxmlformats-officedocument.wordprocessingml.document" :: [] ->
                    "file-richtext"

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
