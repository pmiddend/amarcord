module Amarcord.Bootstrap exposing (AlertProperty(..), Icon, icon, loadingBar, makeAlert, mimeTypeToIcon, spinner, viewCloseHelpButton, viewHelpButton, viewMarkdownSupportText, viewRemoteData, viewRemoteDataHttp)

import Amarcord.API.RequestsHtml exposing (showHttpError)
import Amarcord.Html exposing (div_, h4_, p_)
import Html exposing (Html, a, button, div, i, span, sup, text)
import Html.Attributes exposing (attribute, class, classList, href)
import Http
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


viewRemoteData : String.String -> RemoteData Http.Error a -> Html msg
viewRemoteData message x =
    case x of
        NotAsked ->
            text ""

        Loading ->
            p_ [ text "Request in progress..." ]

        Failure e ->
            div_ [ makeAlert [ AlertDanger ] [ showHttpError e ] ]

        Success _ ->
            div [ class "mt-3" ]
                [ makeAlert [ AlertSuccess ] [ text message ]
                ]


viewRemoteDataHttp : String.String -> RemoteData Http.Error a -> Html msg
viewRemoteDataHttp message x =
    case x of
        NotAsked ->
            text ""

        Loading ->
            p_ [ text "Request in progress..." ]

        Failure e ->
            div_ [ makeAlert [ AlertDanger ] [ showHttpError e ] ]

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


viewHelpButton : String -> Html msg
viewHelpButton target =
    sup []
        [ button
            [ class "btn btn-link rounded-circle"
            , attribute "data-bs-toggle" "collapse"
            , attribute "href" ("#" ++ target)
            ]
            [ icon { name = "patch-question-fill" } ]
        ]


viewCloseHelpButton : String -> Html msg
viewCloseHelpButton target =
    div
        [ class "d-flex flex-row-reverse"
        ]
        [ button
            [ class "btn btn-link"
            , attribute "data-bs-toggle" "collapse"
            , attribute "href" ("#" ++ target)
            ]
            [ icon { name = "arrows-collapse" }, text " Close help" ]
        ]


viewMarkdownSupportText : Html msg
viewMarkdownSupportText =
    div [ class "form-text" ] [ a [ href "https://github.github.com/gfm/" ] [ text "Markdown" ], text " is supported" ]
