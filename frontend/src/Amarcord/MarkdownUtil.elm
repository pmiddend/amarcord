module Amarcord.MarkdownUtil exposing (markupWithoutErrors)

import Html exposing (Html, div, text)
import Markdown.Parser as Markdown
import Markdown.Renderer
import Parser exposing (Problem)
import Parser.Advanced exposing (DeadEnd)


deadEndsToString : List (DeadEnd String Problem) -> String
deadEndsToString deadEnds =
    deadEnds
        |> List.map Markdown.deadEndToString
        |> String.join "\n"


markupWithoutErrors : String -> Html msg
markupWithoutErrors t =
    case
        t
            |> Markdown.parse
            |> Result.mapError deadEndsToString
            |> Result.andThen (Markdown.Renderer.render Markdown.Renderer.defaultHtmlRenderer)
    of
        Ok rendered ->
            div [] rendered

        Err _ ->
            text t
