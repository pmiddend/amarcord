module Amarcord.Parser exposing (deadEndsToHtml)

import Html exposing (Html, div, li, p, span, text, ul)
import List exposing (head, map)
import List.Extra exposing (uncons)
import Parser exposing (DeadEnd, Problem(..))
import String exposing (fromInt)


problemToHtml : Problem -> Html msg
problemToHtml x =
    case x of
        Expecting string ->
            text <| "expecting \"" ++ string ++ "\""

        ExpectingInt ->
            text <| "expecting an integer"

        ExpectingHex ->
            text <| "expecting a hex number"

        ExpectingOctal ->
            text <| "expecting an octal number"

        ExpectingBinary ->
            text <| "expecting a binary number"

        ExpectingFloat ->
            text <| "expecting a floating point number"

        ExpectingNumber ->
            text <| "expecting a number"

        ExpectingVariable ->
            text <| "expecting a variable"

        ExpectingSymbol string ->
            text <| "expecting symbol \"" ++ string ++ "\""

        ExpectingKeyword string ->
            text <| "expecting keyword \"" ++ string ++ "\""

        ExpectingEnd ->
            text <| "expecting end of line"

        UnexpectedChar ->
            text <| "unexpected character"

        Problem string ->
            text <| string

        BadRepeat ->
            text <| "bad repeat"


deadEndToHtml : Bool -> DeadEnd -> Html msg
deadEndToHtml singleRow { row, col, problem } =
    let
        prefix =
            if singleRow then
                text <| "At position " ++ fromInt col ++ ": "

            else
                text <| "At row " ++ fromInt row ++ ", column " ++ fromInt col ++ ": "
    in
    span [] [ prefix, problemToHtml problem ]


deadEndsToHtml : Bool -> List DeadEnd -> Html msg
deadEndsToHtml singleRow deadEnds =
    case uncons deadEnds of
        Nothing ->
            text "No problems found"

        Just ( head, tail ) ->
            case uncons tail of
                Nothing ->
                    deadEndToHtml singleRow head

                _ ->
                    div []
                        [ p [] [ text "Multiple problems found:" ]
                        , ul []
                            (map (\x -> li [] [ deadEndToHtml singleRow x ]) deadEnds)
                        ]
