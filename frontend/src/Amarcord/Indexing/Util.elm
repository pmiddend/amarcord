module Amarcord.Indexing.Util exposing (..)

import Amarcord.CommandLineParser exposing (CommandLineOption(..))
import Amarcord.Html exposing (input_)
import Html exposing (Html, a, div, label, text)
import Html.Attributes exposing (checked, class, for, href, id, type_, value)
import Html.Events exposing (onInput)
import Maybe.Extra


numberToCommandLine : String -> String -> (String -> List CommandLineOption) -> Result String (List CommandLineOption)
numberToCommandLine optionName numberString converter =
    if String.isEmpty numberString then
        Ok []

    else
        case String.toFloat numberString of
            Nothing ->
                Err (optionName ++ ": not a number: " ++ numberString)

            Just _ ->
                Ok (converter numberString)


integerToCommandLine : String -> String -> (String -> List CommandLineOption) -> Result String (List CommandLineOption)
integerToCommandLine optionName numberString converter =
    if String.isEmpty numberString then
        Ok []

    else
        case String.toInt numberString of
            Nothing ->
                Err (optionName ++ ": not an integer: " ++ numberString)

            Just _ ->
                Ok (converter numberString)


viewCitation : String -> String -> Html msg
viewCitation title link =
    a [ href link ] [ text title ]


viewNumericInput : String -> String -> String -> Maybe String -> (String -> msg) -> Html msg
viewNumericInput inputId currentValue description optionalDetails changer =
    div
        [ class "form-floating mb-3"
        ]
        [ input_
            [ type_ "text"
            , class "form-control"
            , id inputId
            , onInput changer
            , value currentValue
            ]
        , label [ for inputId ] [ text description ]
        , case optionalDetails of
            Nothing ->
                text ""

            Just details ->
                div [ class "form-text" ] [ text details ]
        ]


viewFormCheck : String -> String -> Maybe (Html msg) -> Bool -> (() -> msg) -> Html msg
viewFormCheck elementId description details checkedValue f =
    div [ class "form-check" ]
        [ input_
            [ class "form-check-input"
            , type_ "checkbox"
            , checked checkedValue
            , id elementId
            , onInput (\_ -> f ())
            ]
        , label [ class "form-check-label", for elementId ] [ text description ]
        , case details of
            Nothing ->
                text ""

            Just detailsHtml ->
                div [ class "form-text" ] [ detailsHtml ]
        ]


type CommandLineOptionResult model
    = CommandLineUninteresting
    | InvalidCommandLine String
    | ValidCommandLine model


commaSeparatedNumbersToCommandLine : String -> String -> Int -> Result String (List CommandLineOption)
commaSeparatedNumbersToCommandLine optionName commaSeparated numberOfValues =
    let
        numbers =
            Maybe.Extra.values <| List.map (String.toFloat << String.trim) <| String.split "," commaSeparated

        numbersLen =
            List.length numbers
    in
    if numbersLen == 0 then
        Ok []

    else if numbersLen == numberOfValues then
        Ok [ LongOption optionName (String.join "," (List.map String.fromFloat numbers)) ]

    else
        Err (optionName ++ ": invalid format: not " ++ String.fromInt numberOfValues ++ " numbers")


boolToSwitchCommandLine : String -> Bool -> Result error (List CommandLineOption)
boolToSwitchCommandLine switch value =
    if not value then
        Ok []

    else
        Ok [ LongSwitch switch ]


mapMaybe : (a -> Result error (List b)) -> Maybe a -> Result error (List b)
mapMaybe f field =
    -- No specific reason for this to be here, other than that it's used for indexer-related functions right now. Could be moved to the general Util, though.
    Maybe.withDefault (Ok []) (Maybe.map f field)


stringToFloatResult : String -> String -> Result String Float
stringToFloatResult description input =
    case String.toFloat input of
        Nothing ->
            Err (description ++ ": not a number")

        Just result ->
            Ok result
