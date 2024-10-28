module Amarcord.Html exposing (..)

import Html exposing (Attribute, Html, code, div, em, form, h1, h2, h3, h4, h5, hr, img, input, li, nav, option, p, select, span, strong, sup, tbody, td, text, th, thead, tr, ul)
import Html.Attributes exposing (selected, value)
import Html.Events exposing (stopPropagationOn, targetValue)
import Html.Events.Extra exposing (targetValueIntParse)
import Json.Decode as Decode


br_ : Html msg
br_ =
    Html.br [] []


small_ : List (Html msg) -> Html msg
small_ =
    Html.small []


th_ : List (Html msg) -> Html msg
th_ x =
    th [] x


code_ : List (Html msg) -> Html msg
code_ x =
    code [] x


ul_ : List (Html msg) -> Html msg
ul_ x =
    ul [] x


nav_ : List (Html msg) -> Html msg
nav_ x =
    nav [] x


sup_ : List (Html msg) -> Html msg
sup_ x =
    sup [] x


img_ : List (Html.Attribute msg) -> Html msg
img_ x =
    img x []


hr_ : Html msg
hr_ =
    hr [] []


strongText : String -> Html msg
strongText x =
    strong [] [ text x ]


span_ : List (Html msg) -> Html msg
span_ x =
    span [] x


p_ : List (Html msg) -> Html msg
p_ x =
    p [] x


li_ : List (Html msg) -> Html msg
li_ x =
    li [] x


tr_ : List (Html msg) -> Html msg
tr_ x =
    tr [] x


td_ : List (Html msg) -> Html msg
td_ x =
    td [] x


thead_ : List (Html msg) -> Html msg
thead_ x =
    thead [] x


tbody_ : List (Html msg) -> Html msg
tbody_ x =
    tbody [] x


div_ : List (Html msg) -> Html msg
div_ x =
    div [] x


form_ : List (Html msg) -> Html msg
form_ x =
    form [] x


h1_ : List (Html msg) -> Html msg
h1_ x =
    h1 [] x


h2_ : List (Html msg) -> Html msg
h2_ x =
    h2 [] x


h3_ : List (Html msg) -> Html msg
h3_ x =
    h3 [] x


h4_ : List (Html msg) -> Html msg
h4_ x =
    h4 [] x


h5_ : List (Html msg) -> Html msg
h5_ x =
    h5 [] x


input_ : List (Html.Attribute msg) -> Html msg
input_ x =
    input x []


em_ : List (Html msg) -> Html msg
em_ =
    em []


alwaysStop : a -> ( a, Bool )
alwaysStop a =
    ( a, True )


onIntInput : (Int -> msg) -> Attribute msg
onIntInput f =
    stopPropagationOn "input" (Decode.map (\x -> ( x, True )) <| Decode.map f targetValueIntParse)


onFloatInput : (Float -> msg) -> Html.Attribute msg
onFloatInput f =
    stopPropagationOn "input"
        (Decode.map alwaysStop
            (Decode.andThen
                (\v ->
                    case String.toFloat v of
                        Nothing ->
                            Decode.fail <| "invalid integer " ++ v

                        Just ov ->
                            Decode.succeed (f ov)
                )
                targetValue
            )
        )


customDecoder : Decode.Decoder a -> (a -> Maybe b) -> Decode.Decoder b
customDecoder d f =
    let
        resultDecoder x =
            case x of
                Just a ->
                    Decode.succeed a

                Nothing ->
                    Decode.fail "error parsing"
    in
    Decode.map f d |> Decode.andThen resultDecoder


onDecoderInput : Decode.Decoder msg -> Attribute msg
onDecoderInput decoder =
    stopPropagationOn
        "input"
    <|
        Decode.map (\x -> ( x, True )) <|
            (targetValue
                |> Decode.andThen
                    (\decodedString ->
                        -- Bit of an oddity here, just to explain what's happening:
                        -- First, "targetValue" will be a JSON decoder into a string. This will be, for example, the
                        -- value of an <option> tag.
                        --
                        -- Here, we take this string, parse it as a JSON value and then use "decoder" to decode this
                        -- JSON value.
                        --
                        -- However, the string itself is not a valid JSON value. JSON strings are surrounded by '"'
                        -- chars, which we have to add here.
                        --
                        -- The parsing might fail if it has quotation marks in it, but that you hopefully notice.
                        case Decode.decodeString decoder ("\"" ++ decodedString ++ "\"") of
                            Err _ ->
                                Decode.fail ("error parsing " ++ decodedString)

                            Ok v ->
                                Decode.succeed v
                    )
            )


onMaybeInput : (String -> Maybe a) -> Attribute a
onMaybeInput converter =
    stopPropagationOn "input" (Decode.map (\x -> ( x, True )) <| customDecoder targetValue converter)


enumSelect : List (Html.Attribute msg) -> List e -> (e -> String) -> (e -> String) -> (String -> Maybe msg) -> e -> Html msg
enumSelect additionalAttributes options toValue toDescription fromString selectedValue =
    let
        makeOption o =
            option
                [ value (toValue o)
                , selected (o == selectedValue)
                ]
                [ text (toDescription o) ]
    in
    select
        (additionalAttributes
            ++ [ stopPropagationOn "input"
                    (Decode.map alwaysStop
                        (Decode.andThen
                            (\v ->
                                case fromString v of
                                    Nothing ->
                                        Decode.fail <| "invalid option " ++ v

                                    Just ov ->
                                        Decode.succeed ov
                            )
                            targetValue
                        )
                    )
               ]
        )
    <|
        List.map makeOption options
