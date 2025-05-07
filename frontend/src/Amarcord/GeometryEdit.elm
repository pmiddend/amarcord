module Amarcord.GeometryEdit exposing (Model, Msg, extractContent, init, update, view)

import Amarcord.Html exposing (div_, input_)
import Html exposing (Html, div, label, text, textarea)
import Html.Attributes exposing (checked, class, for, id, name, type_, value)
import Html.Events exposing (onClick, onInput)


type Mode
    = FilePath
    | Full


type alias Model =
    { mode : Mode
    , content : String
    , prefix : String
    }


type Msg
    = ChangeMode Mode
    | ChangeContent String


extractContent : Model -> String
extractContent { content } =
    content


init : String -> String -> Model
init prefix content =
    { mode =
        if String.startsWith "/" content then
            FilePath

        else
            Full
    , content = content
    , prefix = prefix
    }


view : Model -> Html Msg
view { prefix, mode, content } =
    div_
        [ div [ class "btn-group btn-group-sm" ]
            [ input_
                [ type_ "radio"
                , class "btn-check"
                , name (prefix ++ "-mode")
                , id (prefix ++ "-path")
                , onClick (ChangeMode FilePath)
                , checked (mode == FilePath)
                ]
            , label [ for (prefix ++ "-path"), class "btn btn-outline-secondary" ] [ text "File path" ]
            , input_
                [ type_ "radio"
                , class "btn-check"
                , name (prefix ++ "-mode")
                , id (prefix ++ "-full")
                , onClick (ChangeMode Full)
                , checked (mode == Full)
                ]
            , label [ for (prefix ++ "-full"), class "btn btn-outline-secondary" ] [ text "Full" ]
            ]
        , case mode of
            FilePath ->
                div_
                    [ input_ [ type_ "text", value content, class "form-control", onInput ChangeContent ]
                    ]

            Full ->
                textarea [ class "form-control", onInput ChangeContent ] [ text content ]
        ]


update : Msg -> Model -> Model
update msg model =
    case msg of
        ChangeMode newMode ->
            { model | mode = newMode }

        ChangeContent newContent ->
            { model | content = newContent }
