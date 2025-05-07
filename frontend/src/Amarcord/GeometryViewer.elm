module Amarcord.GeometryViewer exposing (Model, Msg, extractContent, extractMode, init, update, view)

import Amarcord.Bootstrap exposing (copyToClipboardButton, icon)
import Amarcord.Html exposing (br_, div_, strongText)
import Html exposing (Html, button, pre, span, text)
import Html.Attributes exposing (class, style)
import Html.Events exposing (onClick)
import Ports exposing (copyToClipboard)


type Mode
    = FilePath
    | Full
    | FullExpanded


type alias Model =
    { mode : Mode
    , content : String
    , prefix : String
    }


type Msg
    = ChangeMode Mode
    | CopyToClipboard


extractMode : Model -> Mode
extractMode { mode } =
    mode


init : Maybe Mode -> String -> String -> Model
init priorMode prefix content =
    { mode =
        case priorMode of
            Just havePriorMode ->
                havePriorMode

            Nothing ->
                if String.startsWith "/" content then
                    FilePath

                else
                    Full
    , content = content
    , prefix = prefix
    }


view : Model -> Html Msg
view { mode, content } =
    if String.isEmpty content then
        text "auto-detect"

    else
        case mode of
            FilePath ->
                div_
                    -- [ strongText "Geometry file: "
                    -- , br_
                    -- , span [ class "text-break" ] [ text content ]
                    -- , copyToClipboardButton CopyToClipboard
                    -- ]
                    [ span [ class "text-break" ] [ text content ]
                    , copyToClipboardButton CopyToClipboard
                    ]

            Full ->
                button
                    [ class "btn btn-sm btn-secondary"
                    , onClick (ChangeMode FullExpanded)
                    , style "width" "20em"
                    ]
                    [ icon { name = "arrows-angle-expand" }, text " Show full geometry" ]

            FullExpanded ->
                div_
                    [ button
                        [ class "btn btn-sm btn-secondary mb-2"
                        , onClick (ChangeMode Full)
                        , style "width" "20em"
                        ]
                        [ icon { name = "arrows-angle-contract" }
                        , text " Hide"
                        ]
                    , pre [] [ text content ]
                    ]


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        ChangeMode newMode ->
            ( { model | mode = newMode }, Cmd.none )

        CopyToClipboard ->
            ( model, copyToClipboard model.content )


extractContent : Model -> String
extractContent { content } =
    content
