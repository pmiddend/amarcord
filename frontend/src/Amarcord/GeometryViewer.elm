module Amarcord.GeometryViewer exposing (Model, Msg, extractId, init, update, view)

import Amarcord.GeometryMetadata exposing (GeometryId, GeometryMetadata)
import Html exposing (Html, span, text)


type alias Model =
    Maybe GeometryMetadata


extractId : Model -> Maybe GeometryId
extractId x =
    x |> Maybe.map .id


type Msg
    = Nop


init : Maybe GeometryMetadata -> Model
init model =
    model


view : Model -> Html Msg
view geom =
    case geom of
        Nothing ->
            text ""

        Just { name } ->
            span [] [ text name ]


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    ( model, Cmd.none )



-- type Mode
--     = FilePath
--     | Full
--     | FullExpanded
-- type alias Model =
--     { mode : Mode
--     , content : String
--     , prefix : String
--     }
-- type Msg
--     = ChangeMode Mode
--     | CopyToClipboard
-- extractMode : Model -> Mode
-- extractMode { mode } =
--     mode
-- init : Maybe Mode -> String -> String -> Model
-- init priorMode prefix content =
--     { mode =
--         case priorMode of
--             Just havePriorMode ->
--                 havePriorMode
--             Nothing ->
--                 if String.startsWith "/" content then
--                     FilePath
--                 else
--                     Full
--     , content = content
--     , prefix = prefix
--     }
-- view : Model -> Html Msg
-- view { mode, content } =
--     if String.isEmpty content then
--         text "auto-detect"
--     else
--         case mode of
--             FilePath ->
--                 div_
--                     -- [ strongText "Geometry file: "
--                     -- , br_
--                     -- , span [ class "text-break" ] [ text content ]
--                     -- , copyToClipboardButton CopyToClipboard
--                     -- ]
--                     [ span [ class "text-break" ] [ text content ]
--                     , copyToClipboardButton CopyToClipboard
--                     ]
--             Full ->
--                 button
--                     [ class "btn btn-sm btn-secondary"
--                     , onClick (ChangeMode FullExpanded)
--                     , style "width" "20em"
--                     ]
--                     [ icon { name = "arrows-angle-expand" }, text " Show full geometry" ]
--             FullExpanded ->
--                 div_
--                     [ button
--                         [ class "btn btn-sm btn-secondary mb-2"
--                         , onClick (ChangeMode Full)
--                         , style "width" "20em"
--                         ]
--                         [ icon { name = "arrows-angle-contract" }
--                         , text " Hide"
--                         ]
--                     , pre [] [ text content ]
--                     ]
-- update : Msg -> Model -> ( Model, Cmd Msg )
-- update msg model =
--     case msg of
--         ChangeMode newMode ->
--             ( { model | mode = newMode }, Cmd.none )
--         CopyToClipboard ->
--             ( model, copyToClipboard model.content )
-- extractContent : Model -> String
-- extractContent { content } =
--     content
