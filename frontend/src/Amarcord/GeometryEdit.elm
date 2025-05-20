module Amarcord.GeometryEdit exposing (Model, Msg, extractCurrentId, init, update, view)

import Amarcord.GeometryMetadata exposing (GeometryId(..), GeometryMetadata, geometryIdToString)
import Amarcord.Html exposing (div_, onIntInput)
import Html exposing (Html, option, select, text)
import Html.Attributes exposing (class, selected, value)
import Maybe.Extra exposing (isNothing)


type alias Model =
    { geometries : List GeometryMetadata
    , selectedGeometryId : Maybe GeometryId
    }


extractCurrentId : Model -> Maybe GeometryId
extractCurrentId { selectedGeometryId } =
    selectedGeometryId


init : Maybe GeometryId -> List GeometryMetadata -> Model
init selectedGeometryId geometries =
    { selectedGeometryId = selectedGeometryId, geometries = geometries }


type Msg
    = ChangeSelection GeometryId


update : Msg -> Model -> Model
update msg model =
    case msg of
        ChangeSelection newSelected ->
            { model | selectedGeometryId = Just newSelected }


view : Model -> Html Msg
view model =
    div_
        [ select
            [ class "form-select"
            , onIntInput (ChangeSelection << GeometryId)
            ]
            (option [ selected (isNothing model.selectedGeometryId), value "" ] [ text "«no value»" ]
                :: List.map
                    (\geometry ->
                        option
                            [ value (geometryIdToString geometry.id)
                            , selected (model.selectedGeometryId == Just geometry.id)
                            ]
                            [ text geometry.name ]
                    )
                    model.geometries
            )
        ]



-- type Mode
--     = FilePath
--     | Full
-- type alias Model =
--     { mode : Mode
--     , content : String
--     , prefix : String
--     }
-- type Msg
--     = ChangeMode Mode
--     | ChangeContent String
-- extractContent : Model -> String
-- extractContent { content } =
--     content
-- init : String -> String -> Model
-- init prefix content =
--     { mode =
--         if String.startsWith "/" content then
--             FilePath
--         else
--             Full
--     , content = content
--     , prefix = prefix
--     }
-- view : Model -> Html Msg
-- view { prefix, mode, content } =
--     div_
--         [ div [ class "btn-group btn-group-sm" ]
--             [ input_
--                 [ type_ "radio"
--                 , class "btn-check"
--                 , name (prefix ++ "-mode")
--                 , id (prefix ++ "-path")
--                 , onClick (ChangeMode FilePath)
--                 , checked (mode == FilePath)
--                 ]
--             , label [ for (prefix ++ "-path"), class "btn btn-outline-secondary" ] [ text "File path" ]
--             , input_
--                 [ type_ "radio"
--                 , class "btn-check"
--                 , name (prefix ++ "-mode")
--                 , id (prefix ++ "-full")
--                 , onClick (ChangeMode Full)
--                 , checked (mode == Full)
--                 ]
--             , label [ for (prefix ++ "-full"), class "btn btn-outline-secondary" ] [ text "Full" ]
--             ]
--         , case mode of
--             FilePath ->
--                 div_
--                     [ input_ [ type_ "text", value content, class "form-control", onInput ChangeContent ]
--                     ]
--             Full ->
--                 textarea [ class "form-control", onInput ChangeContent ] [ text content ]
--         ]
-- update : Msg -> Model -> Model
-- update msg model =
--     case msg of
--         ChangeMode newMode ->
--             { model | mode = newMode }
--         ChangeContent newContent ->
--             { model | content = newContent }
