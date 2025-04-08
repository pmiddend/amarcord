module Amarcord.RunFilesForm exposing (Model, Msg(..), init, retrieveFiles, update, view)

import Amarcord.Bootstrap exposing (icon)
import Amarcord.Html exposing (input_, td_, tr_)
import Api.Data exposing (JsonRun, JsonRunFile)
import Html exposing (Html, button, div, label, table, tbody, td, text)
import Html.Attributes exposing (class, for, id, type_, value)
import Html.Events exposing (onClick, onInput)
import List.Extra


type Msg
    = RowChanged Int (JsonRunFile -> JsonRunFile)
    | RowDeleted Int
    | RowAdded


type alias Model =
    { fileList : List JsonRunFile
    }


init : JsonRun -> Model
init run =
    { fileList = run.files }


retrieveFiles : Model -> List JsonRunFile
retrieveFiles { fileList } =
    fileList


viewFileRow : Int -> JsonRunFile -> Html Msg
viewFileRow idx file =
    tr_
        [ td [ class "text-center align-middle" ] [ button [ class "btn btn-sm btn-danger", type_ "button", onClick (RowDeleted idx) ] [ icon { name = "trash" } ] ]
        , td_
            [ div [ class "form-floating" ]
                [ input_
                    [ type_ "text"
                    , value file.source
                    , class "form-control"
                    , id (String.fromInt idx ++ "source")
                    , onInput (\newInput -> RowChanged idx (\oldRow -> { oldRow | source = newInput }))
                    ]
                , label [ for (String.fromInt idx ++ "source") ] [ text "Source" ]
                ]
            ]
        , td_
            [ div [ class "form-floating" ]
                [ input_
                    [ type_ "text"
                    , value file.glob
                    , class "form-control"
                    , id (String.fromInt idx ++ "glob")
                    , onInput (\newInput -> RowChanged idx (\oldRow -> { oldRow | glob = newInput }))
                    ]
                , label [ for (String.fromInt idx ++ "glob") ] [ text "Path" ]
                ]
            ]
        ]


viewAddRow : Html Msg
viewAddRow =
    tr_
        [ td [ class "align-middle text-center" ] [ button [ class "btn btn-sm btn-info mt-3 mb-3", type_ "button", onClick RowAdded ] [ icon { name = "plus-lg" } ] ]
        , td_ []
        , td_ []
        ]


view : Model -> Html Msg
view { fileList } =
    table [ class "table table-sm" ]
        [ tbody []
            (List.indexedMap viewFileRow fileList ++ [ viewAddRow ])
        ]


update : Msg -> Model -> Model
update msg model =
    case msg of
        RowAdded ->
            let
                newFiles =
                    model.fileList ++ [ { id = 0, glob = "", source = "" } ]
            in
            { model | fileList = newFiles }

        RowDeleted idx ->
            let
                newFiles =
                    List.Extra.removeAt idx model.fileList
            in
            { model | fileList = newFiles }

        RowChanged idx modifier ->
            let
                newFiles =
                    List.Extra.updateAt idx modifier model.fileList
            in
            { model | fileList = newFiles }
