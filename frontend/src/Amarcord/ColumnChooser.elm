module Amarcord.ColumnChooser exposing (Model, Msg, init, resolveChosen, update, updateAttributi, view)

import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoName, AttributoType, AttributoValue)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon)
import Amarcord.Html exposing (input_, p_)
import Dict exposing (Dict)
import Html exposing (Html, a, button, div, h2, label, p, span, text)
import Html.Attributes exposing (autocomplete, checked, class, for, id, type_)
import Html.Events exposing (onClick, onInput)
import List
import Maybe.Extra as MaybeExtra
import Set exposing (Set)
import String
import Tuple exposing (first, second)


type Msg
    = ColumnChooserSubmit
    | ColumnChooserToggleColumn String Bool
    | ColumnChooserToggle


type alias Model =
    { allColumns : Dict AttributoName ( Attributo AttributoType, Int )
    , chosenColumns : Set String
    , editingColumns : Set String
    , open : Bool
    }


attributiDictFromList : List (Attributo AttributoType) -> Dict String ( Attributo AttributoType, Int )
attributiDictFromList attributi =
    List.foldl (\( a, i ) prior -> Dict.insert a.name ( a, i ) prior) Dict.empty (List.indexedMap (\i a -> ( a, i )) attributi)


init : List (Attributo AttributoType) -> Model
init allColumnsList =
    { allColumns = attributiDictFromList allColumnsList
    , chosenColumns = List.foldl (\a prior -> Set.insert a.name prior) Set.empty allColumnsList
    , editingColumns = Set.empty
    , open = False
    }


resolveChosen : Model -> List (Attributo AttributoType)
resolveChosen { chosenColumns, allColumns } =
    List.map first <| List.sortBy second <| List.foldl (\c prior -> MaybeExtra.unwrap [] List.singleton (Dict.get c allColumns) ++ prior) [] (Set.toList chosenColumns)


columnChooserHiddenColumnCount : Model -> Int
columnChooserHiddenColumnCount { allColumns, chosenColumns } =
    Dict.size allColumns - Set.size chosenColumns


updateAttributi : Model -> List (Attributo AttributoType) -> Model
updateAttributi model newAttributi =
    if Dict.isEmpty model.allColumns then
        { allColumns = attributiDictFromList <| List.filter (\a -> a.associatedTable == AssociatedTable.Run) newAttributi
        , chosenColumns =
            List.foldl
                (\a prior ->
                    if a.associatedTable == AssociatedTable.Run then
                        Set.insert a.name prior

                    else
                        prior
                )
                Set.empty
                newAttributi
        , editingColumns = Set.empty
        , open = False
        }

    else
        let
            currentAttributi : List (Attributo AttributoType)
            currentAttributi =
                List.filter (\a -> a.associatedTable == AssociatedTable.Run) newAttributi

            currentAttributiNames : Set AttributoName
            currentAttributiNames =
                Set.fromList <| List.map .name currentAttributi

            lastAttributiNames : Set AttributoName
            lastAttributiNames =
                Set.fromList <| Dict.keys model.allColumns

            newAttributiNames =
                Set.diff currentAttributiNames lastAttributiNames

            deletedAttributiNames =
                Set.diff lastAttributiNames currentAttributiNames
        in
        { allColumns = attributiDictFromList currentAttributi
        , chosenColumns = Set.union newAttributiNames (Set.diff model.chosenColumns deletedAttributiNames)
        , editingColumns = Set.diff model.editingColumns deletedAttributiNames
        , open = model.open
        }


view : Model -> Html Msg
view columnChooser =
    let
        viewAttributoButton ( attributoName, attributo ) =
            let
                isChecked =
                    Set.member attributoName columnChooser.editingColumns
            in
            [ input_
                [ type_ "checkbox"
                , class "btn-check"
                , id ("column-" ++ attributoName)
                , autocomplete False
                , checked isChecked
                , onInput (always (ColumnChooserToggleColumn attributoName (not isChecked)))
                ]
            , label
                [ class
                    ("btn "
                        ++ (if isChecked then
                                "btn-outline-success"

                            else
                                "btn-outline-secondary"
                           )
                    )
                , for ("column-" ++ attributoName)
                ]
                [ text attributoName ]
            ]

        hiddenColumnCount =
            columnChooserHiddenColumnCount columnChooser
    in
    div [ class "accordion" ]
        [ div [ class "accordion-item" ]
            [ h2 [ class "accordion-header" ]
                [ button [ class "accordion-button", type_ "button", onClick ColumnChooserToggle ]
                    [ icon { name = "columns" }
                    , span [ class "ms-1" ]
                        [ text <|
                            " Choose columns"
                                ++ (if hiddenColumnCount > 0 then
                                        " (" ++ String.fromInt hiddenColumnCount ++ " hidden)"

                                    else
                                        ""
                                   )
                        ]
                    ]
                ]
            , div
                [ class
                    ("accordion-collapse collapse"
                        ++ (if columnChooser.open then
                                " show"

                            else
                                ""
                           )
                    )
                ]
                [ div [ class "accordion-body" ]
                    [ p [ class "lead" ] [ text "Click to enable/disable columns, then press \"Confirm\"." ]
                    , div [ class "btn-group-vertical mb-3" ] (List.concatMap viewAttributoButton (Dict.toList columnChooser.allColumns))
                    , p_
                        [ button [ class "btn btn-primary me-2", type_ "button", onClick ColumnChooserSubmit ] [ icon { name = "save" }, text " Confirm" ]
                        , button [ class "btn btn-secondary", type_ "button", onClick ColumnChooserToggle ] [ icon { name = "x-lg" }, text " Cancel" ]
                        ]
                    ]
                ]
            ]
        ]


update : Model -> Msg -> ( Model, Cmd msg )
update model message =
    case message of
        ColumnChooserSubmit ->
            let
                newColumnChooser =
                    { allColumns = model.allColumns
                    , editingColumns = Set.empty
                    , chosenColumns = model.editingColumns
                    , open = False
                    }
            in
            ( newColumnChooser, Cmd.none )

        ColumnChooserToggleColumn attributo turnOn ->
            let
                newColumnChooser =
                    { allColumns = model.allColumns
                    , editingColumns =
                        if turnOn then
                            Set.insert attributo model.editingColumns

                        else
                            Set.remove attributo model.editingColumns
                    , chosenColumns = model.chosenColumns
                    , open = True
                    }
            in
            ( newColumnChooser, Cmd.none )

        ColumnChooserToggle ->
            if model.open then
                let
                    newColumnChooser =
                        { allColumns = model.allColumns
                        , editingColumns = Set.empty
                        , chosenColumns = model.chosenColumns
                        , open = False
                        }
                in
                ( newColumnChooser, Cmd.none )

            else
                let
                    newColumnChooser =
                        { allColumns = model.allColumns
                        , editingColumns = model.chosenColumns
                        , chosenColumns = model.chosenColumns
                        , open = True
                        }
                in
                ( newColumnChooser, Cmd.none )
