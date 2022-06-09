module Amarcord.ColumnChooser exposing (Model, Msg(..), init, resolveChosen, subscriptions, update, updateAttributi, view)

import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoName, AttributoType, AttributoValue)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon)
import Amarcord.Html exposing (br_, em_, input_, p_)
import Amarcord.LocalStorage exposing (LocalStorage, LocalStorageColumn, encodeLocalStorage)
import DnDList exposing (Listen(..), Movement(..), Operation(..))
import Html exposing (Html, a, button, div, h2, i, label, li, p, span, text, ul)
import Html.Attributes exposing (checked, class, id, type_)
import Html.Events exposing (onClick, onInput)
import List
import List.Extra as ListExtra
import Ports exposing (storeLocalStorage)
import Set exposing (Set)
import String


type alias DragId =
    Int


type alias DropId =
    Int


type Msg
    = ColumnChooserSubmit
    | ColumnChooserToggleColumn String Bool
    | ColumnChooserToggle
    | ColumnChooserDragDrop DnDList.Msg


type alias ToggledAttributo =
    { attributo : Attributo AttributoType
    , isOn : Bool
    }


toggleOn : Attributo AttributoType -> ToggledAttributo
toggleOn a =
    { attributo = a, isOn = True }


toggleOff : Attributo AttributoType -> ToggledAttributo
toggleOff a =
    { attributo = a, isOn = False }


type alias Model =
    { allColumns : List ToggledAttributo
    , editingColumns : List ToggledAttributo
    , open : Bool
    , localStorage : Maybe LocalStorage
    , dnd : DnDList.Model
    }


mergeLocalStorageWithAllColumns : Maybe LocalStorage -> List (Attributo AttributoType) -> List ToggledAttributo
mergeLocalStorageWithAllColumns localStorage allAttributi =
    case localStorage of
        -- Nothing in local storage -> turn all known columns on!
        Nothing ->
            List.map toggleOn allAttributi

        -- We already have columns in local storage
        Just { columns } ->
            let
                currentColumnNames =
                    Set.fromList <| List.map .name allAttributi

                oldColumnNames =
                    Set.fromList <| List.map .attributoName columns

                newColumnNames =
                    Set.diff currentColumnNames oldColumnNames

                newColumns =
                    List.filterMap (\cn -> ListExtra.find (\a -> a.name == cn) allAttributi) (Set.toList newColumnNames)

                -- For each column in local storage, find it in the list of current attributi
                -- If it's found, great. If not, ignore.
                -- This handles the case of columns being added (nothing is done here) and columns being deleted
                -- (they are ignored here).
                transducer : LocalStorageColumn -> List ToggledAttributo -> List ToggledAttributo
                transducer localColumn accumulatedColumns =
                    case ListExtra.find (\a -> a.name == localColumn.attributoName) allAttributi of
                        Nothing ->
                            accumulatedColumns

                        Just a ->
                            { attributo = a, isOn = localColumn.isOn } :: accumulatedColumns
            in
            List.foldr transducer [] columns ++ List.map (\a -> { attributo = a, isOn = False }) newColumns


dndConfig : DnDList.Config ToggledAttributo
dndConfig =
    { beforeUpdate = \_ _ list -> list
    , movement = Vertical
    , listen = OnDrag
    , operation = Rotate
    }


system : DnDList.System ToggledAttributo Msg
system =
    DnDList.create dndConfig ColumnChooserDragDrop


init : Maybe LocalStorage -> List (Attributo AttributoType) -> Model
init localStorage allColumnsList =
    { allColumns = mergeLocalStorageWithAllColumns localStorage (List.filter (\a -> a.associatedTable == AssociatedTable.Run) allColumnsList)
    , editingColumns = []
    , open = False
    , localStorage = localStorage
    , dnd = system.model
    }


subscriptions : Model -> (Msg -> msg) -> Sub msg
subscriptions model f =
    Sub.map f (system.subscriptions model.dnd)


resolveChosen : Model -> List (Attributo AttributoType)
resolveChosen { allColumns } =
    List.filterMap
        (\ta ->
            if ta.isOn then
                Just ta.attributo

            else
                Nothing
        )
        allColumns


hiddenCount : Model -> Int
hiddenCount { allColumns } =
    ListExtra.count (\a -> not a.isOn) allColumns


updateAttributi : Model -> List (Attributo AttributoType) -> Model
updateAttributi model newAttributi =
    if List.isEmpty model.allColumns then
        { allColumns =
            mergeLocalStorageWithAllColumns model.localStorage <| List.filter (\a -> a.associatedTable == AssociatedTable.Run) newAttributi
        , editingColumns = []
        , open = False
        , localStorage = model.localStorage
        , dnd = model.dnd
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
                Set.fromList <| List.map (\a -> a.attributo.name) model.allColumns

            newAttributiNames =
                Set.diff currentAttributiNames lastAttributiNames

            deletedAttributiNames =
                Set.diff lastAttributiNames currentAttributiNames

            newAttributiResolved : List (Attributo AttributoType)
            newAttributiResolved =
                List.filter (\a -> Set.member a.name newAttributiNames) newAttributi

            processAttributoList : List ToggledAttributo -> List ToggledAttributo
            processAttributoList attributi =
                List.filter (\ta -> not <| Set.member ta.attributo.name deletedAttributiNames) attributi ++ List.map toggleOff newAttributiResolved
        in
        { allColumns = processAttributoList model.allColumns
        , editingColumns = processAttributoList model.editingColumns
        , open = model.open
        , localStorage = model.localStorage
        , dnd = model.dnd
        }


view : Model -> Html Msg
view columnChooser =
    let
        hiddenColumnCount =
            hiddenCount columnChooser

        viewAttributoListItem itemIdx { attributo, isOn } =
            let
                itemId =
                    "attributo-chooser-" ++ attributo.name

                cb =
                    input_
                        [ class "form-check-input me-2"
                        , type_ "checkbox"
                        , checked isOn
                        , onInput (always (ColumnChooserToggleColumn attributo.name (not isOn)))
                        ]

                dragHandle props =
                    i (class "bi-grip-vertical me-3" :: props) []
            in
            case system.info columnChooser.dnd of
                Nothing ->
                    li [ class "list-group-item", id itemId ]
                        [ dragHandle (system.dragEvents itemIdx itemId)
                        , label []
                            [ cb
                            , text attributo.name
                            ]
                        ]

                Just { dragIndex } ->
                    if itemIdx == dragIndex then
                        li [ class "list-group-item list-group-item-light", id itemId ] [ dragHandle [], label [] [ cb, em_ [ text "«drop here»" ] ] ]

                    else
                        li ([ class "list-group-item", id itemId ] ++ system.dropEvents itemIdx itemId)
                            [ dragHandle []
                            , cb
                            , text attributo.name
                            ]

        ghostView =
            case system.info columnChooser.dnd |> Maybe.andThen (\{ dragIndex } -> columnChooser.editingColumns |> List.drop dragIndex |> List.head) of
                Just item ->
                    Html.div
                        (system.ghostStyles columnChooser.dnd)
                        [ text item.attributo.name ]

                Nothing ->
                    text ""
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
                    [ p [ class "lead" ] [ text "Click to enable/disable columns, then press \"Confirm\".", br_, text "Press and hold ", icon { name = "grip-vertical" }, text " to drag and drop to change order." ]
                    , ul [ class "list-group mb-3" ] (List.indexedMap viewAttributoListItem columnChooser.editingColumns)
                    , ghostView

                    --                    , div [ class "col" ] [ div [ class "mb-3" ] (List.concatMap viewAttributoButton columnChooser.editingColumns) ]
                    , p_
                        [ button [ class "btn btn-primary me-2", type_ "button", onClick ColumnChooserSubmit ] [ icon { name = "save" }, text " Confirm" ]
                        , button [ class "btn btn-secondary", type_ "button", onClick ColumnChooserToggle ] [ icon { name = "x-lg" }, text " Cancel" ]
                        ]
                    ]
                ]
            ]
        ]



-- up 3 [1,2,3]
-- 1 :: 2 :: [3]
-- 2 :: 3 :: f [3]


makeLocalStorageColumns : List ToggledAttributo -> List LocalStorageColumn
makeLocalStorageColumns =
    List.map (\{ attributo, isOn } -> { isOn = isOn, attributoName = attributo.name })


update : Model -> Msg -> ( Model, Cmd Msg )
update model message =
    case message of
        ColumnChooserSubmit ->
            let
                newColumnChooser =
                    { allColumns = model.editingColumns
                    , editingColumns = []
                    , open = False
                    , localStorage = model.localStorage
                    , dnd = model.dnd
                    }
            in
            ( newColumnChooser, storeLocalStorage (encodeLocalStorage { columns = makeLocalStorageColumns model.editingColumns }) )

        ColumnChooserToggleColumn attributoNameToChange turnOn ->
            let
                newColumnChooser =
                    { allColumns = model.allColumns
                    , editingColumns =
                        List.map
                            (\{ attributo, isOn } ->
                                { attributo = attributo
                                , isOn =
                                    if attributo.name == attributoNameToChange then
                                        turnOn

                                    else
                                        isOn
                                }
                            )
                            model.editingColumns
                    , open = True
                    , localStorage = model.localStorage
                    , dnd = model.dnd
                    }
            in
            ( newColumnChooser, Cmd.none )

        ColumnChooserToggle ->
            if model.open then
                let
                    newColumnChooser =
                        { allColumns = model.allColumns
                        , editingColumns = []
                        , open = False
                        , localStorage = model.localStorage
                        , dnd = model.dnd
                        }
                in
                ( newColumnChooser, Cmd.none )

            else
                let
                    newColumnChooser =
                        { allColumns = model.allColumns
                        , editingColumns = model.allColumns
                        , open = True
                        , localStorage = model.localStorage
                        , dnd = model.dnd
                        }
                in
                ( newColumnChooser, Cmd.none )

        ColumnChooserDragDrop msg ->
            let
                ( dnd, items ) =
                    system.update msg model.dnd model.editingColumns

                newColumnChooser : Model
                newColumnChooser =
                    { allColumns = model.allColumns
                    , editingColumns = items
                    , open = True
                    , localStorage = model.localStorage
                    , dnd = dnd
                    }
            in
            ( newColumnChooser, system.commands dnd )
