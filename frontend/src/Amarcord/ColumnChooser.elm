module Amarcord.ColumnChooser exposing (Model, Msg, init, resolveChosen, update, updateAttributi, view)

import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoName, AttributoType, AttributoValue)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon)
import Amarcord.Html exposing (input_, p_)
import Html exposing (Html, a, button, div, h2, label, p, span, text)
import Html.Attributes exposing (autocomplete, checked, class, for, id, type_)
import Html.Events exposing (onClick, onInput)
import List
import List.Extra as ListExtra
import Set exposing (Set)
import String


type Direction
    = Up
    | Down


type Msg
    = ColumnChooserSubmit
    | ColumnChooserToggleColumn String Bool
    | ColumnChooserToggle
    | ColumnChooserMove String Direction


type alias ToggledAttributo =
    { attributo : Attributo AttributoType
    , isOn : Bool
    }


toggleOn : Attributo AttributoType -> ToggledAttributo
toggleOn a =
    { attributo = a, isOn = True }


type alias Model =
    { allColumns : List ToggledAttributo
    , editingColumns : List ToggledAttributo
    , open : Bool
    }


init : List (Attributo AttributoType) -> Model
init allColumnsList =
    { allColumns = List.map toggleOn allColumnsList
    , editingColumns = []
    , open = False
    }


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
            List.filterMap
                (\a ->
                    if a.associatedTable == AssociatedTable.Run then
                        Just { attributo = a, isOn = True }

                    else
                        Nothing
                )
                newAttributi
        , editingColumns = []
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
                List.filter (\ta -> not <| Set.member ta.attributo.name deletedAttributiNames) attributi ++ List.map toggleOn newAttributiResolved
        in
        { allColumns = processAttributoList model.allColumns
        , editingColumns = processAttributoList model.editingColumns
        , open = model.open
        }


view : Model -> Html Msg
view columnChooser =
    let
        viewAttributoButton : ToggledAttributo -> List (Html Msg)
        viewAttributoButton { attributo, isOn } =
            [ div [ class "d-flex p-2 align-items-center" ]
                [ button [ class "btn btn-primary-outline btn-sm", onClick (ColumnChooserMove attributo.name Down), type_ "button" ]
                    [ icon { name = "arrow-down-circle" } ]
                , button [ class "btn btn-primary-outline btn-sm", onClick (ColumnChooserMove attributo.name Up), type_ "button" ]
                    [ icon { name = "arrow-up-circle" } ]
                , div
                    [ class "form-check ms-2" ]
                    [ input_
                        [ type_ "checkbox"
                        , class "form-check-input"
                        , id ("column-" ++ attributo.name)
                        , autocomplete False
                        , checked isOn
                        , onInput (always (ColumnChooserToggleColumn attributo.name (not isOn)))
                        ]
                    , label
                        [ class "form-check-label"
                        , for ("column-" ++ attributo.name)
                        ]
                        [ text attributo.name ]
                    ]
                ]
            ]

        hiddenColumnCount =
            hiddenCount columnChooser
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
                    , div [ class "col" ] [ div [ class "mb-3" ] (List.concatMap viewAttributoButton columnChooser.editingColumns) ]
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


moveElement : List a -> (a -> Bool) -> Direction -> List a
moveElement rootList f dir =
    let
        moveElementUp xs =
            case xs of
                [] ->
                    []

                first :: middle :: rest ->
                    if f first then
                        xs

                    else if f middle then
                        middle :: first :: rest

                    else
                        first :: moveElementUp (middle :: rest)

                first :: [] ->
                    first :: []

        moveElementDown xs =
            case xs of
                [] ->
                    []

                first :: middle :: rest ->
                    if f first then
                        middle :: first :: rest

                    else
                        first :: moveElementDown (middle :: rest)

                first :: [] ->
                    first :: []
    in
    case dir of
        Up ->
            moveElementUp rootList

        Down ->
            moveElementDown rootList


update : Model -> Msg -> ( Model, Cmd msg )
update model message =
    case message of
        ColumnChooserSubmit ->
            let
                newColumnChooser =
                    { allColumns = model.editingColumns
                    , editingColumns = []
                    , open = False
                    }
            in
            ( newColumnChooser, Cmd.none )

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
                        }
                in
                ( newColumnChooser, Cmd.none )

            else
                let
                    newColumnChooser =
                        { allColumns = model.allColumns
                        , editingColumns = model.allColumns
                        , open = True
                        }
                in
                ( newColumnChooser, Cmd.none )

        ColumnChooserMove string direction ->
            let
                newColumnChooser =
                    { allColumns = model.allColumns
                    , editingColumns = moveElement model.editingColumns (\ta -> ta.attributo.name == string) direction
                    , open = True
                    }
            in
            ( newColumnChooser, Cmd.none )
