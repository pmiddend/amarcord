module Amarcord.AttributoHtml exposing (AttributoEditValue(..), AttributoEditValueWithStatus, AttributoFormMsg(..), AttributoNameWithValueUpdate, EditStatus(..), EditableAttributi, EditableAttributiAndOriginal, EditableAttributo, convertEditValues, createEditableAttributi, editEditableAttributi, emptyEditableAttributiAndOriginal, extractStringAttributo, findEditableAttributo, formatFloatHumanFriendly, formatIntHumanFriendly, isEditValueChemicalId, makeAttributoHeader, resetEditableAttributo, unsavedAttributoChanges, viewAttributoCell, viewAttributoForm, viewRunExperimentTypeCell)

import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoName, AttributoType(..), AttributoValue(..), attributoValueToString, createAnnotatedAttributoMap, emptyAttributoMap, mapAttributo, retrieveAttributoValue, updateAttributoMap)
import Amarcord.Chemical exposing (Chemical, ChemicalType)
import Amarcord.Html exposing (br_, em_, input_, span_, strongText)
import Amarcord.MarkdownUtil exposing (markupWithoutErrors)
import Amarcord.NumericRange exposing (NumericRange, emptyNumericRange, numericRangeToString, valueInRange)
import Amarcord.Util exposing (collectResults, formatPosixDateTimeCompatible, formatPosixHumanFriendly, formatPosixTimeOfDayHumanFriendly, localDateTimeParser)
import Dict exposing (Dict, get)
import FormatNumber exposing (format)
import FormatNumber.Locales exposing (Decimals(..), Locale, usLocale)
import Html exposing (Html, div, label, option, select, span, td, text)
import Html.Attributes exposing (checked, class, for, id, selected, step, style, type_, value)
import Html.Events exposing (onInput)
import Html.Events.Extra exposing (onEnter)
import List exposing (intersperse)
import List.Extra as List
import Maybe exposing (withDefault)
import Maybe.Extra exposing (isNothing, orElse, traverse, unwrap)
import Parser exposing (deadEndsToString, run)
import Set
import String exposing (fromInt, join, split, toInt, trim)
import Time exposing (Zone, millisToPosix, posixToMillis)
import Time.Extra exposing (partsToPosix)
import Tuple exposing (second)


mutedSubheader : String.String -> Html.Html msg
mutedSubheader t =
    span [ class "text-muted fst-italic", style "font-size" "0.8rem" ] [ text t ]


makeAttributoHeader : Attributo AttributoType -> List (Html msg)
makeAttributoHeader a =
    case a.type_ of
        Number { suffix } ->
            case suffix of
                Just realSuffix ->
                    [ text a.name
                    , br_
                    , mutedSubheader realSuffix
                    ]

                _ ->
                    [ text a.name ]

        _ ->
            [ text a.name ]


type alias ViewAttributoValueProperties =
    { shortDateTime : Bool
    , colorize : Bool
    }


manualCellClass : String
manualCellClass =
    "table-info"


viewRunExperimentTypeCell : String -> Html msg
viewRunExperimentTypeCell experimentType =
    td [ class manualCellClass ] [ text experimentType ]


viewAttributoCell : ViewAttributoValueProperties -> Zone -> Dict Int String -> AttributoMap AttributoValue -> Attributo AttributoType -> Html msg
viewAttributoCell props zone chemicalIds attributiValues { name, group, type_ } =
    td
        [ class
            (if props.colorize && group == "manual" then
                manualCellClass

             else
                ""
            )
        ]
        [ case retrieveAttributoValue name attributiValues of
            Nothing ->
                text ""

            Just v ->
                viewAttributoValue props zone chemicalIds type_ v
        ]


viewAttributoValue : ViewAttributoValueProperties -> Zone -> Dict Int String -> AttributoType -> AttributoValue -> Html msg
viewAttributoValue props zone chemicalIds type_ value =
    case value of
        ValueNone ->
            text ""

        ValueBoolean bool ->
            if bool then
                text "yes"

            else
                text "no"

        ValueInt int ->
            case type_ of
                ChemicalId ->
                    if int == 0 then
                        text ""

                    else
                        case get int chemicalIds of
                            Nothing ->
                                text "invalid chemical ID"

                            Just sid ->
                                text sid

                DateTime ->
                    text <|
                        (if props.shortDateTime then
                            formatPosixTimeOfDayHumanFriendly

                         else
                            formatPosixHumanFriendly
                        )
                            zone
                            (millisToPosix int)

                _ ->
                    text (formatIntHumanFriendly int)

        ValueString string ->
            text string

        ValueList attributoValues ->
            case type_ of
                List { subType } ->
                    case subType of
                        Number _ ->
                            span_ <| text "(" :: intersperse (text ", ") (List.map (viewAttributoValue props zone chemicalIds subType) attributoValues) ++ [ text ")" ]

                        String ->
                            span_ <| intersperse (text ",") <| List.map (viewAttributoValue props zone chemicalIds subType) attributoValues

                        _ ->
                            text "unsupported list element type"

                _ ->
                    text "unsupported list type"

        ValueNumber float ->
            text (formatFloatHumanFriendly float)


formatIntHumanFriendly : Int -> String
formatIntHumanFriendly =
    -- Taken from https://github.com/cuducos/elm-format-number/blob/main/src/FormatNumber/Parser.elm
    let
        splitByWestern : String -> List String
        splitByWestern integers =
            let
                reversedSplitThousands : String -> List String
                reversedSplitThousands value =
                    if String.length value > 3 then
                        value
                            |> String.dropRight 3
                            |> reversedSplitThousands
                            |> (::) (String.right 3 value)

                    else
                        [ value ]
            in
            integers
                |> reversedSplitThousands
                |> List.reverse
    in
    String.join "," << splitByWestern << String.fromInt


formatFloatHumanFriendly : Float -> String
formatFloatHumanFriendly float =
    let
        locale : Locale
        locale =
            { usLocale | decimals = Max 3 }
    in
    format locale float


type EditStatus
    = Edited
    | Unchanged


type AttributoEditValue
    = EditValueInt String
    | EditValueDateTime String
    | EditValueBoolean Bool
    | EditValueChemicalId (Maybe Int)
    | EditValueString String
    | EditValueList
        { subType : AttributoType
        , minLength : Maybe Int
        , maxLength : Maybe Int
        , editValue : String
        }
    | EditValueNumber
        { range : NumericRange
        , suffix : Maybe String
        , standardUnit : Bool
        , editValue : String
        , editValueTolerance : String
        , toleranceIsAbsolute : Bool
        }
    | EditValueChoice { choiceValues : List String, editValue : String }


isEditValueChemicalId : AttributoEditValue -> Bool
isEditValueChemicalId x =
    case x of
        EditValueChemicalId _ ->
            True

        _ ->
            False


type ValueUpdate
    = SetValue AttributoEditValue


type alias AttributoNameWithValueUpdate =
    { attributoName : AttributoName
    , valueUpdate : ValueUpdate
    }


type AttributoFormMsg
    = AttributoFormValueUpdate AttributoNameWithValueUpdate
    | AttributoFormSubmit


viewAttributoForm : List (Chemical Int a b) -> ChemicalType -> EditableAttributo -> Html AttributoFormMsg
viewAttributoForm chemicals chemicalType a =
    case a.type_.editValue of
        EditValueString s ->
            div [ class "mb-3" ] <|
                [ label [ for ("attributo-" ++ a.name), class "form-label" ] [ text a.name ]
                , input_
                    [ type_ "text"
                    , class "form-control"
                    , id ("attributo-" ++ a.name)
                    , value s
                    , onEnter AttributoFormSubmit
                    , onInput (EditValueString >> SetValue >> AttributoNameWithValueUpdate a.name >> AttributoFormValueUpdate)
                    ]
                , if a.description /= "" then
                    div [ class "form-text" ] [ markupWithoutErrors a.description ]

                  else
                    text ""
                ]

        EditValueInt s ->
            div [ class "mb-3" ] <|
                [ label [ for ("attributo-" ++ a.name), class "form-label" ] [ text a.name ]
                , div [ class "w-50" ]
                    [ input_
                        [ type_ "number"
                        , class "form-control"
                        , id ("attributo-" ++ a.name)
                        , value s
                        , onEnter AttributoFormSubmit
                        , onInput (AttributoFormValueUpdate << AttributoNameWithValueUpdate a.name << SetValue << EditValueInt)
                        ]
                    ]
                , if a.description /= "" then
                    div [ class "form-text" ] [ text a.description ]

                  else
                    text ""
                ]

        EditValueList l ->
            div [ class "mb-3" ] <|
                [ label [ for ("attributo-" ++ a.name), class "form-label" ] [ text a.name ]
                , input_
                    [ type_ "text"
                    , class "form-control"
                    , id ("attributo-" ++ a.name)
                    , value l.editValue
                    , onEnter AttributoFormSubmit
                    , onInput (\newInput -> AttributoFormValueUpdate <| AttributoNameWithValueUpdate a.name <| SetValue <| EditValueList { l | editValue = newInput })
                    ]
                , div [ class "form-text text-muted" ] [ strongText "Note on editing", text ": This is a list, but you can just insert the list elements, comma-separated in the text field." ]
                , if a.description /= "" then
                    div [ class "form-text" ] [ strongText "Description: ", text a.description ]

                  else
                    text ""
                ]

        EditValueChoice { choiceValues, editValue } ->
            let
                makeOption choiceValue =
                    option
                        [ selected (editValue == choiceValue)
                        , value choiceValue
                        ]
                        [ text
                            (if choiceValue == "" then
                                "«no value»"

                             else
                                choiceValue
                            )
                        ]
            in
            div [ class "mb-3" ] <|
                [ label [ for ("attributo-" ++ a.name), class "form-label" ] [ text a.name ]
                , select
                    [ id ("attributo-" ++ a.name)
                    , class "form-select"
                    , onInput (\newInput -> AttributoFormValueUpdate <| AttributoNameWithValueUpdate a.name <| SetValue <| EditValueChoice { choiceValues = choiceValues, editValue = newInput })
                    ]
                    (List.map makeOption ("" :: choiceValues))
                ]

        EditValueNumber n ->
            let
                inputGroupText =
                    withDefault "" n.suffix
                        ++ (if n.range == emptyNumericRange then
                                ""

                            else
                                " ∈ " ++ numericRangeToString n.range
                           )
            in
            div [ class "mb-3" ] <|
                [ label [ for ("attributo-" ++ a.name), class "form-label" ] [ text a.name ]
                , div [ class "w-75 input-group" ]
                    [ input_
                        [ type_ "number"
                        , step "0.01"
                        , class "form-control"
                        , id ("attributo-" ++ a.name)
                        , value n.editValue
                        , onEnter AttributoFormSubmit
                        , onInput (\newValue -> AttributoFormValueUpdate <| AttributoNameWithValueUpdate a.name <| SetValue <| EditValueNumber { n | editValue = newValue })
                        ]
                    , if inputGroupText == "" then
                        text ""

                      else
                        span [ class "input-group-text" ] [ text inputGroupText ]
                    ]
                , if a.description /= "" then
                    div [ class "form-text" ] [ text a.description ]

                  else
                    text ""
                ]

        EditValueDateTime x ->
            div [ class "mb-3" ] <|
                [ label [ for ("attributo-" ++ a.name), class "form-label" ] [ text a.name ]
                , div [ class "w-50" ]
                    [ input_
                        [ type_ "datetime-local"
                        , class "form-control"
                        , id ("attributo-" ++ a.name)
                        , value x
                        , onEnter AttributoFormSubmit
                        , onInput (AttributoFormValueUpdate << AttributoNameWithValueUpdate a.name << SetValue << EditValueDateTime)
                        ]
                    ]
                , if a.description /= "" then
                    div [ class "form-text" ] [ text a.description ]

                  else
                    text ""
                ]

        EditValueBoolean x ->
            div [ class "mb-3" ] <|
                [ div [ class "form-check" ] <|
                    [ input_
                        [ type_ "checkbox"
                        , class "form-check-input"
                        , id ("attributo-" ++ a.name)
                        , checked x
                        , onInput (always <| AttributoFormValueUpdate <| AttributoNameWithValueUpdate a.name <| SetValue <| EditValueBoolean (not x))
                        ]
                    , label [ for ("attributo-" ++ a.name), class "form-check-label" ] [ text a.name ]
                    , if a.description /= "" then
                        div [ class "form-text" ] [ text a.description ]

                      else
                        text ""
                    ]
                ]

        EditValueChemicalId selectedId ->
            let
                makeOption { id, name } =
                    option [ selected (Just id == selectedId), value (fromInt id) ] [ text name ]

                filterChemical : Chemical Int a b -> Bool
                filterChemical { type_ } =
                    type_ == chemicalType
            in
            div [ class "mb-3" ] <|
                [ label [ for ("attributo-" ++ a.name), class "form-label" ] [ text a.name ]
                , select
                    [ id ("attributo-" ++ a.name)
                    , class "form-select"
                    , onInput (AttributoFormValueUpdate << AttributoNameWithValueUpdate a.name << SetValue << EditValueChemicalId << String.toInt)
                    ]
                    (option [ selected (isNothing selectedId), value "0" ] [ text "«no value»" ] :: List.map makeOption (List.filter filterChemical chemicals))
                ]


type alias AttributoEditValueWithStatus =
    { editStatus : EditStatus
    , editValue : AttributoEditValue
    }


attributoEditValueWithStatusReset : AttributoEditValueWithStatus -> AttributoEditValueWithStatus
attributoEditValueWithStatusReset { editValue } =
    { editStatus = Unchanged, editValue = editValue }


type alias EditableAttributo =
    Attributo AttributoEditValueWithStatus


type alias EditableAttributi =
    List EditableAttributo


findEditableAttributo : EditableAttributi -> String -> Result (Html msg) EditableAttributo
findEditableAttributo editableAttributi name =
    Result.fromMaybe (span_ [ text <| "attributo ", em_ [ text name ], text " not found" ]) <|
        List.find (\ea -> ea.name == name) editableAttributi


extractStringAttributo : EditableAttributo -> Result (Html msg) String
extractStringAttributo x =
    case x.type_.editValue of
        EditValueString pointGroup ->
            Ok pointGroup

        _ ->
            Err <| text <| "attributo " ++ x.name ++ " has wrong type, is not a string"


resetEditableAttributo : EditableAttributo -> EditableAttributo
resetEditableAttributo =
    mapAttributo attributoEditValueWithStatusReset


type alias EditableAttributiAndOriginal =
    { editableAttributi : EditableAttributi
    , originalAttributi : AttributoMap AttributoValue
    }


emptyEditableAttributiAndOriginal : EditableAttributiAndOriginal
emptyEditableAttributiAndOriginal =
    { editableAttributi = [], originalAttributi = emptyAttributoMap }


unsavedAttributoChanges : EditableAttributi -> Bool
unsavedAttributoChanges =
    List.any (\a -> a.type_.editStatus == Edited)


createEditableAttributi : Zone -> List (Attributo AttributoType) -> AttributoMap AttributoValue -> EditableAttributiAndOriginal
createEditableAttributi zone attributi m =
    let
        -- two steps:
        -- 1. convert existing values into "best" manual values
        -- 2. add missing attributo and add as empty attributi, too
        -- Convert attributo metadata, as well as an attributo value, into an "editable attributo"
        convertToEditValues : String -> Attributo ( AttributoType, AttributoValue ) -> Dict String (Attributo AttributoEditValueWithStatus) -> Dict String (Attributo AttributoEditValueWithStatus)
        convertToEditValues attributoName a prev =
            case attributoValueToEditValue zone attributoName attributi (second a.type_) of
                Nothing ->
                    prev

                Just finishedEditValue ->
                    Dict.insert attributoName
                        (mapAttributo
                            (always
                                { editStatus = Unchanged
                                , editValue = finishedEditValue
                                }
                            )
                            a
                        )
                        prev

        existingAttributiMap : Dict String EditableAttributo
        existingAttributiMap =
            Dict.foldr convertToEditValues Dict.empty (createAnnotatedAttributoMap attributi m)

        totalAttributoNames : Set.Set String
        totalAttributoNames =
            Set.fromList (List.map .name attributi)

        missingKeys : Set.Set String
        missingKeys =
            Set.diff totalAttributoNames <| Set.fromList (Dict.keys existingAttributiMap)

        missingAttributi : List (Attributo AttributoType)
        missingAttributi =
            List.filter (\x -> Set.member x.name missingKeys) attributi

        missingAttributiMap : Dict String EditableAttributo
        missingAttributiMap =
            Dict.fromList <|
                List.map
                    (\a -> ( a.name, mapAttributo (\type_ -> { editStatus = Unchanged, editValue = emptyEditValue type_ }) a ))
                <|
                    missingAttributi
    in
    { originalAttributi = m, editableAttributi = Dict.values <| Dict.union existingAttributiMap missingAttributiMap }


emptyEditValue : AttributoType -> AttributoEditValue
emptyEditValue a =
    case a of
        Int ->
            EditValueInt ""

        Boolean ->
            EditValueBoolean False

        DateTime ->
            EditValueDateTime ""

        ChemicalId ->
            EditValueChemicalId Nothing

        String ->
            EditValueString ""

        List { subType, minLength, maxLength } ->
            EditValueList { subType = subType, minLength = minLength, maxLength = maxLength, editValue = "" }

        Number { range, suffix, standardUnit, tolerance, toleranceIsAbsolute } ->
            EditValueNumber
                { range = range
                , suffix = suffix
                , standardUnit = standardUnit
                , editValue = ""
                , toleranceIsAbsolute = toleranceIsAbsolute
                , editValueTolerance = Maybe.withDefault "" (Maybe.map String.fromFloat tolerance)
                }

        Choice { choiceValues } ->
            EditValueChoice { choiceValues = choiceValues, editValue = "" }


attributoValueToEditValue : Zone -> AttributoName -> List (Attributo AttributoType) -> AttributoValue -> Maybe AttributoEditValue
attributoValueToEditValue zone attributoName attributi value =
    let
        attributoFound : Maybe (Attributo AttributoType)
        attributoFound =
            List.find (\x -> x.name == attributoName) attributi

        convert : Attributo AttributoType -> Maybe AttributoEditValue
        convert a =
            case ( a.type_, value ) of
                ( Int, ValueInt x ) ->
                    Just (EditValueInt (String.fromInt x))

                ( Int, ValueNone ) ->
                    Just (EditValueInt "")

                ( ChemicalId, ValueInt x ) ->
                    Just (EditValueChemicalId (Just x))

                ( ChemicalId, ValueNone ) ->
                    Just (EditValueChemicalId Nothing)

                ( String, ValueNone ) ->
                    Just (EditValueString "")

                ( String, ValueString x ) ->
                    Just (EditValueString x)

                ( DateTime, ValueInt x ) ->
                    Just (EditValueDateTime (formatPosixDateTimeCompatible zone (millisToPosix x)))

                ( Choice { choiceValues }, ValueNone ) ->
                    Just (EditValueChoice { editValue = "", choiceValues = choiceValues })

                ( Choice { choiceValues }, ValueString x ) ->
                    Just (EditValueChoice { editValue = x, choiceValues = choiceValues })

                ( Number { range, suffix, standardUnit, tolerance, toleranceIsAbsolute }, ValueNone ) ->
                    Just
                        (EditValueNumber
                            { range = range
                            , suffix = suffix
                            , standardUnit = standardUnit
                            , editValue = ""
                            , editValueTolerance = Maybe.withDefault "" (Maybe.map String.fromFloat tolerance)
                            , toleranceIsAbsolute = toleranceIsAbsolute
                            }
                        )

                ( Number { range, suffix, standardUnit, tolerance, toleranceIsAbsolute }, ValueNumber x ) ->
                    Just
                        (EditValueNumber
                            { range = range
                            , suffix = suffix
                            , standardUnit = standardUnit
                            , editValue = String.fromFloat x
                            , editValueTolerance = Maybe.withDefault "" (Maybe.map String.fromFloat tolerance)
                            , toleranceIsAbsolute = toleranceIsAbsolute
                            }
                        )

                ( Number { range, suffix, standardUnit, tolerance, toleranceIsAbsolute }, ValueInt x ) ->
                    Just
                        (EditValueNumber
                            { range = range
                            , suffix = suffix
                            , standardUnit = standardUnit
                            , editValue = String.fromInt x
                            , editValueTolerance = Maybe.withDefault "" (Maybe.map String.fromFloat tolerance)
                            , toleranceIsAbsolute = toleranceIsAbsolute
                            }
                        )

                ( List { minLength, maxLength, subType }, ValueNone ) ->
                    Just
                        (EditValueList
                            { subType = subType
                            , minLength = minLength
                            , maxLength = maxLength
                            , editValue = ""
                            }
                        )

                ( List { minLength, maxLength, subType }, ValueList xs ) ->
                    Just
                        (EditValueList
                            { subType = subType
                            , minLength = minLength
                            , maxLength = maxLength
                            , editValue = join "," <| List.map attributoValueToString xs
                            }
                        )

                ( Boolean, ValueBoolean x ) ->
                    Just (EditValueBoolean x)

                ( Boolean, ValueNone ) ->
                    Just (EditValueBoolean False)

                _ ->
                    Nothing

        -- Debug.log (Debug.toString x) Nothing
    in
    Maybe.andThen convert attributoFound


editEditableAttributi : EditableAttributi -> AttributoNameWithValueUpdate -> EditableAttributi
editEditableAttributi ea { attributoName, valueUpdate } =
    case valueUpdate of
        SetValue newValue ->
            let
                -- Update the value if it exists (if it doesn't, this is weird!), and set the state to "edited"
                -- so when the object is stored, it's added to the manually edited attributes list
                updateValue : Attributo AttributoEditValueWithStatus -> Attributo AttributoEditValueWithStatus
                updateValue x =
                    if x.name == attributoName then
                        mapAttributo (always { editStatus = Edited, editValue = newValue }) x

                    else
                        x
            in
            List.map updateValue ea


editValueToValue : Zone -> AttributoEditValue -> Result String AttributoValue
editValueToValue zone x =
    case x of
        EditValueInt "" ->
            Ok ValueNone

        EditValueInt string ->
            unwrap (Err "not an integer") (Ok << ValueInt) <| toInt string

        EditValueBoolean boolValue ->
            Ok (ValueBoolean boolValue)

        EditValueDateTime string ->
            case run localDateTimeParser string of
                Ok { year, month, day, hour, minute } ->
                    Ok <| ValueInt <| posixToMillis <| partsToPosix zone { year = year, month = month, day = day, hour = hour, minute = minute, second = 0, millisecond = 0 }

                Err error ->
                    Err (deadEndsToString error)

        EditValueChemicalId chemicalId ->
            Ok (ValueInt (withDefault 0 chemicalId))

        EditValueString string ->
            Ok (ValueString string)

        EditValueList { minLength, maxLength, subType, editValue } ->
            let
                parts =
                    List.map trim <| split "," editValue

                noParts =
                    List.length parts
            in
            if orElse (Maybe.map (\ml -> noParts < ml) minLength) (Maybe.map (\ml -> noParts > ml) maxLength) == Just True then
                Err "invalid range"

            else
                case subType of
                    String ->
                        Ok (ValueList <| List.map ValueString parts)

                    Number _ ->
                        unwrap (Err "invalid number in list") Ok <| Maybe.map ValueList <| traverse (String.toFloat >> Maybe.map ValueNumber) parts

                    _ ->
                        Err "invalid list subtype"

        EditValueNumber { range, editValue } ->
            if editValue == "" then
                Ok ValueNone

            else
                case String.toFloat editValue of
                    Nothing ->
                        Err "invalid decimal number"

                    Just inputNumeric ->
                        if valueInRange range inputNumeric then
                            Ok (ValueNumber inputNumeric)

                        else
                            Err "value not in range"

        EditValueChoice { choiceValues, editValue } ->
            let
                choiceValid =
                    editValue == "" || List.member editValue choiceValues
            in
            if choiceValid then
                Ok (ValueString editValue)

            else
                Err "invalid choice"


convertEditValues : Zone -> EditableAttributiAndOriginal -> Result (List ( AttributoName, String )) (AttributoMap AttributoValue)
convertEditValues zone { editableAttributi } =
    let
        -- first, filter for manually edited values (the other ones we don't care about here)
        manuallyEdited : List ( AttributoName, AttributoEditValue )
        manuallyEdited =
            List.foldr
                (\attributo prev ->
                    if attributo.type_.editStatus == Edited then
                        ( attributo.name, attributo.type_.editValue ) :: prev

                    else
                        prev
                )
                []
                editableAttributi

        -- Convert the edited value to the real value (with optional error)
        convertSingle : ( AttributoName, AttributoEditValue ) -> Result ( AttributoName, String ) ( AttributoName, AttributoValue )
        convertSingle ( name, v ) =
            case editValueToValue zone v of
                Err e ->
                    -- add attributo name to error for better display later
                    Err ( name, e )

                Ok value ->
                    Ok ( name, value )

        -- Convert _all_ edited values, optionally failing
        converted : Result (List ( AttributoName, String )) (List ( AttributoName, AttributoValue ))
        converted =
            collectResults (List.map convertSingle manuallyEdited)

        -- Combine result of editing with original map
        combineWithOriginal : List ( AttributoName, AttributoValue ) -> AttributoMap AttributoValue
        combineWithOriginal =
            List.foldr (\( name, value ) -> updateAttributoMap name value) Dict.empty
    in
    Result.map combineWithOriginal converted
