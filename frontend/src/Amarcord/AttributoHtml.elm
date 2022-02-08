module Amarcord.AttributoHtml exposing (AttributoEditValue(..), AttributoNameWithValueUpdate, EditStatus(..), EditableAttributi, EditableAttributiAndOriginal, EditableAttributo, convertEditValues, createEditableAttributi, editEditableAttributi, makeAttributoCell, makeAttributoHeader, mutedSubheader, resetEditableAttributo, unsavedAttributoChanges, viewAttributoForm)

import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoName, AttributoType(..), AttributoValue(..), createAnnotatedAttributoMap, mapAttributo, retrieveAttributoValue, updateAttributoMap)
import Amarcord.Bootstrap exposing (icon)
import Amarcord.Html exposing (br_, input_, span_, strongText, td_)
import Amarcord.NumericRange exposing (NumericRange, emptyNumericRange, numericRangeToString, valueInRange)
import Amarcord.Sample exposing (Sample)
import Amarcord.Util exposing (collectResults, formatPosixDateTimeCompatible, formatPosixHumanFriendly, formatPosixTimeOfDayHumanFriendly)
import Dict exposing (Dict, get)
import FormatNumber exposing (format)
import FormatNumber.Locales exposing (Decimals(..), Locale, usLocale)
import Html exposing (Html, div, label, option, select, span, text)
import Html.Attributes exposing (checked, class, for, id, selected, step, style, type_, value)
import Html.Events exposing (onInput)
import Iso8601 exposing (toTime)
import List exposing (intersperse)
import List.Extra as List
import Maybe exposing (withDefault)
import Maybe.Extra exposing (isNothing, orElse, traverse, unwrap)
import Set
import String exposing (fromInt, join, split, toInt, trim)
import Time exposing (Zone, millisToPosix)
import Tuple exposing (first, mapFirst, second)


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
    }


makeAttributoCell : ViewAttributoValueProperties -> Zone -> Dict Int String -> AttributoMap AttributoValue -> Attributo AttributoType -> Html msg
makeAttributoCell props zone sampleIds attributiValues { name, type_ } =
    td_
        [ case retrieveAttributoValue name attributiValues of
            Nothing ->
                text ""

            Just v ->
                viewAttributoValue props zone sampleIds type_ v
        ]


viewAttributoValue : ViewAttributoValueProperties -> Zone -> Dict Int String -> AttributoType -> AttributoValue -> Html msg
viewAttributoValue props zone sampleIds type_ value =
    case value of
        ValueBoolean bool ->
            if bool then
                icon { name = "check-lg" }

            else
                text ""

        ValueInt int ->
            case type_ of
                SampleId ->
                    text <| withDefault (fromInt int) <| get int sampleIds

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
                    text (fromInt int)

        ValueString string ->
            text string

        ValueList attributoValues ->
            case type_ of
                List { subType } ->
                    case subType of
                        Number _ ->
                            span_ <| [ text "(" ] ++ List.map (viewAttributoValue props zone sampleIds subType) attributoValues ++ [ text ")" ]

                        String ->
                            span_ <| intersperse (text ",") <| List.map (viewAttributoValue props zone sampleIds subType) attributoValues

                        _ ->
                            text "unsupported list element type"

                _ ->
                    text "unsupported list type"

        ValueNumber float ->
            let
                locale : Locale
                locale =
                    { usLocale | decimals = Max 2 }
            in
            text (format locale float)


type EditStatus
    = Edited
    | Unchanged


type AttributoEditValue
    = EditValueInt String
    | EditValueDateTime String
    | EditValueBoolean Bool
    | EditValueSampleId (Maybe Int)
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
        }
    | EditValueChoice { choiceValues : List String, editValue : String }


type ValueUpdate
    = SetValue AttributoEditValue
    | IgnoreValue


type alias AttributoNameWithValueUpdate =
    { attributoName : AttributoName
    , valueUpdate : ValueUpdate
    }


viewAttributoForm : List (Sample Int a b) -> Attributo ( EditStatus, AttributoEditValue ) -> Html AttributoNameWithValueUpdate
viewAttributoForm samples a =
    case second a.type_ of
        EditValueString s ->
            div [ class "mb-3" ] <|
                [ label [ for ("attributo-" ++ a.name), class "form-label" ] [ text a.name ]
                , input_
                    [ type_ "text"
                    , class "form-control"
                    , id ("attributo-" ++ a.name)
                    , value s
                    , onInput (EditValueString >> SetValue >> AttributoNameWithValueUpdate a.name)
                    ]
                ]
                    ++ [ if a.description /= "" then
                            div [ class "form-text" ] [ text a.description ]

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
                        , onInput (AttributoNameWithValueUpdate a.name << SetValue << EditValueInt)
                        ]
                    ]
                ]
                    ++ [ if a.description /= "" then
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
                    , onInput (\newInput -> AttributoNameWithValueUpdate a.name <| SetValue <| EditValueList { l | editValue = newInput })
                    ]
                ]
                    ++ [ div [ class "form-text text-muted" ] [ strongText "Note on editing", text ": This is a list, but you can just insert the list elements, comma-separated in the text field." ] ]
                    ++ [ if a.description /= "" then
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
                    , onInput (\newInput -> AttributoNameWithValueUpdate a.name <| SetValue <| EditValueChoice { choiceValues = choiceValues, editValue = newInput })
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
                        , onInput (\newValue -> AttributoNameWithValueUpdate a.name <| SetValue <| EditValueNumber { n | editValue = newValue })
                        ]
                    , if inputGroupText == "" then
                        text ""

                      else
                        span [ class "input-group-text" ] [ text inputGroupText ]
                    ]
                ]
                    ++ [ if a.description /= "" then
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
                        , onInput (AttributoNameWithValueUpdate a.name << SetValue << EditValueDateTime)
                        ]
                    ]
                ]
                    ++ [ if a.description /= "" then
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
                        , onInput (always <| AttributoNameWithValueUpdate a.name <| SetValue <| EditValueBoolean (not x))
                        ]
                    , label [ for ("attributo-" ++ a.name), class "form-check-label" ] [ text a.name ]
                    ]
                        ++ [ if a.description /= "" then
                                div [ class "form-text" ] [ text a.description ]

                             else
                                text ""
                           ]
                ]

        EditValueSampleId selectedId ->
            let
                makeOption { id, name } =
                    option [ selected (Just id == selectedId), value (fromInt id) ] [ text name ]
            in
            div [ class "mb-3" ] <|
                [ label [ for ("attributo-" ++ a.name), class "form-label" ] [ text a.name ]
                , select
                    [ id ("attributo-" ++ a.name)
                    , class "form-select"
                    , onInput (AttributoNameWithValueUpdate a.name << SetValue << EditValueSampleId << String.toInt)
                    ]
                    (option [ selected (isNothing selectedId), value "0" ] [ text "«no value»" ] :: List.map makeOption samples)
                ]


type alias EditableAttributo =
    Attributo ( EditStatus, AttributoEditValue )


type alias EditableAttributi =
    List EditableAttributo


resetEditableAttributo : EditableAttributo -> EditableAttributo
resetEditableAttributo =
    mapAttributo (mapFirst (always Unchanged))


type alias EditableAttributiAndOriginal =
    { editableAttributi : EditableAttributi
    , originalAttributi : AttributoMap AttributoValue
    }


unsavedAttributoChanges : EditableAttributi -> Bool
unsavedAttributoChanges =
    List.any (\a -> first a.type_ == Edited)


createEditableAttributi : Zone -> List (Attributo AttributoType) -> AttributoMap AttributoValue -> EditableAttributiAndOriginal
createEditableAttributi zone attributi m =
    let
        -- two steps:
        -- 1. convert existing values into "best" manual values
        -- 2. add missing attributo and add as empty attributi, too
        -- Convert attributo metadata, as well as an attributo value, into an "editable attributo"
        convertToEditValues : String -> Attributo ( AttributoType, AttributoValue ) -> Dict String (Attributo ( EditStatus, AttributoEditValue )) -> Dict String (Attributo ( EditStatus, AttributoEditValue ))
        convertToEditValues attributoName a prev =
            case attributoValueToEditValue zone attributoName attributi (second a.type_) of
                Nothing ->
                    prev

                Just finishedEditValue ->
                    Dict.insert attributoName (mapAttributo (always ( Unchanged, finishedEditValue )) a) prev

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
            Dict.fromList <| List.map (\a -> ( a.name, mapAttributo (\type_ -> ( Unchanged, emptyEditValue type_ )) a )) <| missingAttributi
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

        SampleId ->
            EditValueSampleId Nothing

        String ->
            EditValueString ""

        List { subType, minLength, maxLength } ->
            EditValueList { subType = subType, minLength = minLength, maxLength = maxLength, editValue = "" }

        Number { range, suffix, standardUnit } ->
            EditValueNumber { range = range, suffix = suffix, standardUnit = standardUnit, editValue = "" }

        Choice { choiceValues } ->
            EditValueChoice { choiceValues = choiceValues, editValue = "" }


attributoValueToEditValue : Zone -> AttributoName -> List (Attributo AttributoType) -> AttributoValue -> Maybe AttributoEditValue
attributoValueToEditValue zone attributoName attributi value =
    let
        attributoFound : Maybe (Attributo AttributoType)
        attributoFound =
            List.find (\x -> x.name == attributoName) attributi

        --| This is used for the comma-separated "list of xy" input field
        attributoValueToString : AttributoValue -> String
        attributoValueToString x =
            case x of
                ValueBoolean bool ->
                    if bool then
                        "true"

                    else
                        "false"

                ValueInt int ->
                    String.fromInt int

                ValueString string ->
                    string

                ValueList attributoValues ->
                    join "," <| List.map attributoValueToString attributoValues

                ValueNumber float ->
                    String.fromFloat float

        convert : Attributo AttributoType -> Maybe AttributoEditValue
        convert a =
            case ( a.type_, value ) of
                ( Int, ValueInt x ) ->
                    Just (EditValueInt (String.fromInt x))

                ( SampleId, ValueInt x ) ->
                    Just (EditValueSampleId (Just x))

                ( String, ValueString x ) ->
                    Just (EditValueString x)

                ( DateTime, ValueString x ) ->
                    case toTime x of
                        Err _ ->
                            Nothing

                        Ok posix ->
                            Just (EditValueDateTime (formatPosixDateTimeCompatible zone posix))

                ( Choice { choiceValues }, ValueString x ) ->
                    Just (EditValueChoice { editValue = x, choiceValues = choiceValues })

                ( Number { range, suffix, standardUnit }, ValueNumber x ) ->
                    Just (EditValueNumber { range = range, suffix = suffix, standardUnit = standardUnit, editValue = String.fromFloat x })

                ( Number { range, suffix, standardUnit }, ValueInt x ) ->
                    Just (EditValueNumber { range = range, suffix = suffix, standardUnit = standardUnit, editValue = String.fromInt x })

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

                _ ->
                    Nothing

        -- Debug.log (Debug.toString x) Nothing
    in
    Maybe.andThen convert attributoFound


editEditableAttributi : EditableAttributi -> AttributoNameWithValueUpdate -> EditableAttributi
editEditableAttributi ea { attributoName, valueUpdate } =
    case valueUpdate of
        IgnoreValue ->
            ea

        SetValue newValue ->
            let
                -- Update the value if it exists (if it doesn't, this is weird!), and set the state to "edited"
                -- so when the object is stored, it's added to the manually edited attributes list
                updateValue : Attributo ( EditStatus, AttributoEditValue ) -> Attributo ( EditStatus, AttributoEditValue )
                updateValue x =
                    if x.name == attributoName then
                        mapAttributo (always ( Edited, newValue )) x

                    else
                        x
            in
            List.map updateValue ea


editValueToValue : AttributoEditValue -> Result String AttributoValue
editValueToValue x =
    case x of
        EditValueInt string ->
            unwrap (Err "not an integer") (Ok << ValueInt) <| toInt string

        EditValueBoolean boolValue ->
            Ok (ValueBoolean boolValue)

        EditValueDateTime string ->
            Ok (ValueString string)

        EditValueSampleId sampleId ->
            Ok (ValueInt (withDefault 0 sampleId))

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


convertEditValues : EditableAttributiAndOriginal -> Result (List ( AttributoName, String )) (AttributoMap AttributoValue)
convertEditValues { originalAttributi, editableAttributi } =
    let
        -- first, filter for manually edited values (the other ones we don't care about here)
        manuallyEdited : List ( AttributoName, AttributoEditValue )
        manuallyEdited =
            List.foldr
                (\attributo prev ->
                    if first attributo.type_ == Edited then
                        ( attributo.name, second attributo.type_ ) :: prev

                    else
                        prev
                )
                []
                editableAttributi

        -- Convert the edited value to the real value (with optional error)
        convertSingle : ( AttributoName, AttributoEditValue ) -> Result ( AttributoName, String ) ( AttributoName, AttributoValue )
        convertSingle ( name, v ) =
            case editValueToValue v of
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
            List.foldr (\( name, value ) -> updateAttributoMap name value) originalAttributi
    in
    Result.map combineWithOriginal converted
