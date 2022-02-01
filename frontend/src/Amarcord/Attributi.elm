module Amarcord.Attributi exposing (Model, Msg, init, update, view)

import Amarcord.AssociatedTable exposing (AssociatedTable(..), associatedTableToString)
import Amarcord.Attributo exposing (Attributo, AttributoName, AttributoType(..), attributoIsNumber, attributoIsString, encodeAttributoName, fromAttributoName, httpGetAndDecodeAttributi, mapAttributo, mapAttributoMaybe, toAttributoName)
import Amarcord.Bootstrap exposing (AlertType(..), icon, makeAlert, showHttpError)
import Amarcord.Dialog as Dialog
import Amarcord.Html exposing (div_, form_, h4_, input_, p_, span_, strongText, tbody_, td_, th_, thead_, tr_)
import Amarcord.JsonSchema exposing (JsonSchema(..), encodeJsonSchema)
import Amarcord.NumericRange exposing (NumericRange(..), NumericRangeValue(..), coparseRange, emptyNumericRange, isEmptyNumericRange, numericRangeExclusiveMaximum, numericRangeExclusiveMinimum, numericRangeMaximum, numericRangeMinimum, numericRangeToString, parseRange)
import Amarcord.Parser exposing (deadEndsToHtml)
import Amarcord.UserError exposing (UserError, userErrorDecoder)
import Amarcord.Util exposing (httpDelete, httpPatch)
import Html exposing (..)
import Html.Attributes exposing (attribute, checked, class, disabled, for, id, placeholder, scope, selected, style, type_, value)
import Html.Events exposing (onClick, onInput)
import Http exposing (jsonBody)
import Json.Decode as Decode
import Json.Encode as Encode
import List exposing (singleton)
import List.Extra exposing (find)
import Maybe as Maybe exposing (andThen, withDefault)
import Maybe.Extra exposing (isJust, isNothing, unwrap)
import RemoteData exposing (RemoteData(..), fromResult, isLoading)
import Result exposing (mapError)
import Result.Extra as ResultExtra
import String exposing (fromInt, join, split, trim)


type NormalizedSuffix
    = NotNeeded
    | InvalidSuffix
    | ValidSuffix String


isInvalidSuffix : NormalizedSuffix -> Bool
isInvalidSuffix x =
    case x of
        InvalidSuffix ->
            True

        _ ->
            False


type AttributoTypeAug
    = AugSimple AttributoType
    | AugList { minLengthInput : String, maxLengthInput : String, subType : AttributoType }
    | AugNumber
        { rangeInput : String
        , suffixInput : String
        , suffixNormalized : NormalizedSuffix
        , standardUnit : Bool
        }
    | AugChoice { choiceValues : String }


type AttributoTypeEnum
    = ATInt
    | ATDateTime
    | ATBoolean
    | ATSample
    | ATString
    | ATList
    | ATNumber
    | ATChoice


attributoTypeEnumToString : AttributoTypeEnum -> String
attributoTypeEnumToString x =
    case x of
        ATInt ->
            "int"

        ATDateTime ->
            "date-time"

        ATSample ->
            "sample"

        ATString ->
            "string"

        ATList ->
            "list"

        ATNumber ->
            "number"

        ATChoice ->
            "choice"

        ATBoolean ->
            "yes/no"


attributoTypeToEnum : AttributoType -> AttributoTypeEnum
attributoTypeToEnum x =
    case x of
        Int ->
            ATInt

        DateTime ->
            ATDateTime

        SampleId ->
            ATSample

        String ->
            ATString

        List _ ->
            ATList

        Number _ ->
            ATNumber

        Choice _ ->
            ATChoice

        Boolean ->
            ATBoolean


initialAttributoAugType : AttributoTypeEnum -> AttributoTypeAug
initialAttributoAugType x =
    case x of
        ATNumber ->
            AugNumber
                { rangeInput = ""
                , suffixInput = ""
                , suffixNormalized = NotNeeded
                , standardUnit = False
                }

        ATChoice ->
            AugChoice
                { choiceValues = ""
                }

        ATInt ->
            AugSimple Int

        ATDateTime ->
            AugSimple DateTime

        ATSample ->
            AugSimple SampleId

        ATString ->
            AugSimple String

        ATList ->
            AugList { subType = String, minLengthInput = "", maxLengthInput = "" }

        ATBoolean ->
            AugSimple Boolean


attributoAugTypeToEnum : AttributoTypeAug -> AttributoTypeEnum
attributoAugTypeToEnum x =
    case x of
        AugNumber _ ->
            ATNumber

        AugChoice _ ->
            ATChoice

        AugSimple attributoType ->
            attributoTypeToEnum attributoType

        AugList _ ->
            ATList


emptyAugAttributo : Attributo AttributoTypeAug
emptyAugAttributo =
    { name = toAttributoName "", description = "", associatedTable = Sample, type_ = AugSimple String }


type alias ConversionFlags =
    { ignoreUnits : Bool
    }


attributoTypeToJsonSchema : AttributoType -> JsonSchema
attributoTypeToJsonSchema x =
    case x of
        Int ->
            JsonSchemaInteger { format = Nothing }

        DateTime ->
            JsonSchemaString { enum = Nothing, format = Just "date-time" }

        SampleId ->
            JsonSchemaInteger { format = Just "sample-id" }

        String ->
            JsonSchemaString { enum = Nothing, format = Nothing }

        List { subType, minLength, maxLength } ->
            JsonSchemaArray { minItems = minLength, maxItems = maxLength, items = attributoTypeToJsonSchema subType, format = Nothing }

        Number { range, suffix, standardUnit } ->
            JsonSchemaNumber
                { suffix = suffix
                , format =
                    if standardUnit then
                        Just "standard-unit"

                    else
                        Nothing
                , minimum = numericRangeMinimum range
                , exclusiveMinimum = numericRangeExclusiveMinimum range
                , maximum = numericRangeMaximum range
                , exclusiveMaximum = numericRangeExclusiveMaximum range
                }

        Choice { choiceValues } ->
            JsonSchemaString { format = Nothing, enum = Just choiceValues }

        Boolean ->
            JsonSchemaBoolean


attributoAugTypeFromType : AttributoType -> AttributoTypeAug
attributoAugTypeFromType x =
    case x of
        Number { range, suffix, standardUnit } ->
            AugNumber
                { rangeInput = coparseRange range
                , suffixInput = withDefault "" suffix
                , standardUnit = standardUnit
                , suffixNormalized = unwrap NotNeeded ValidSuffix suffix
                }

        Choice { choiceValues } ->
            AugChoice { choiceValues = join "," choiceValues }

        List { subType, minLength, maxLength } ->
            AugList
                { subType = subType
                , minLengthInput = unwrap "" String.fromInt minLength
                , maxLengthInput = unwrap "" String.fromInt maxLength
                }

        y ->
            AugSimple y


attributoAugTypeToType : AttributoTypeAug -> Maybe AttributoType
attributoAugTypeToType x =
    case x of
        AugSimple attributoType ->
            Just attributoType

        AugNumber n ->
            case parseRange n.rangeInput of
                Err _ ->
                    Nothing

                Ok range ->
                    Just
                        (Number
                            { range = range
                            , standardUnit = n.standardUnit
                            , suffix =
                                if n.suffixInput == "" then
                                    Nothing

                                else
                                    Just n.suffixInput
                            }
                        )

        AugChoice { choiceValues } ->
            Just <| Choice { choiceValues = List.map trim <| split "," choiceValues }

        AugList { minLengthInput, maxLengthInput, subType } ->
            -- This needs some explanation: we have three different cases here:
            -- 1. The length input is empty. That's fine, it's an optional value and this means it's missing
            -- 2. The length input has a valid number in it. That's also fine.
            -- 2. The length input has an invalid number in it. That's bad.
            --
            -- We encode this as Result () (Maybe Int), signifying errors, and optional "presentness"
            let
                parseOptionalLength : String.String -> Result.Result () (Maybe.Maybe Int)
                parseOptionalLength input =
                    if input == "" then
                        Ok Nothing

                    else
                        unwrap (Err ()) (Ok << Just) (String.toInt input)

                minLength =
                    parseOptionalLength minLengthInput

                maxLength =
                    parseOptionalLength maxLengthInput
            in
            Result.toMaybe <| Result.map2 (\min max -> List { subType = subType, minLength = min, maxLength = max }) minLength maxLength


type StandardUnitCheckResult
    = StandardUnitValid { input : String, normalized : String }
    | StandardUnitInvalid { input : String, error : String }


type Msg
    = AttributiReceived (Result Http.Error (List (Attributo AttributoType)))
    | AddAttributo
    | EditAttributoAssociatedTable AssociatedTable
    | EditAttributoName AttributoName
    | EditAttributoSubmit
    | EditAttributoCancel
    | EditAttributoType AttributoTypeEnum
    | EditAttributoDescription String
    | EditConversionFlags ConversionFlags
    | EditAttributoAugChange AttributoTypeAug
    | EditSubmitFinished (Result Http.Error (Maybe UserError))
    | CheckStandardUnitFinished (Result Http.Error StandardUnitCheckResult)
    | AskDelete AttributoName
    | ConfirmDelete AttributoName
    | CancelDelete
    | DeleteFinished (Result Http.Error (Maybe UserError))
    | InitiateEdit AttributoName


type alias Model =
    { attributiList : RemoteData Http.Error (List (Attributo AttributoType))
    , editAttributo : Maybe (Attributo AttributoTypeAug)
    , editAttributoOriginalName : Maybe AttributoName
    , conversionFlags : ConversionFlags
    , modifyRequest : RemoteData Http.Error ()
    , deleteRequest : RemoteData Http.Error ()
    , deleteModalOpen : Maybe AttributoName
    , unitValidationRequest : RemoteData Http.Error ()
    }


httpDeleteAttributo : AttributoName -> Cmd Msg
httpDeleteAttributo attributoName =
    httpDelete
        { url = "/api/attributi"
        , body = jsonBody (Encode.object [ ( "name", encodeAttributoName attributoName ) ])
        , expect = Http.expectJson DeleteFinished (Decode.maybe (Decode.field "error" userErrorDecoder))
        }


encodeAssociatedTable : AssociatedTable -> Encode.Value
encodeAssociatedTable x =
    case x of
        Run ->
            Encode.string "run"

        Sample ->
            Encode.string "sample"


encodeAttributo : (a -> Encode.Value) -> Attributo a -> Encode.Value
encodeAttributo typeEncoder a =
    Encode.object
        [ ( "name", encodeAttributoName a.name )
        , ( "description", Encode.string a.description )
        , ( "associatedTable", encodeAssociatedTable a.associatedTable )
        , ( "type", typeEncoder a.type_ )
        ]


httpCreateAttributo : Attributo JsonSchema -> Cmd Msg
httpCreateAttributo a =
    Http.post
        { url = "/api/attributi"
        , expect = Http.expectJson EditSubmitFinished (Decode.maybe (Decode.field "error" userErrorDecoder))
        , body = jsonBody (encodeAttributo encodeJsonSchema a)
        }


httpCheckStandardUnit : String -> Cmd Msg
httpCheckStandardUnit unit =
    let
        decodeCheckUnitResult input normalized error =
            case normalized of
                Nothing ->
                    StandardUnitInvalid { input = input, error = withDefault "unknown error" error }

                Just normalizedReal ->
                    StandardUnitValid { input = input, normalized = normalizedReal }
    in
    Http.post
        { url = "/api/unit"
        , expect = Http.expectJson CheckStandardUnitFinished (Decode.map3 decodeCheckUnitResult (Decode.field "input" Decode.string) (Decode.maybe (Decode.field "normalized" Decode.string)) (Decode.maybe (Decode.field "error" Decode.string)))
        , body = jsonBody (Encode.object [ ( "input", Encode.string unit ) ])
        }


encodeConversionFlags : ConversionFlags -> Encode.Value
encodeConversionFlags { ignoreUnits } =
    Encode.object [ ( "ignoreUnits", Encode.bool ignoreUnits ) ]


httpEditAttributo : ConversionFlags -> AttributoName -> Attributo JsonSchema -> Cmd Msg
httpEditAttributo conversionFlags nameBefore a =
    httpPatch
        { url = "/api/attributi"
        , expect = Http.expectJson EditSubmitFinished (Decode.maybe (Decode.field "error" userErrorDecoder))
        , body = jsonBody (Encode.object [ ( "newAttributo", encodeAttributo encodeJsonSchema a ), ( "nameBefore", encodeAttributoName nameBefore ), ( "conversionFlags", encodeConversionFlags conversionFlags ) ])
        }


init : () -> ( Model, Cmd Msg )
init _ =
    ( { attributiList = Loading
      , editAttributo = Nothing
      , editAttributoOriginalName = Nothing
      , conversionFlags = { ignoreUnits = False }
      , modifyRequest = NotAsked
      , deleteRequest = NotAsked
      , deleteModalOpen = Nothing
      , unitValidationRequest = NotAsked
      }
    , httpGetAndDecodeAttributi AttributiReceived
    )


attributoTypeToHtml : AttributoType -> List (Html msg)
attributoTypeToHtml x =
    case x of
        Int ->
            [ text "integer" ]

        DateTime ->
            [ text "date-time" ]

        SampleId ->
            [ text "sample ID" ]

        String ->
            [ text "string" ]

        Boolean ->
            [ text "boolean" ]

        List { subType, minLength, maxLength } ->
            case ( minLength, maxLength ) of
                ( Nothing, Nothing ) ->
                    text "list of " :: attributoTypeToHtml subType

                ( Just min, Nothing ) ->
                    (text <|
                        "list (min "
                            ++ fromInt min
                            ++ " element"
                            ++ (if min == 1 then
                                    ""

                                else
                                    "s"
                               )
                            ++ ") of "
                    )
                        :: attributoTypeToHtml subType

                ( Nothing, Just max ) ->
                    (text <|
                        "list (max "
                            ++ fromInt max
                            ++ " element"
                            ++ (if max == 1 then
                                    ""

                                else
                                    "s"
                               )
                            ++ ") of "
                    )
                        :: attributoTypeToHtml subType

                ( Just min, Just max ) ->
                    (text <| "list (between " ++ fromInt min ++ " and " ++ fromInt max ++ " elements) of ") :: attributoTypeToHtml subType

        Number { range, suffix } ->
            [ text <|
                withDefault "number" suffix
                    ++ (if isEmptyNumericRange range then
                            ""

                        else
                            " ∈ " ++ numericRangeToString range
                       )
            ]

        Choice { choiceValues } ->
            [ text <| "one of: " ++ join ", " choiceValues ]


viewAttributoRow : Attributo AttributoType -> Html Msg
viewAttributoRow { name, description, associatedTable, type_ } =
    tr_
        [ th [ scope "row", style "white-space" "nowrap" ] [ text (associatedTableToString associatedTable ++ "." ++ fromAttributoName name) ]
        , td_ [ text description ]
        , td [ style "white-space" "nowrap" ] (attributoTypeToHtml type_)
        , td [ style "white-space" "nowrap" ]
            [ button [ class "btn btn-sm btn-danger me-3", onClick (AskDelete name) ] [ icon { name = "trash" } ]
            , button [ class "btn btn-sm btn-info", onClick (InitiateEdit name) ] [ icon { name = "pencil-square" } ]
            ]
        ]


viewConversionFlags : ConversionFlags -> Html Msg
viewConversionFlags { ignoreUnits } =
    div [ class "mb-3" ]
        [ h5 [] [ text "Conversion parameters" ]
        , div [ class "form-check" ]
            [ input_
                [ class "form-check-input"
                , type_ "checkbox"
                , checked ignoreUnits
                , id "ignore-units"
                , onInput (\_ -> EditConversionFlags { ignoreUnits = not ignoreUnits })
                ]
            , label [ class "form-check-label", for "ignore-units" ] [ text "Ignore units" ]
            , div [ class "form-text" ] [ text "When changing an attributo which is a “standard unit”, ignore the unit while converting the actual values. For example, when you’re converting a “KHz” value to “Hz”, if this is not set, we would convert “1 KHz” to “1000 Hz”. If it’s set, we convert to “1Hz” instead." ]
            ]
        ]


viewTypeSpecificForm : AttributoTypeAug -> Html Msg
viewTypeSpecificForm x =
    case x of
        AugList { subType, minLengthInput, maxLengthInput } ->
            div []
                [ div [ class "mb-3 input-group" ]
                    [ span [ class "input-group-text" ] [ text "Minimum and maximum length" ]
                    , input_
                        [ type_ "number"
                        , class "form-control"
                        , id "minimum-length"
                        , placeholder "min"
                        , value minLengthInput
                        , onInput
                            (\newInt ->
                                EditAttributoAugChange
                                    (AugList
                                        { subType = subType, minLengthInput = newInt, maxLengthInput = maxLengthInput }
                                    )
                            )
                        ]
                    , input_
                        [ type_ "number"
                        , class "form-control"
                        , id "maximum-length"
                        , value maxLengthInput
                        , placeholder "max"
                        , onInput
                            (\newInt ->
                                EditAttributoAugChange
                                    (AugList
                                        { subType = subType, minLengthInput = minLengthInput, maxLengthInput = newInt }
                                    )
                            )
                        ]
                    ]
                , div [ class "mb-3 w-50" ]
                    [ label [ for "list-subtype", class "form-label" ] [ text "Element type" ]
                    , select
                        [ id "list-subtype"
                        , class "form-select"
                        , onInput
                            (\v ->
                                EditAttributoAugChange
                                    (AugList
                                        { minLengthInput = minLengthInput
                                        , maxLengthInput = maxLengthInput
                                        , subType =
                                            if v == "number" then
                                                Number { standardUnit = False, range = emptyNumericRange, suffix = Nothing }

                                            else
                                                String
                                        }
                                    )
                            )
                        ]
                        [ option [ value "number", selected (attributoIsNumber subType) ] [ text "number" ]
                        , option [ value "string", selected (attributoIsString subType) ] [ text "string" ]
                        ]
                    ]
                ]

        AugSimple _ ->
            text ""

        AugNumber { rangeInput, suffixInput, suffixNormalized, standardUnit } ->
            let
                rangeInputResult : Result (Html msg) NumericRange
                rangeInputResult =
                    if trim rangeInput == "" then
                        Ok emptyNumericRange

                    else
                        mapError (deadEndsToHtml True) (parseRange rangeInput)
            in
            div_
                [ div [ class "mb-3" ]
                    [ label [ class "form-label", for "range" ] [ text "Range" ]
                    , input_
                        [ type_ "text"
                        , class "form-control w-25"
                        , class (ResultExtra.unpack (\_ -> "is-invalid") (\_ -> "") rangeInputResult)
                        , id "range"
                        , value rangeInput
                        , onInput
                            (\newRange ->
                                EditAttributoAugChange
                                    (AugNumber
                                        { rangeInput = newRange
                                        , suffixInput = suffixInput
                                        , suffixNormalized = suffixNormalized
                                        , standardUnit = standardUnit
                                        }
                                    )
                            )
                        ]
                    , div [ class "form-text" ]
                        [ text "Optional range in interval notation. For example, “(oo, 3]” is “x ≤ 3” (“oo” representing the hard-to-type infinity symbol “∞” here), and “(-1, 10)” is “-1 < x < 10”." ]
                    , ResultExtra.unpack (\error -> div [ class "invalid-feedback" ] [ error ]) (\_ -> text "") rangeInputResult
                    ]
                , div [ class "mb-3" ]
                    [ label [ for "suffix", class "form-label" ] [ text "Suffix or unit" ]
                    , div [ class "input-group has-validation" ]
                        [ div [ class "input-group-text" ]
                            [ span [ class "me-1" ] [ text "Unit" ]
                            , input_
                                [ type_ "checkbox"
                                , class "form-check-input mt-0"
                                , id "standard-unit"
                                , checked standardUnit
                                , onInput
                                    (\_ ->
                                        EditAttributoAugChange
                                            (AugNumber
                                                { rangeInput = rangeInput
                                                , suffixInput = suffixInput
                                                , suffixNormalized = suffixNormalized
                                                , standardUnit = not standardUnit
                                                }
                                            )
                                    )
                                ]
                            ]
                        , input_
                            [ type_ "text"
                            , class
                                ("form-control"
                                    ++ (if isInvalidSuffix suffixNormalized then
                                            " is-invalid"

                                        else
                                            ""
                                       )
                                )
                            , id "suffix"
                            , value suffixInput
                            , onInput
                                (\newSuffix ->
                                    EditAttributoAugChange
                                        (AugNumber
                                            { rangeInput = rangeInput
                                            , suffixInput = newSuffix
                                            , suffixNormalized = suffixNormalized
                                            , standardUnit = standardUnit
                                            }
                                        )
                                )
                            ]
                        , case suffixNormalized of
                            InvalidSuffix ->
                                div [ class "invalid-feedback" ] [ text "Invalid unit" ]

                            _ ->
                                text ""
                        ]
                    , case suffixNormalized of
                        ValidSuffix normalized ->
                            div [ class "text-muted" ] [ text "Normalized unit: ", strong [] [ text normalized ] ]

                        _ ->
                            text ""
                    , div [ class "form-text" ] [ text "Can be either a non-standard suffix (say “gummibears”) or a standard unit like “MHz” or “N/m^2”." ]
                    ]
                ]

        AugChoice { choiceValues } ->
            div [ class "mb-3" ]
                [ label [ for "choice-input", class "form-label" ]
                    [ text "Choices"
                    ]
                , input_
                    [ type_ "text"
                    , class "form-control"
                    , id "choice-input"
                    , value choiceValues
                    , onInput (\newChoices -> EditAttributoAugChange (AugChoice { choiceValues = newChoices }))
                    ]
                , div [ class "form-text" ] [ text "Comma-separated list of string choices." ]
                ]


viewTypeForm : AttributoTypeAug -> Html Msg
viewTypeForm a =
    let
        makeTypeRadio : AttributoTypeEnum -> Html Msg
        makeTypeRadio e =
            div [ class "form-check form-check-inline" ]
                [ input_
                    [ id <| "attributo-type-" ++ attributoTypeEnumToString e
                    , class "form-check-input"
                    , type_ "radio"
                    , checked (attributoAugTypeToEnum a == e)
                    , onInput (\_ -> EditAttributoType e)
                    ]
                , label [ for <| "attributo-type-" ++ attributoTypeEnumToString e ] [ text (attributoTypeEnumToString e) ]
                ]
    in
    div []
        [ div []
            [ div [ class "form-check form-check-inline" ]
                (List.map makeTypeRadio [ ATInt, ATBoolean, ATDateTime, ATString, ATList, ATNumber, ATChoice ])
            , viewTypeSpecificForm a
            ]
        ]


nameInvalidReason : Maybe.Maybe AttributoName -> AttributoName -> List { a | name : AttributoName } -> Maybe String
nameInvalidReason originalName newName attributiList =
    if fromAttributoName newName == "" then
        Just "Name is mandatory"

    else if Just newName == originalName || List.all (\a -> a.name /= newName) attributiList then
        Nothing

    else
        Just "Name is not unique"


submitDisabled : Model -> List (Attributo AttributoType) -> Attributo AttributoTypeAug -> Bool
submitDisabled model attributiList attributo =
    let
        attributoDoesntValidate =
            case attributo.type_ of
                AugNumber { rangeInput, suffixNormalized } ->
                    ResultExtra.isErr (parseRange rangeInput) || isInvalidSuffix suffixNormalized

                _ ->
                    False
    in
    isJust (nameInvalidReason model.editAttributoOriginalName attributo.name attributiList) || isLoading model.modifyRequest || isLoading model.unitValidationRequest || attributoDoesntValidate


viewEditForm : Model -> List (Attributo AttributoType) -> Attributo AttributoTypeAug -> Html Msg
viewEditForm model attributiList attributo =
    form_
        [ h4_
            [ text
                (if isNothing model.editAttributoOriginalName then
                    "Add new attributo"

                 else
                    "Edit attributo"
                )
            ]
        , div [ class "mb-3" ]
            [ label [ for "associated-table", class "form-label" ] [ text "Attributo is for ..." ]
            , div_
                [ div [ class "form-check form-check-inline" ]
                    [ input_
                        [ id "associated-table-run"
                        , class "form-check-input"
                        , type_ "radio"
                        , checked (attributo.associatedTable == Run)
                        , onInput (\_ -> EditAttributoAssociatedTable Run)
                        ]
                    , label [ for "associated-table-run" ] [ text "Run" ]
                    ]
                , div [ class "form-check form-check-inline" ]
                    [ input_
                        [ id "associated-table-sample"
                        , class "form-check-input"
                        , type_ "radio"
                        , checked (attributo.associatedTable == Sample)
                        , onInput (\_ -> EditAttributoAssociatedTable Sample)
                        ]
                    , label [ for "associated-table-sample" ] [ text "Sample" ]
                    ]
                ]
            ]
        , div [ class "mb-3" ]
            [ label [ for "name", class "form-label" ] [ text "Name" ]
            , div [ class "w-75" ]
                [ input
                    [ type_ "text"
                    , class
                        ("form-control"
                            ++ (case nameInvalidReason model.editAttributoOriginalName attributo.name attributiList of
                                    Just _ ->
                                        " is-invalid"

                                    _ ->
                                        ""
                               )
                        )
                    , id "name"
                    , value (fromAttributoName attributo.name)
                    , onInput (EditAttributoName << toAttributoName)
                    ]
                    []
                ]
            , case nameInvalidReason model.editAttributoOriginalName attributo.name attributiList of
                Nothing ->
                    text ""

                Just reason ->
                    div [ class "invalid-feedback" ]
                        [ text reason
                        ]
            , div [ class "form-text" ]
                [ text "The name is displayed in the table headings and must be unique among all attributi. It cannot contain non-alphanumeric special characters except underscores."
                ]
            ]
        , div [ class "mb-3" ]
            [ label [ for "description", class "form-label" ] [ text "Description" ]
            , input
                [ type_ "text"
                , class "form-control"
                , id "description"
                , value attributo.description
                , onInput EditAttributoDescription
                ]
                []
            ]
        , div [ class "mb-3" ]
            [ label [ for "type", class "form-label" ] [ text "Type" ]
            , viewTypeForm attributo.type_
            ]
        , if isNothing model.editAttributoOriginalName then
            text ""

          else
            viewConversionFlags model.conversionFlags
        , button
            [ class "btn btn-primary me-3 mb-3"
            , onClick EditAttributoSubmit
            , type_ "button"
            , disabled (submitDisabled model attributiList attributo)
            ]
            [ icon { name = "plus-lg" }
            , text
                (if isNothing model.editAttributoOriginalName then
                    " Add new attributo"

                 else
                    " Confirm edit"
                )
            ]
        , button [ class "btn btn-secondary me-3 mb-3", type_ "button", onClick EditAttributoCancel ]
            [ icon { name = "x-lg" }, text " Cancel" ]
        ]


viewInner : Model -> List (Html Msg)
viewInner model =
    case model.attributiList of
        NotAsked ->
            singleton <| text ""

        Loading ->
            singleton <| text "Loading..."

        Failure e ->
            singleton <| makeAlert AlertDanger <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve Attributi" ] ] ++ showHttpError e

        Success attributiListReal ->
            let
                help =
                    div [ class "accordion mb-3" ]
                        [ div [ class "accordion-item" ]
                            [ h2 [ class "accordion-header" ]
                                [ button
                                    [ class "accordion-button btn-light"
                                    , type_ "button"
                                    , attribute "data-bs-toggle" "collapse"
                                    , attribute "data-bs-target" "#collapseHelp"
                                    ]
                                    [ i [ class "bi-question-circle me-3" ] []
                                    , text " What are attributi?"
                                    ]
                                ]
                            , div [ id "collapseHelp", class "accordion-collapse collapse" ]
                                [ div [ class "accordion-body" ]
                                    [ p_ [ text "Every experiment is a little different. Different detectors, different samples, you name it!" ]
                                    ]
                                ]
                            ]
                        ]

                prefix =
                    case model.editAttributo of
                        Nothing ->
                            button [ class "btn btn-primary", onClick AddAttributo ] [ icon { name = "plus-lg" }, text " Add attributo" ]

                        Just ea ->
                            viewEditForm model attributiListReal ea

                modifyRequestResult =
                    case model.modifyRequest of
                        NotAsked ->
                            text ""

                        Loading ->
                            p_ [ text "Request in progress..." ]

                        Failure e ->
                            div_ [ makeAlert AlertDanger (showHttpError e) ]

                        Success _ ->
                            div [ class "mt-3" ]
                                [ makeAlert AlertSuccess [ text "Request successful!" ]
                                ]

                deleteRequestResult =
                    case model.deleteRequest of
                        NotAsked ->
                            text ""

                        Loading ->
                            p_ [ text "Request in progress..." ]

                        Failure e ->
                            div_ [ makeAlert AlertDanger (showHttpError e) ]

                        Success _ ->
                            div [ class "mt-3" ]
                                [ makeAlert AlertSuccess [ text "Deletion successful!" ]
                                ]
            in
            help
                :: prefix
                :: modifyRequestResult
                :: deleteRequestResult
                :: [ table [ class "table table-striped" ]
                        [ thead_
                            [ tr_
                                [ th_ [ text "Name" ]
                                , th [ style "width" "100%" ] [ text "Description" ]
                                , th_ [ text "Type" ]
                                , th_ [ text "Actions" ]
                                ]
                            ]
                        , tbody_
                            (List.map viewAttributoRow attributiListReal)
                        ]
                   ]


view : Model -> Html Msg
view model =
    let
        maybeDeleteModal =
            case model.deleteModalOpen of
                Nothing ->
                    []

                Just attributoName ->
                    [ Dialog.view
                        (Just
                            { header = Nothing
                            , body = Just (span_ [ text "Really delete attributo ", strongText (fromAttributoName attributoName), text "?" ])
                            , closeMessage = Just CancelDelete
                            , containerClass = Nothing
                            , footer = Just (button [ class "btn btn-danger", onClick (ConfirmDelete attributoName) ] [ text "Really delete!" ])
                            }
                        )
                    ]
    in
    div [ class "container" ] (maybeDeleteModal ++ viewInner model)


augUnitAndSuffix : AttributoTypeAug -> Maybe ( Bool, String )
augUnitAndSuffix x =
    case x of
        AugNumber props ->
            Just ( props.standardUnit, props.suffixInput )

        _ ->
            Nothing


type EditNumberResult
    = DontDoAnything
    | ResetNormalizedStatus
    | CheckUnit String


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        -- Some of the conversion flags changed
        EditConversionFlags newConversionFlags ->
            ( { model | conversionFlags = newConversionFlags }, Cmd.none )

        -- The associated table selection changed for the currently edited object
        EditAttributoAssociatedTable newTable ->
            case model.editAttributo of
                Nothing ->
                    ( model, Cmd.none )

                Just editAttributo ->
                    ( { model | editAttributo = Just { editAttributo | associatedTable = newTable } }, Cmd.none )

        -- The name was changed
        EditAttributoName newName ->
            case model.editAttributo of
                Nothing ->
                    ( model, Cmd.none )

                Just editAttributo ->
                    ( { model | editAttributo = Just { editAttributo | name = newName } }, Cmd.none )

        -- The name description was changed
        EditAttributoDescription newDescription ->
            case model.editAttributo of
                Nothing ->
                    ( model, Cmd.none )

                Just editAttributo ->
                    ( { model | editAttributo = Just { editAttributo | description = newDescription } }, Cmd.none )

        -- The user pressed "Add new attributo"
        AddAttributo ->
            ( { model | editAttributo = Just emptyAugAttributo, editAttributoOriginalName = Nothing }, Cmd.none )

        -- The list of all attributi was received
        AttributiReceived x ->
            ( { model | attributiList = fromResult x }, Cmd.none )

        -- The attributo type changed (as a whole, e.g. from int to string or something)
        EditAttributoType attributoTypeEnum ->
            case model.editAttributo of
                Nothing ->
                    ( model, Cmd.none )

                Just editAttributo ->
                    ( { model | editAttributo = Just { editAttributo | type_ = initialAttributoAugType attributoTypeEnum } }, Cmd.none )

        -- Part of an attributo type was changed
        EditAttributoAugChange attributoTypeAug ->
            case model.editAttributo of
                Nothing ->
                    ( model, Cmd.none )

                -- This needs some explanation: if we have a standard unit, and this unit is changed, we need to check
                -- if the unit is actually valid.
                --
                -- So we send a validation request and wait for the result. HOWEVER, while this request is in flight,
                -- the unit could change again because the user continued typing.
                --
                -- In the code below, we first check if we currently have a validation request. If so, we don't do
                -- anything.
                --
                -- If we don't have a request in flight, we check if the user checked the "is standard unit" box, so we
                -- need to do an initial validation of the unit.
                --
                -- Similarly, if the user unchecks the "is standard unit" box, we need to clear the validation status.
                Just editAttributo ->
                    let
                        standardUnitChanged : AttributoTypeAug -> AttributoTypeAug -> EditNumberResult
                        standardUnitChanged x y =
                            -- Trivial case: we're just loading a request
                            if isLoading model.unitValidationRequest then
                                DontDoAnything

                            else
                                case ( augUnitAndSuffix x, augUnitAndSuffix y ) of
                                    -- We have a number and it has changed in some way
                                    ( Just ( standardUnitBefore, suffixBefore ), Just ( standardUnitAfter, suffixAfter ) ) ->
                                        -- We don't have a unit anymore
                                        if not standardUnitAfter then
                                            ResetNormalizedStatus
                                            -- We have a unit before and after, but the suffix didn't really change

                                        else if suffixBefore == suffixAfter && standardUnitBefore then
                                            DontDoAnything
                                            -- We have a unit before and after, and the suffix changed

                                        else
                                            CheckUnit suffixAfter

                                    -- We switched from a different type to the number type
                                    ( Nothing, Just ( standardUnitAfter, suffixAfter ) ) ->
                                        if not standardUnitAfter then
                                            ResetNormalizedStatus

                                        else
                                            CheckUnit suffixAfter

                                    -- The attributo isn't a number at all
                                    _ ->
                                        DontDoAnything

                        changeResult : EditNumberResult
                        changeResult =
                            standardUnitChanged editAttributo.type_ attributoTypeAug

                        newCmd : Cmd Msg
                        newCmd =
                            case changeResult of
                                CheckUnit newSuffix ->
                                    httpCheckStandardUnit newSuffix

                                _ ->
                                    Cmd.none

                        newAttributoType : AttributoTypeAug
                        newAttributoType =
                            case attributoTypeAug of
                                AugNumber { rangeInput, suffixInput, suffixNormalized, standardUnit } ->
                                    case changeResult of
                                        ResetNormalizedStatus ->
                                            AugNumber { rangeInput = rangeInput, suffixInput = suffixInput, suffixNormalized = NotNeeded, standardUnit = standardUnit }

                                        _ ->
                                            attributoTypeAug

                                _ ->
                                    attributoTypeAug
                    in
                    ( { model | editAttributo = Just { editAttributo | type_ = newAttributoType } }, newCmd )

        CheckStandardUnitFinished requestResult ->
            case model.editAttributo of
                Nothing ->
                    ( { model | unitValidationRequest = Success () }, Cmd.none )

                Just editAttributo ->
                    case editAttributo.type_ of
                        AugNumber { rangeInput, suffixInput, standardUnit } ->
                            -- It could be that we made the unit check request, then the user changed the standard unit
                            -- to false, so we don't need the request result anymore
                            if not standardUnit then
                                ( { model | unitValidationRequest = Success (), editAttributo = Just { editAttributo | type_ = AugNumber { rangeInput = rangeInput, suffixInput = suffixInput, standardUnit = standardUnit, suffixNormalized = NotNeeded } } }, Cmd.none )

                            else
                                case requestResult of
                                    Ok (StandardUnitValid { input, normalized }) ->
                                        if input == suffixInput then
                                            ( { model | unitValidationRequest = Success (), editAttributo = Just { editAttributo | type_ = AugNumber { rangeInput = rangeInput, suffixInput = suffixInput, standardUnit = standardUnit, suffixNormalized = ValidSuffix normalized } } }, Cmd.none )

                                        else
                                            ( model, httpCheckStandardUnit suffixInput )

                                    Ok (StandardUnitInvalid { input }) ->
                                        if input == suffixInput then
                                            ( { model | unitValidationRequest = Success (), editAttributo = Just { editAttributo | type_ = AugNumber { rangeInput = rangeInput, suffixInput = suffixInput, standardUnit = standardUnit, suffixNormalized = InvalidSuffix } } }, Cmd.none )

                                        else
                                            ( model, httpCheckStandardUnit suffixInput )

                                    Err x ->
                                        ( { model | unitValidationRequest = Failure x, editAttributo = Just { editAttributo | type_ = AugNumber { rangeInput = rangeInput, suffixInput = suffixInput, standardUnit = standardUnit, suffixNormalized = InvalidSuffix } } }, Cmd.none )

                        _ ->
                            ( { model | unitValidationRequest = Success () }, Cmd.none )

        -- The submit button was pressed and the attributo should be edited or created
        EditAttributoSubmit ->
            case model.editAttributo of
                Nothing ->
                    ( model, Cmd.none )

                Just editAttributo ->
                    case mapAttributoMaybe (\y -> andThen (\z -> Just (attributoTypeToJsonSchema z)) (attributoAugTypeToType y)) editAttributo of
                        Nothing ->
                            ( model, Cmd.none )

                        Just baseAttributo ->
                            case model.editAttributoOriginalName of
                                Nothing ->
                                    ( { model | modifyRequest = Loading }, httpCreateAttributo baseAttributo )

                                Just originalName ->
                                    ( { model | modifyRequest = Loading }, httpEditAttributo model.conversionFlags originalName baseAttributo )

        EditAttributoCancel ->
            ( { model | editAttributo = Nothing, editAttributoOriginalName = Nothing }, Cmd.none )

        EditSubmitFinished result ->
            case result of
                Err e ->
                    ( { model | modifyRequest = Failure e }, Cmd.none )

                Ok (Just userError) ->
                    ( { model | modifyRequest = Failure (Http.BadBody userError.title) }, Cmd.none )

                Ok Nothing ->
                    ( { model | modifyRequest = Success (), editAttributo = Nothing, editAttributoOriginalName = Nothing }, httpGetAndDecodeAttributi AttributiReceived )

        AskDelete attributoName ->
            ( { model | deleteModalOpen = Just attributoName }, Cmd.none )

        ConfirmDelete attributoName ->
            ( { model | deleteRequest = Loading, deleteModalOpen = Nothing }, httpDeleteAttributo attributoName )

        CancelDelete ->
            ( { model | deleteModalOpen = Nothing }, Cmd.none )

        DeleteFinished result ->
            case result of
                Err e ->
                    ( { model | deleteRequest = Failure e }, Cmd.none )

                Ok (Just userError) ->
                    ( { model | deleteRequest = Failure (Http.BadBody userError.title) }, Cmd.none )

                Ok Nothing ->
                    ( { model | deleteRequest = Success () }, httpGetAndDecodeAttributi AttributiReceived )

        InitiateEdit attributoName ->
            case model.attributiList of
                Success attributiListReal ->
                    ( { model | editAttributo = Maybe.map (mapAttributo attributoAugTypeFromType) (find (\attributo -> attributo.name == attributoName) attributiListReal), editAttributoOriginalName = Just attributoName }, Cmd.none )

                _ ->
                    ( model, Cmd.none )
