module Amarcord.Attributo exposing
    ( Attributo
    , AttributoId
    , AttributoMap
    , AttributoName
    , AttributoType(..)
    , AttributoValue(..)
    , attributoExposureTime
    , attributoIsChemicalId
    , attributoIsNumber
    , attributoIsString
    , attributoMapDecoder
    , attributoMapIds
    , attributoMapToListOfAttributi
    , attributoTypeToSchemaArray
    , attributoTypeToSchemaBoolean
    , attributoTypeToSchemaInt
    , attributoTypeToSchemaNumber
    , attributoTypeToSchemaString
    , convertAttributoFromApi
    , convertAttributoMapFromApi
    , convertAttributoValueFromApi
    , createAnnotatedAttributoMap
    , emptyAttributoMap
    , extractInt
    , mapAttributo
    , mapAttributoMaybe
    , prettyPrintAttributoValue
    , retrieveAttributoValue
    , retrieveFloatAttributoValue
    , updateAttributoMap
    )

import Amarcord.AssociatedTable exposing (AssociatedTable, associatedTableFromApi)
import Amarcord.NumericRange as NumericRange exposing (NumericRange, emptyNumericRange, numericRangeExclusiveMaximum, numericRangeExclusiveMinimum, numericRangeMaximum, numericRangeMinimum)
import Api.Data exposing (JSONSchemaArray, JSONSchemaArraySubtype(..), JSONSchemaArrayType(..), JSONSchemaBoolean, JSONSchemaBooleanType(..), JSONSchemaInteger, JSONSchemaIntegerFormat(..), JSONSchemaIntegerType(..), JSONSchemaNumber, JSONSchemaNumberFormat(..), JSONSchemaNumberType(..), JSONSchemaString, JSONSchemaStringType(..), JsonAttributo, JsonAttributoValue)
import Dict exposing (Dict)
import Json.Decode as Decode
import Json.Decode.Extra as JsonExtra
import List exposing (filterMap)
import Maybe
import Maybe.Extra as MaybeExtra


type AttributoValue
    = ValueInt Int
    | ValueString String
    | ValueList (List AttributoValue)
    | ValueNumber Float
    | ValueBoolean Bool
    | ValueNone


attributoValueToBool : AttributoValue -> Maybe Bool
attributoValueToBool x =
    case x of
        ValueBoolean b ->
            Just b

        _ ->
            Nothing


attributoValueToInt : AttributoValue -> Maybe Int
attributoValueToInt x =
    case x of
        ValueInt b ->
            Just b

        _ ->
            Nothing


attributoValueToFloat : AttributoValue -> Maybe Float
attributoValueToFloat x =
    case x of
        ValueNumber b ->
            Just b

        _ ->
            Nothing


attributoValueToString : AttributoValue -> Maybe String
attributoValueToString x =
    case x of
        ValueString b ->
            Just b

        _ ->
            Nothing


attributoValueToListOfBool : AttributoValue -> Maybe (List Bool)
attributoValueToListOfBool x =
    case x of
        ValueList xs ->
            Just (filterMap attributoValueToBool xs)

        _ ->
            Nothing


attributoValueToListOfFloat : AttributoValue -> Maybe (List Float)
attributoValueToListOfFloat x =
    case x of
        ValueList xs ->
            Just (filterMap attributoValueToFloat xs)

        _ ->
            Nothing


attributoValueToListOfString : AttributoValue -> Maybe (List String)
attributoValueToListOfString x =
    case x of
        ValueList xs ->
            Just (filterMap attributoValueToString xs)

        _ ->
            Nothing


attributoValueToJson : Int -> AttributoValue -> JsonAttributoValue
attributoValueToJson aid a =
    { attributoId = aid
    , attributoValueBool = attributoValueToBool a
    , attributoValueFloat = attributoValueToFloat a
    , attributoValueInt = attributoValueToInt a
    , attributoValueListBool = attributoValueToListOfBool a
    , attributoValueListFloat = attributoValueToListOfFloat a
    , attributoValueListStr = attributoValueToListOfString a
    , attributoValueStr = attributoValueToString a
    }


{-| This is used for the comma-separated "list of xy" input field
-}
prettyPrintAttributoValue : AttributoValue -> String
prettyPrintAttributoValue x =
    case x of
        ValueNone ->
            ""

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
            String.join "," <| List.map prettyPrintAttributoValue attributoValues

        ValueNumber float ->
            String.fromFloat float


attributoValueDecoder : Decode.Decoder AttributoValue
attributoValueDecoder =
    Decode.oneOf
        [ Decode.map ValueString Decode.string
        , Decode.map ValueInt Decode.int
        , Decode.map ValueNumber Decode.float
        , Decode.map ValueBoolean Decode.bool
        , Decode.null ValueNone
        , Decode.map ValueList (Decode.list (Decode.lazy (\_ -> attributoValueDecoder)))
        ]


type AttributoType
    = Int
    | DateTime
    | ChemicalId
    | String
    | Boolean
    | List
        { subType : AttributoType
        , minLength : Maybe Int
        , maxLength : Maybe Int
        }
    | Number
        { range : NumericRange
        , suffix : Maybe String
        , standardUnit : Bool
        , tolerance : Maybe Float
        , toleranceIsAbsolute : Bool
        }
    | Choice { choiceValues : List String }


attributoIsNumber : AttributoType -> Bool
attributoIsNumber x =
    case x of
        Number _ ->
            True

        _ ->
            False


attributoIsChemicalId : AttributoType -> Bool
attributoIsChemicalId x =
    case x of
        ChemicalId ->
            True

        _ ->
            False


attributoIsString : AttributoType -> Bool
attributoIsString x =
    case x of
        String ->
            True

        _ ->
            False


type alias AttributoName =
    String


type alias AttributoId =
    Int


type alias Attributo a =
    { id : AttributoId
    , name : AttributoName
    , description : String
    , group : String
    , associatedTable : AssociatedTable
    , type_ : a
    }


type alias AttributoMap a =
    Dict AttributoId a


generalAttributoMapDecoder : Decode.Decoder value -> Decode.Decoder (AttributoMap value)
generalAttributoMapDecoder valueDecoder =
    let
        transducer : String -> value -> Result String (AttributoMap value) -> Result String (AttributoMap value)
        transducer stringKey value previousDict =
            case previousDict of
                Err e ->
                    Err e

                Ok v ->
                    case String.toInt stringKey of
                        Nothing ->
                            Err ("couldn't convert attributo ID " ++ stringKey ++ " to integer")

                        Just intKey ->
                            Ok <| Dict.insert intKey value v
    in
    Decode.dict valueDecoder |> Decode.andThen (JsonExtra.fromResult << Dict.foldr transducer (Ok Dict.empty))


attributoMapDecoder : Decode.Decoder (AttributoMap AttributoValue)
attributoMapDecoder =
    generalAttributoMapDecoder attributoValueDecoder


attributoMapIds : AttributoMap a -> List AttributoId
attributoMapIds =
    Dict.keys


mapAttributo : (a -> b) -> Attributo a -> Attributo b
mapAttributo f { id, name, description, group, associatedTable, type_ } =
    { id = id
    , name = name
    , description = description
    , group = group
    , associatedTable = associatedTable
    , type_ = f type_
    }


mapAttributoMaybe : (a -> Maybe b) -> Attributo a -> Maybe (Attributo b)
mapAttributoMaybe f { id, name, description, group, associatedTable, type_ } =
    f type_
        |> Maybe.map
            (\converted ->
                { id = id
                , name = name
                , description = description
                , group = group
                , associatedTable = associatedTable
                , type_ = converted
                }
            )


emptyAttributoMap : AttributoMap a
emptyAttributoMap =
    Dict.empty


retrieveAttributoValue : AttributoId -> AttributoMap a -> Maybe a
retrieveAttributoValue =
    Dict.get


retrieveFloatAttributoValue : AttributoId -> AttributoMap AttributoValue -> Maybe Float
retrieveFloatAttributoValue id m =
    Maybe.andThen extractFloat <| retrieveAttributoValue id m


updateAttributoMap : AttributoId -> a -> AttributoMap a -> AttributoMap a
updateAttributoMap id value m =
    Dict.insert id value m


createAnnotatedAttributoMap : List (Attributo x) -> AttributoMap a -> Dict AttributoId (Attributo ( x, a ))
createAnnotatedAttributoMap attributi values =
    let
        bestValueMapper : Attributo x -> Maybe ( AttributoId, Attributo ( x, a ) )
        bestValueMapper a =
            Maybe.map (\value -> ( a.id, mapAttributo (\attributoType -> ( attributoType, value )) a )) <| retrieveAttributoValue a.id values

        bestValues : List ( AttributoId, Attributo ( x, a ) )
        bestValues =
            MaybeExtra.values <| List.map bestValueMapper attributi
    in
    Dict.fromList bestValues


extractInt : AttributoValue -> Maybe Int
extractInt x =
    case x of
        ValueInt s ->
            Just s

        _ ->
            Nothing


extractFloat : AttributoValue -> Maybe Float
extractFloat x =
    case x of
        ValueNumber s ->
            Just s

        _ ->
            Nothing


attributoExposureTime : AttributoName
attributoExposureTime =
    "exposure_time"


convertAttributoTypeFromApi : Api.Data.JsonAttributo -> AttributoType
convertAttributoTypeFromApi { attributoTypeInteger, attributoTypeNumber, attributoTypeString, attributoTypeArray } =
    case attributoTypeInteger of
        Just { format } ->
            if format == Just JSONSchemaIntegerFormatDateTime then
                DateTime

            else if format == Just JSONSchemaIntegerFormatChemicalId then
                ChemicalId

            else
                Int

        Nothing ->
            case attributoTypeNumber of
                Just params ->
                    Number
                        { suffix = params.suffix
                        , tolerance = params.tolerance
                        , toleranceIsAbsolute = Maybe.withDefault True params.toleranceIsAbsolute
                        , standardUnit = params.format == Just JSONSchemaNumberFormatStandardUnit
                        , range =
                            NumericRange.rangeFromJsonSchema
                                params.minimum
                                params.maximum
                                params.exclusiveMinimum
                                params.exclusiveMaximum
                        }

                Nothing ->
                    case attributoTypeString of
                        Just params ->
                            case params.enum of
                                Nothing ->
                                    String

                                Just choices ->
                                    Choice { choiceValues = choices }

                        Nothing ->
                            case attributoTypeArray of
                                Just params ->
                                    List
                                        { minLength = params.minItems
                                        , maxLength = params.maxItems
                                        , subType =
                                            case params.itemType of
                                                JSONSchemaArraySubtypeString ->
                                                    String

                                                JSONSchemaArraySubtypeNumber ->
                                                    Number
                                                        { range = emptyNumericRange
                                                        , suffix = Nothing
                                                        , standardUnit = False
                                                        , tolerance = Nothing
                                                        , toleranceIsAbsolute = False
                                                        }

                                                JSONSchemaArraySubtypeBool ->
                                                    Boolean
                                        }

                                Nothing ->
                                    Boolean


convertAttributoFromApi : JsonAttributo -> Attributo AttributoType
convertAttributoFromApi a =
    { id = a.id
    , name = a.name
    , description = a.description
    , group = a.group
    , associatedTable = associatedTableFromApi a.associatedTable
    , type_ = convertAttributoTypeFromApi a
    }


convertAttributoValueFromApi : JsonAttributoValue -> AttributoValue
convertAttributoValueFromApi v =
    MaybeExtra.orList
        [ Maybe.map ValueBoolean v.attributoValueBool
        , Maybe.map ValueNumber v.attributoValueFloat
        , Maybe.map ValueInt v.attributoValueInt
        , Maybe.map (ValueList << List.map ValueBoolean) v.attributoValueListBool
        , Maybe.map (ValueList << List.map ValueNumber) v.attributoValueListFloat
        , Maybe.map (ValueList << List.map ValueString) v.attributoValueListStr
        , Maybe.map ValueString v.attributoValueStr
        ]
        |> Maybe.withDefault ValueNone


convertAttributoMapFromApi : List JsonAttributoValue -> AttributoMap AttributoValue
convertAttributoMapFromApi =
    List.foldr
        (\newAttributo -> Dict.insert newAttributo.attributoId (convertAttributoValueFromApi newAttributo))
        Dict.empty


attributoMapToListOfAttributi : AttributoMap AttributoValue -> List JsonAttributoValue
attributoMapToListOfAttributi =
    Dict.foldl (\attributoId attributoValue oldList -> attributoValueToJson attributoId attributoValue :: oldList) []


attributoTypeToSchemaInt : AttributoType -> Maybe JSONSchemaInteger
attributoTypeToSchemaInt x =
    case x of
        Int ->
            Just { type_ = JSONSchemaIntegerTypeInteger, format = Nothing }

        _ ->
            Nothing


attributoTypeToSchemaBoolean : AttributoType -> Maybe JSONSchemaBoolean
attributoTypeToSchemaBoolean x =
    case x of
        Boolean ->
            Just { type_ = JSONSchemaBooleanTypeBoolean }

        _ ->
            Nothing


attributoTypeToSchemaNumber : AttributoType -> Maybe JSONSchemaNumber
attributoTypeToSchemaNumber x =
    case x of
        Number { range, suffix, tolerance, toleranceIsAbsolute, standardUnit } ->
            Just
                { type_ = JSONSchemaNumberTypeNumber
                , minimum = numericRangeMinimum range
                , maximum = numericRangeMaximum range
                , exclusiveMinimum = numericRangeExclusiveMinimum range
                , exclusiveMaximum = numericRangeExclusiveMaximum range
                , suffix = suffix
                , format =
                    if standardUnit then
                        Just JSONSchemaNumberFormatStandardUnit

                    else
                        Nothing
                , tolerance = tolerance
                , toleranceIsAbsolute = Just toleranceIsAbsolute
                }

        _ ->
            Nothing


attributoTypeToSchemaString : AttributoType -> Maybe JSONSchemaString
attributoTypeToSchemaString x =
    case x of
        String ->
            Just
                { type_ = JSONSchemaStringTypeString
                , enum = Nothing
                }

        Choice { choiceValues } ->
            Just
                { type_ = JSONSchemaStringTypeString
                , enum = Just choiceValues
                }

        _ ->
            Nothing


attributoTypeToSchemaArray : AttributoType -> Maybe JSONSchemaArray
attributoTypeToSchemaArray x =
    case x of
        List { minLength, maxLength, subType } ->
            Just
                { type_ = JSONSchemaArrayTypeArray
                , minItems = minLength
                , maxItems = maxLength
                , itemType =
                    case subType of
                        String ->
                            JSONSchemaArraySubtypeString

                        Choice _ ->
                            JSONSchemaArraySubtypeString

                        Number _ ->
                            JSONSchemaArraySubtypeNumber

                        _ ->
                            JSONSchemaArraySubtypeBool
                }

        _ ->
            Nothing
