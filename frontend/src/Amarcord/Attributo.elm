module Amarcord.Attributo exposing
    ( Attributo
    , AttributoMap
    , AttributoName
    , AttributoType(..)
    , AttributoValue(..)
    , attributoDecoder
    , attributoExposureTime
    , attributoIsChemicalId
    , attributoIsNumber
    , attributoIsString
    , attributoMapDecoder
    , attributoMapNames
    , attributoStarted
    , attributoStopped
    , attributoTypeDecoder
    , attributoValueDecoder
    , attributoValueToString
    , createAnnotatedAttributoMap
    , emptyAttributoMap
    , extractDateTime
    , extractInt
    , mapAttributo
    , mapAttributoMaybe
    , retrieveAttributoValue
    , retrieveDateTimeAttributoValue
    , retrieveFloatAttributoValue
    , updateAttributoMap
    )

import Amarcord.AssociatedTable exposing (AssociatedTable, associatedTableDecoder)
import Amarcord.JsonSchema exposing (JsonSchema(..), jsonSchemaDecoder)
import Amarcord.NumericRange exposing (NumericRange, rangeFromJsonSchema)
import Amarcord.Util exposing (resultToJsonDecoder)
import Dict exposing (Dict)
import Json.Decode as Decode
import List
import Maybe
import Maybe.Extra as MaybeExtra
import Result
import Time exposing (Posix, millisToPosix)


type AttributoValue
    = ValueInt Int
    | ValueString String
    | ValueList (List AttributoValue)
    | ValueNumber Float
    | ValueBoolean Bool
    | ValueNone


{-| This is used for the comma-separated "list of xy" input field
-}
attributoValueToString : AttributoValue -> String
attributoValueToString x =
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
            String.join "," <| List.map attributoValueToString attributoValues

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


type alias Attributo a =
    { name : AttributoName
    , description : String
    , group : String
    , associatedTable : AssociatedTable
    , type_ : a
    }


type alias AttributoMap a =
    Dict String a


attributoMapDecoder : Decode.Decoder (AttributoMap AttributoValue)
attributoMapDecoder =
    Decode.dict attributoValueDecoder


attributoMapNames : AttributoMap a -> List String
attributoMapNames =
    Dict.keys


mapAttributo : (a -> b) -> Attributo a -> Attributo b
mapAttributo f { name, description, group, associatedTable, type_ } =
    { name = name, description = description, group = group, associatedTable = associatedTable, type_ = f type_ }


mapAttributoMaybe : (a -> Maybe b) -> Attributo a -> Maybe (Attributo b)
mapAttributoMaybe f { name, description, group, associatedTable, type_ } =
    case f type_ of
        Nothing ->
            Nothing

        Just converted ->
            Just { name = name, description = description, group = group, associatedTable = associatedTable, type_ = converted }


attributoDecoder : Decode.Decoder a -> Decode.Decoder (Attributo a)
attributoDecoder typeDecoder =
    Decode.map5
        Attributo
        (Decode.field "name" Decode.string)
        (Decode.field "description" Decode.string)
        (Decode.field "group" Decode.string)
        (Decode.field "associatedTable" associatedTableDecoder)
        (Decode.field "type" typeDecoder)


jsonSchemaToAttributoType : JsonSchema -> Result String AttributoType
jsonSchemaToAttributoType x =
    case x of
        JsonSchemaNumber { minimum, exclusiveMinimum, exclusiveMaximum, maximum, suffix, format, tolerance, toleranceIsAbsolute } ->
            Ok
                (Number
                    { range = rangeFromJsonSchema minimum maximum exclusiveMinimum exclusiveMaximum
                    , standardUnit = format == Just "standard-unit"
                    , suffix = suffix
                    , tolerance = tolerance
                    , toleranceIsAbsolute = toleranceIsAbsolute
                    }
                )

        JsonSchemaInteger { format } ->
            case format of
                Just "chemical-id" ->
                    Ok ChemicalId

                Just "date-time" ->
                    Ok DateTime

                Just unknown ->
                    Err <| "invalid integer format \"" ++ unknown ++ "\""

                Nothing ->
                    Ok Int

        JsonSchemaArray { minItems, maxItems, items, format } ->
            case format of
                Just unknown ->
                    Err <| "unknown array format \"" ++ unknown ++ "\""

                Nothing ->
                    jsonSchemaToAttributoType items |> Result.map (\subType -> List { minLength = minItems, maxLength = maxItems, subType = subType })

        JsonSchemaString { enum } ->
            case enum of
                Nothing ->
                    Ok String

                Just choices ->
                    Ok (Choice { choiceValues = choices })

        JsonSchemaBoolean ->
            Ok Boolean


attributoTypeDecoder : Decode.Decoder AttributoType
attributoTypeDecoder =
    Decode.andThen (\x -> resultToJsonDecoder (jsonSchemaToAttributoType x)) jsonSchemaDecoder


emptyAttributoMap : AttributoMap a
emptyAttributoMap =
    Dict.empty


retrieveAttributoValue : AttributoName -> AttributoMap a -> Maybe a
retrieveAttributoValue name m =
    Dict.get name m


retrieveFloatAttributoValue : AttributoName -> AttributoMap AttributoValue -> Maybe Float
retrieveFloatAttributoValue name m =
    Maybe.andThen extractFloat <| retrieveAttributoValue name m


retrieveDateTimeAttributoValue : AttributoName -> AttributoMap AttributoValue -> Maybe Posix
retrieveDateTimeAttributoValue name m =
    Maybe.andThen extractDateTime <| retrieveAttributoValue name m


updateAttributoMap : AttributoName -> a -> AttributoMap a -> AttributoMap a
updateAttributoMap name value m =
    Dict.insert name value m


createAnnotatedAttributoMap : List (Attributo x) -> AttributoMap a -> Dict String (Attributo ( x, a ))
createAnnotatedAttributoMap attributi values =
    let
        bestValueMapper : Attributo x -> Maybe ( String, Attributo ( x, a ) )
        bestValueMapper a =
            Maybe.map (\value -> ( a.name, mapAttributo (\attributoType -> ( attributoType, value )) a )) <| retrieveAttributoValue a.name values

        bestValues : List ( String, Attributo ( x, a ) )
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


extractDateTime : AttributoValue -> Maybe Posix
extractDateTime =
    Maybe.map millisToPosix << extractInt


attributoStarted : AttributoName
attributoStarted =
    "started"


attributoStopped : AttributoName
attributoStopped =
    "stopped"


attributoExposureTime : AttributoName
attributoExposureTime =
    "exposure_time"
