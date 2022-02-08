module Amarcord.Attributo exposing
    ( Attributo
    , AttributoMap
    , AttributoName
    , AttributoType(..)
    , AttributoValue(..)
    , attributoDecoder
    , attributoIsNumber
    , attributoIsString
    , attributoMapDecoder
    , attributoTypeDecoder
    , attributoValueDecoder
    , createAnnotatedAttributoMap
    , emptyAttributoMap
    , encodeAttributoMap
    , extractDateTime
    , httpGetAndDecodeAttributi
    , jsonSchemaToAttributoType
    , mapAttributo
    , mapAttributoMaybe
    , retrieveAttributoValue
    , retrieveDateTimeAttributoValue
    , updateAttributoMap
    )

import Amarcord.AssociatedTable exposing (AssociatedTable, associatedTableDecoder)
import Amarcord.JsonSchema exposing (JsonSchema(..), jsonSchemaDecoder)
import Amarcord.NumericRange exposing (NumericRange, rangeFromJsonSchema)
import Amarcord.Util exposing (resultToJsonDecoder)
import Dict exposing (Dict)
import Http
import Json.Decode as Decode
import Json.Encode as Encode
import List
import Maybe
import Maybe.Extra as MaybeExtra
import Result
import String
import Time exposing (Posix, Zone, millisToPosix)


type AttributoValue
    = ValueInt Int
    | ValueString String
    | ValueList (List AttributoValue)
    | ValueNumber Float
    | ValueBoolean Bool


type AttributoType
    = Int
    | DateTime
    | SampleId
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
        }
    | Choice { choiceValues : List String }


attributoIsNumber : AttributoType -> Bool
attributoIsNumber x =
    case x of
        Number _ ->
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


type AttributoMap a
    = AttributoMap (Dict String a)


encodeAttributoMap : AttributoMap AttributoValue -> Encode.Value
encodeAttributoMap (AttributoMap d) =
    Encode.dict identity encodeAttributoValue d


encodeAttributoValue : AttributoValue -> Encode.Value
encodeAttributoValue x =
    case x of
        ValueInt int ->
            Encode.int int

        ValueString string ->
            Encode.string string

        ValueList attributoValues ->
            Encode.list encodeAttributoValue attributoValues

        ValueNumber float ->
            Encode.float float

        ValueBoolean bool ->
            Encode.bool bool


attributoValueDecoder : Decode.Decoder AttributoValue
attributoValueDecoder =
    Decode.oneOf
        [ Decode.map ValueString Decode.string
        , Decode.map ValueInt Decode.int
        , Decode.map ValueNumber Decode.float
        , Decode.map ValueBoolean Decode.bool
        , Decode.map ValueList (Decode.list (Decode.lazy (\_ -> attributoValueDecoder)))
        ]


attributoMapDecoder : Decode.Decoder (AttributoMap AttributoValue)
attributoMapDecoder =
    Decode.map AttributoMap <| Decode.dict attributoValueDecoder


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
        JsonSchemaNumber { minimum, exclusiveMinimum, exclusiveMaximum, maximum, suffix, format } ->
            Ok
                (Number
                    { range = rangeFromJsonSchema minimum exclusiveMinimum exclusiveMaximum maximum
                    , standardUnit = format == Just "standard-unit"
                    , suffix = suffix
                    }
                )

        JsonSchemaInteger { format } ->
            case format of
                Just "sample-id" ->
                    Ok SampleId

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
                    jsonSchemaToAttributoType items |> Result.andThen (\subType -> Ok (List { minLength = minItems, maxLength = maxItems, subType = subType }))

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


httpGetAndDecodeAttributi : (Result Http.Error (List (Attributo AttributoType)) -> msg) -> Cmd msg
httpGetAndDecodeAttributi f =
    Http.get
        { url = "/api/attributi"
        , expect = Http.expectJson f (Decode.field "attributi" (Decode.list (attributoDecoder attributoTypeDecoder)))
        }


emptyAttributoMap : AttributoMap a
emptyAttributoMap =
    AttributoMap Dict.empty


retrieveAttributoValue : AttributoName -> AttributoMap a -> Maybe a
retrieveAttributoValue name (AttributoMap m) =
    Dict.get name m


retrieveDateTimeAttributoValue : AttributoName -> AttributoMap AttributoValue -> Maybe Posix
retrieveDateTimeAttributoValue name m =
    Maybe.andThen extractDateTime <| retrieveAttributoValue name m


updateAttributoMap : AttributoName -> a -> AttributoMap a -> AttributoMap a
updateAttributoMap name value (AttributoMap m) =
    AttributoMap <| Dict.insert name value m


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


extractString : AttributoValue -> Maybe String
extractString x =
    case x of
        ValueString s ->
            Just s

        _ ->
            Nothing


extractInt : AttributoValue -> Maybe Int
extractInt x =
    case x of
        ValueInt s ->
            Just s

        _ ->
            Nothing


extractDateTime : AttributoValue -> Maybe Posix
extractDateTime =
    Maybe.map millisToPosix << extractInt
