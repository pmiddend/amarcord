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
    , encodeAttributoName
    , extractDateTime
    , fromAttributoName
    , httpGetAndDecodeAttributi
    , httpGetAttributi
    , jsonSchemaToAttributoType
    , makeAttributoCell
    , makeAttributoHeader
    , mapAttributo
    , mapAttributoMaybe
    , mutedSubheader
    , removeAttributoFromMap
    , retrieveAttributoValue
    , toAttributoName
    , updateAttributoMap
    )

import Amarcord.AssociatedTable exposing (AssociatedTable, associatedTableDecoder)
import Amarcord.Bootstrap exposing (icon)
import Amarcord.Html exposing (br_, span_, td_)
import Amarcord.JsonSchema exposing (JsonSchema(..), jsonSchemaDecoder)
import Amarcord.NumericRange exposing (NumericRange, rangeFromJsonSchema)
import Amarcord.Util exposing (formatPosixHumanFriendly, formatPosixTimeOfDayHumanFriendly, resultToJsonDecoder, toTimeMaybe)
import Dict exposing (Dict, get)
import FormatNumber exposing (format)
import FormatNumber.Locales exposing (Decimals(..), Locale, usLocale)
import Html exposing (Html, span, text)
import Html.Attributes exposing (class, style)
import Http
import Iso8601 exposing (toTime)
import Json.Decode as Decode
import Json.Encode as Encode
import List exposing (intersperse)
import Maybe exposing (withDefault)
import Maybe.Extra as MaybeExtra
import Result
import String exposing (fromInt)
import Time exposing (Posix, Zone)


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


type AttributoName
    = AttributoName String


encodeAttributoName : AttributoName -> Encode.Value
encodeAttributoName (AttributoName n) =
    Encode.string n


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
        (Decode.map AttributoName (Decode.field "name" Decode.string))
        (Decode.field "description" Decode.string)
        (Decode.field "group" Decode.string)
        (Decode.field "associatedTable" associatedTableDecoder)
        (Decode.field "type" typeDecoder)


httpGetAttributi : (Result Http.Error (List (Attributo JsonSchema)) -> msg) -> Cmd msg
httpGetAttributi f =
    Http.get
        { url = "/api/attributi"
        , expect = Http.expectJson f (Decode.field "attributi" (Decode.list (attributoDecoder jsonSchemaDecoder)))
        }


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

        JsonSchemaString { enum, format } ->
            case format of
                Nothing ->
                    case enum of
                        Nothing ->
                            Ok String

                        Just choices ->
                            Ok (Choice { choiceValues = choices })

                Just "date-time" ->
                    Ok DateTime

                Just unknown ->
                    Err <| "unknown string format \"" ++ unknown ++ "\""

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
retrieveAttributoValue (AttributoName name) (AttributoMap m) =
    Dict.get name m


fromAttributoName : AttributoName -> String
fromAttributoName (AttributoName n) =
    n


toAttributoName : String -> AttributoName
toAttributoName =
    AttributoName


removeAttributoFromMap : AttributoName -> AttributoMap a -> AttributoMap a
removeAttributoFromMap (AttributoName name) (AttributoMap m) =
    AttributoMap <| Dict.remove name m


updateAttributoMap : AttributoName -> a -> AttributoMap a -> AttributoMap a
updateAttributoMap (AttributoName name) value (AttributoMap m) =
    AttributoMap <| Dict.insert name value m


createAnnotatedAttributoMap : List (Attributo x) -> AttributoMap a -> Dict String (Attributo ( x, a ))
createAnnotatedAttributoMap attributi values =
    let
        bestValueMapper : Attributo x -> Maybe ( String, Attributo ( x, a ) )
        bestValueMapper a =
            Maybe.map (\value -> ( fromAttributoName a.name, mapAttributo (\attributoType -> ( attributoType, value )) a )) <| retrieveAttributoValue a.name values

        bestValues : List ( String, Attributo ( x, a ) )
        bestValues =
            MaybeExtra.values <| List.map bestValueMapper attributi
    in
    Dict.fromList bestValues


mutedSubheader : String.String -> Html.Html msg
mutedSubheader t =
    span [ class "text-muted fst-italic", style "font-size" "0.8rem" ] [ text t ]


makeAttributoHeader : Attributo AttributoType -> List (Html msg)
makeAttributoHeader a =
    case a.type_ of
        Number { suffix } ->
            case suffix of
                Just realSuffix ->
                    [ text (fromAttributoName a.name)
                    , br_
                    , mutedSubheader realSuffix
                    ]

                _ ->
                    [ text (fromAttributoName a.name) ]

        _ ->
            [ text (fromAttributoName a.name) ]


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

                _ ->
                    text (fromInt int)

        ValueString string ->
            case type_ of
                DateTime ->
                    case toTime string of
                        Err _ ->
                            text <| "Error converting time string " ++ string

                        Ok v ->
                            if props.shortDateTime then
                                text <| formatPosixTimeOfDayHumanFriendly zone v

                            else
                                text <| formatPosixHumanFriendly zone v

                _ ->
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


extractString : AttributoValue -> Maybe String
extractString x =
    case x of
        ValueString s ->
            Just s

        _ ->
            Nothing


extractDateTime : AttributoValue -> Maybe Posix
extractDateTime =
    Maybe.andThen toTimeMaybe << extractString
