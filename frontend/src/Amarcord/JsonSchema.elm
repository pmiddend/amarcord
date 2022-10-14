module Amarcord.JsonSchema exposing (..)

import Json.Decode as Decode
import Json.Encode as Encode
import Maybe.Extra exposing (unwrap)


type JsonSchema
    = JsonSchemaInteger { format : Maybe String }
    | JsonSchemaBoolean
    | JsonSchemaNumber
        { minimum : Maybe Float
        , maximum : Maybe Float
        , exclusiveMinimum : Maybe Float
        , exclusiveMaximum : Maybe Float
        , suffix : Maybe String
        , format : Maybe String
        , tolerance : Maybe Float
        , toleranceIsAbsolute : Bool
        }
    | JsonSchemaString { enum : Maybe (List String) }
    | JsonSchemaArray { minItems : Maybe Int, maxItems : Maybe Int, items : JsonSchema, format : Maybe String }


jsonSchemaDecoder : Decode.Decoder JsonSchema
jsonSchemaDecoder =
    let
        decodeWithType : String -> Decode.Decoder JsonSchema
        decodeWithType typeStr =
            case typeStr of
                "integer" ->
                    Decode.map (\format -> JsonSchemaInteger { format = format }) (Decode.maybe (Decode.field "format" Decode.string))

                "boolean" ->
                    Decode.succeed JsonSchemaBoolean

                "number" ->
                    Decode.map8
                        (\minimum maximum exclusiveMinimum exclusiveMaximum suffix format tolerance toleranceIsAbsolute ->
                            JsonSchemaNumber
                                { minimum = minimum
                                , maximum = maximum
                                , exclusiveMinimum = exclusiveMinimum
                                , exclusiveMaximum = exclusiveMaximum
                                , suffix = suffix
                                , format = format
                                , tolerance = tolerance
                                , toleranceIsAbsolute = Maybe.withDefault False toleranceIsAbsolute
                                }
                        )
                        (Decode.maybe (Decode.field "minimum" Decode.float))
                        (Decode.maybe (Decode.field "maximum" Decode.float))
                        (Decode.maybe (Decode.field "exclusiveMinimum" Decode.float))
                        (Decode.maybe (Decode.field "exclusiveMaximum" Decode.float))
                        (Decode.maybe (Decode.field "suffix" Decode.string))
                        (Decode.maybe (Decode.field "format" Decode.string))
                        (Decode.maybe (Decode.field "tolerance" Decode.float))
                        (Decode.maybe (Decode.field "toleranceIsAbsolute" Decode.bool))

                "string" ->
                    Decode.map (\enum -> JsonSchemaString { enum = enum })
                        (Decode.maybe (Decode.field "enum" (Decode.list Decode.string)))

                "array" ->
                    Decode.map4 (\items minItems maxItems format -> JsonSchemaArray { items = items, minItems = minItems, maxItems = maxItems, format = format })
                        (Decode.field "items" (Decode.lazy (\_ -> jsonSchemaDecoder)))
                        (Decode.maybe (Decode.field "minItems" Decode.int))
                        (Decode.maybe (Decode.field "maxItems" Decode.int))
                        (Decode.maybe (Decode.field "format" Decode.string))

                _ ->
                    Decode.fail ("invalid json schema type \"" ++ typeStr ++ "\"")
    in
    Decode.field "type" Decode.string |> Decode.andThen decodeWithType


maybeToList : (a -> b) -> Maybe a -> List b
maybeToList f =
    unwrap [] (\x -> [ f x ])


encodeString : a -> Maybe String -> List ( a, Encode.Value )
encodeString title =
    maybeToList (\y -> ( title, Encode.string y ))


encodeFloat : a -> Maybe Float -> List ( a, Encode.Value )
encodeFloat title =
    maybeToList (\y -> ( title, Encode.float y ))


encodeInt : a -> Maybe Int -> List ( a, Encode.Value )
encodeInt title =
    maybeToList (\y -> ( title, Encode.int y ))


encodeJsonSchema : JsonSchema -> Encode.Value
encodeJsonSchema x =
    case x of
        JsonSchemaInteger { format } ->
            Encode.object <|
                ( "type", Encode.string "integer" )
                    :: (case format of
                            Nothing ->
                                []

                            Just formatReal ->
                                [ ( "format", Encode.string formatReal ) ]
                       )

        JsonSchemaNumber { minimum, maximum, exclusiveMinimum, exclusiveMaximum, suffix, format, tolerance, toleranceIsAbsolute } ->
            let
                formatList =
                    encodeString "format" format

                suffixList =
                    encodeString "suffix" suffix

                rangeList =
                    encodeFloat "minimum" minimum ++ encodeFloat "maximum" maximum ++ encodeFloat "exclusiveMinimum" exclusiveMinimum ++ encodeFloat "exclusiveMaximum" exclusiveMaximum

                toleranceList =
                    ( "toleranceIsAbsolute", Encode.bool toleranceIsAbsolute ) :: encodeFloat "tolerance" tolerance
            in
            Encode.object (( "type", Encode.string "number" ) :: formatList ++ suffixList ++ rangeList ++ toleranceList)

        JsonSchemaArray { minItems, maxItems, format, items } ->
            Encode.object <| ( "type", Encode.string "array" ) :: encodeInt "minItems" minItems ++ encodeInt "maxItems" maxItems ++ encodeString "format" format ++ [ ( "items", encodeJsonSchema items ) ]

        JsonSchemaString { enum } ->
            let
                enumList =
                    case enum of
                        Nothing ->
                            []

                        Just [] ->
                            []

                        Just xs ->
                            [ ( "enum", Encode.list Encode.string xs ) ]
            in
            Encode.object <| ( "type", Encode.string "string" ) :: enumList

        JsonSchemaBoolean ->
            Encode.object <| [ ( "type", Encode.string "boolean" ) ]
