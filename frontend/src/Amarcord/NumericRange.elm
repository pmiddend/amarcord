module Amarcord.NumericRange exposing (..)

import Parser exposing ((|.), (|=), DeadEnd, Parser, backtrackable, end, float, map, oneOf, run, spaces, succeed, symbol)
import Result
import String exposing (fromFloat)


type NumericRangeValue
    = Missing
    | Inclusive Float
    | Exclusive Float


type NumericRange
    = NumericRange
        { minimum : NumericRangeValue
        , maximum : NumericRangeValue
        }


isEmptyNumericRange : NumericRange -> Bool
isEmptyNumericRange x =
    case x of
        NumericRange { minimum, maximum } ->
            case ( minimum, maximum ) of
                ( Missing, Missing ) ->
                    True

                _ ->
                    False


emptyNumericRange : NumericRange
emptyNumericRange =
    NumericRange { minimum = Missing, maximum = Missing }


numericRangeToString : NumericRange -> String
numericRangeToString n =
    case n of
        NumericRange { minimum, maximum } ->
            let
                prefix =
                    case minimum of
                        Missing ->
                            "(∞"

                        Inclusive x ->
                            "[" ++ fromFloat x

                        Exclusive x ->
                            "(" ++ fromFloat x

                suffix =
                    case maximum of
                        Missing ->
                            "∞)"

                        Inclusive x ->
                            fromFloat x ++ "]"

                        Exclusive x ->
                            fromFloat x ++ ")"
            in
            prefix ++ "," ++ suffix


fromMinAndMax : NumericRangeValue -> NumericRangeValue -> NumericRange
fromMinAndMax x y =
    NumericRange { minimum = x, maximum = y }


parseRange : String -> Result (List DeadEnd) NumericRange
parseRange x =
    let
        beginParser : Parser NumericRangeValue
        beginParser =
            oneOf
                [ map (\_ -> Missing) (succeed () |. symbol "(oo" |. spaces)
                , succeed Exclusive |. symbol "(" |. spaces |= float
                , succeed Inclusive |. symbol "[" |. spaces |= float
                ]

        endParser : Parser NumericRangeValue
        endParser =
            oneOf
                [ map (\_ -> Missing) (succeed () |. symbol "oo)")
                , map Exclusive (backtrackable (float |. spaces |. symbol ")"))
                , map Inclusive (float |. spaces |. symbol "]")
                ]

        totalParser =
            succeed fromMinAndMax |= beginParser |. symbol "," |. spaces |= endParser |. end
    in
    if x == "" then
        Ok emptyNumericRange

    else
        run totalParser x


numericRangeMinimum : NumericRange -> Maybe Float
numericRangeMinimum (NumericRange { minimum }) =
    case minimum of
        Missing ->
            Nothing

        Inclusive x ->
            Just x

        Exclusive _ ->
            Nothing


numericRangeMaximum : NumericRange -> Maybe Float
numericRangeMaximum (NumericRange { maximum }) =
    case maximum of
        Missing ->
            Nothing

        Inclusive x ->
            Just x

        Exclusive _ ->
            Nothing


numericRangeExclusiveMinimum : NumericRange -> Maybe Float
numericRangeExclusiveMinimum (NumericRange { minimum }) =
    case minimum of
        Missing ->
            Nothing

        Inclusive _ ->
            Nothing

        Exclusive x ->
            Just x


numericRangeExclusiveMaximum : NumericRange -> Maybe Float
numericRangeExclusiveMaximum (NumericRange { maximum }) =
    case maximum of
        Missing ->
            Nothing

        Inclusive _ ->
            Nothing

        Exclusive x ->
            Just x


coparseRange : NumericRange -> String
coparseRange n =
    case n of
        NumericRange { minimum, maximum } ->
            let
                prefix =
                    case minimum of
                        Missing ->
                            "(oo"

                        Inclusive x ->
                            "[" ++ fromFloat x

                        Exclusive x ->
                            "(" ++ fromFloat x

                suffix =
                    case maximum of
                        Missing ->
                            "oo)"

                        Inclusive x ->
                            fromFloat x ++ "]"

                        Exclusive x ->
                            fromFloat x ++ ")"
            in
            prefix ++ "," ++ suffix


rangeFromJsonSchema : Maybe Float -> Maybe Float -> Maybe Float -> Maybe Float -> NumericRange
rangeFromJsonSchema minimum maximum exclusiveMinimum exclusiveMaximum =
    NumericRange
        { minimum = rangeFromTwoFloats minimum exclusiveMinimum
        , maximum = rangeFromTwoFloats maximum exclusiveMaximum
        }


rangeFromTwoFloats : Maybe Float -> Maybe Float -> NumericRangeValue
rangeFromTwoFloats value exclusiveValue =
    case ( value, exclusiveValue ) of
        ( Nothing, Nothing ) ->
            Missing

        ( Just inclusive, _ ) ->
            Inclusive inclusive

        ( _, Just exclusive ) ->
            Exclusive exclusive


valueInRange : NumericRange -> Float -> Bool
valueInRange (NumericRange range) value =
    let
        leftSideWorks =
            case range.minimum of
                Missing ->
                    True

                Inclusive float ->
                    value >= float

                Exclusive float ->
                    value > float

        rightSideWorks =
            case range.maximum of
                Missing ->
                    True

                Inclusive float ->
                    value <= float

                Exclusive float ->
                    value < float
    in
    leftSideWorks && rightSideWorks
