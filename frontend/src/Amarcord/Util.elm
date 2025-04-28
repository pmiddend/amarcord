module Amarcord.Util exposing (..)

import Browser.Dom
import List exposing (foldr)
import List.Extra as ListExtra
import Maybe.Extra exposing (isJust)
import Parser exposing ((|.), (|=), DeadEnd, Problem(..), run)
import String exposing (fromInt, padLeft)
import Task
import Time exposing (Month(..), Posix, Zone, here, now, posixToMillis, toDay, toHour, toMinute, toMonth, toSecond, toYear)
import Time.Extra exposing (partsToPosix)
import Tuple exposing (first, second)


collectResults : List (Result e b) -> Result (List e) (List b)
collectResults xs =
    let
        g newElement previousElements =
            case newElement of
                Ok newSuccess ->
                    case previousElements of
                        Ok previousElementsOk ->
                            Ok (newSuccess :: previousElementsOk)

                        Err previousErrors ->
                            Err previousErrors

                Err newError ->
                    case previousElements of
                        Ok _ ->
                            Err [ newError ]

                        Err previousErrors ->
                            Err (newError :: previousErrors)
    in
    foldr g (Ok []) xs


monthToNumericString : Month -> String
monthToNumericString x =
    case x of
        Jan ->
            "01"

        Feb ->
            "02"

        Mar ->
            "03"

        Apr ->
            "04"

        May ->
            "05"

        Jun ->
            "06"

        Jul ->
            "07"

        Aug ->
            "08"

        Sep ->
            "09"

        Oct ->
            "10"

        Nov ->
            "11"

        Dec ->
            "12"


formatPosixDateTimeCompatible : Zone -> Posix -> String
formatPosixDateTimeCompatible zone posix =
    let
        year =
            String.fromInt (toYear zone posix)

        month =
            monthToNumericString <| toMonth zone posix

        day =
            padLeft 2 '0' <| String.fromInt <| toDay zone posix

        hour =
            padLeft 2 '0' <| String.fromInt <| toHour zone posix

        minute =
            padLeft 2 '0' <| String.fromInt <| toMinute zone posix
    in
    year ++ "-" ++ month ++ "-" ++ day ++ "T" ++ hour ++ ":" ++ minute


secondsDiffHumanFriendly : Int -> String
secondsDiffHumanFriendly diffSeconds =
    millisDiffHumanFriendly (diffSeconds * 1000)


millisDiffHumanFriendly : Int -> String
millisDiffHumanFriendly diffMillis =
    let
        diffSeconds =
            diffMillis // 1000
    in
    if diffSeconds < 60 then
        fromInt diffSeconds ++ " seconds"

    else
        let
            diffMinutes =
                diffSeconds // 60
        in
        if diffMinutes == 1 then
            "1 minute"

        else if diffMinutes > 60 then
            let
                diffHours =
                    diffMinutes // 60

                diffRemMinutes =
                    modBy 60 diffMinutes
            in
            fromInt diffHours ++ " hours, " ++ fromInt diffRemMinutes ++ " minutes"

        else
            fromInt diffMinutes ++ " minutes"


posixDiffHumanFriendly : Posix -> Posix -> String
posixDiffHumanFriendly p1 p2 =
    millisDiffHumanFriendly <| abs (posixToMillis p1 - posixToMillis p2)


formatPosixTimeOfDayHumanFriendly : Zone -> Posix -> String
formatPosixTimeOfDayHumanFriendly zone posix =
    let
        hour =
            padLeft 2 '0' <| String.fromInt <| toHour zone posix

        minute =
            padLeft 2 '0' <| String.fromInt <| toMinute zone posix

        second =
            padLeft 2 '0' <| String.fromInt <| toSecond zone posix
    in
    hour ++ ":" ++ minute ++ ":" ++ second


formatDateHumanFriendly : Zone -> Posix -> String
formatDateHumanFriendly zone posix =
    let
        year =
            String.fromInt (toYear zone posix)

        month =
            monthToNumericString <| toMonth zone posix

        day =
            padLeft 2 '0' <| String.fromInt <| toDay zone posix
    in
    year ++ "/" ++ month ++ "/" ++ day


formatPosixHumanFriendly : Zone -> Posix -> String
formatPosixHumanFriendly zone posix =
    let
        year =
            String.fromInt (toYear zone posix)

        month =
            monthToNumericString <| toMonth zone posix

        day =
            padLeft 2 '0' <| String.fromInt <| toDay zone posix

        hour =
            padLeft 2 '0' <| String.fromInt <| toHour zone posix

        minute =
            padLeft 2 '0' <| String.fromInt <| toMinute zone posix

        second =
            padLeft 2 '0' <| String.fromInt <| toSecond zone posix
    in
    year ++ "/" ++ month ++ "/" ++ day ++ " " ++ hour ++ ":" ++ minute ++ ":" ++ second


posixBefore : Posix -> Posix -> Bool
posixBefore a b =
    posixToMillis a < posixToMillis b


type alias HereAndNow =
    { zone : Zone
    , now : Posix
    }


retrieveHereAndNow : Task.Task x HereAndNow
retrieveHereAndNow =
    Task.map2 HereAndNow here now


scrollToTop : (() -> msg) -> Cmd msg
scrollToTop f =
    Task.perform f <| Browser.Dom.setViewport 0 0


posixDiffMillis : Posix -> Posix -> Int
posixDiffMillis after before =
    posixToMillis after - posixToMillis before


posixDiffMinutes : Posix -> Posix -> Int
posixDiffMinutes after before =
    posixDiffMillis after before // 1000 // 60


type alias LocalDateTime =
    { year : Int
    , month : Month
    , day : Int
    , hour : Int
    , minute : Int
    }


parserZeroPaddedInt : Parser.Parser Int
parserZeroPaddedInt =
    Parser.getChompedString (Parser.chompWhile Char.isDigit)
        |> Parser.andThen
            (\x ->
                case String.toInt x of
                    Nothing ->
                        Parser.problem <| "invalid integer: " ++ x

                    Just i ->
                        Parser.succeed i
            )


monthFromInt : Int -> Parser.Parser Month
monthFromInt x =
    case x of
        1 ->
            Parser.succeed Jan

        2 ->
            Parser.succeed Feb

        3 ->
            Parser.succeed Mar

        4 ->
            Parser.succeed Apr

        5 ->
            Parser.succeed May

        6 ->
            Parser.succeed Jun

        7 ->
            Parser.succeed Jul

        8 ->
            Parser.succeed Aug

        9 ->
            Parser.succeed Sep

        10 ->
            Parser.succeed Oct

        11 ->
            Parser.succeed Nov

        12 ->
            Parser.succeed Dec

        _ ->
            Parser.problem ("invalid month " ++ String.fromInt x)


localDateTimeParser : Parser.Parser LocalDateTime
localDateTimeParser =
    Parser.succeed LocalDateTime
        |= Parser.int
        |. Parser.symbol "-"
        |= (parserZeroPaddedInt |> Parser.andThen monthFromInt)
        |. Parser.symbol "-"
        |= parserZeroPaddedInt
        |. Parser.symbol "T"
        |= parserZeroPaddedInt
        |. Parser.symbol ":"
        |= parserZeroPaddedInt


localDateTimeStringToPosix : Zone -> String -> Result String Posix
localDateTimeStringToPosix zone input =
    case run localDateTimeParser input of
        Ok { year, month, day, hour, minute } ->
            Ok <| partsToPosix zone { year = year, month = month, day = day, hour = hour, minute = minute, second = 0, millisecond = 0 }

        Err error ->
            Err (deadEndsToString error)


listContainsBy : (a -> Bool) -> List a -> Bool
listContainsBy f =
    isJust << ListExtra.find f


forgetMsgInput : Result x b -> Result x {}
forgetMsgInput =
    Result.map (always {})


withLeftNeighbor : List a -> (Maybe a -> a -> b) -> List b
withLeftNeighbor xs f =
    let
        transducer : a -> ( Maybe a, List b ) -> ( Maybe a, List b )
        transducer new priorMaybeAndList =
            case first priorMaybeAndList of
                Nothing ->
                    ( Just new, [ f Nothing new ] )

                Just prior ->
                    ( Just new, f (Just prior) new :: second priorMaybeAndList )
    in
    List.reverse <| second <| List.foldl transducer ( Nothing, [] ) xs


foldPairs : List a -> (( a, a ) -> b) -> List b
foldPairs xs f =
    let
        transducer : a -> ( Maybe a, List b ) -> ( Maybe a, List b )
        transducer new priorMaybeAndList =
            case first priorMaybeAndList of
                Nothing ->
                    ( Just new, [] )

                Just prior ->
                    ( Just new, f ( prior, new ) :: second priorMaybeAndList )
    in
    second <| List.foldl transducer ( Nothing, [] ) xs


join3 : a -> b -> c -> ( a, b, c )
join3 a b c =
    ( a, b, c )


deadEndsToString : List DeadEnd -> String
deadEndsToString deadEnds =
    String.join "; " (List.map deadEndToString deadEnds)


deadEndToString : DeadEnd -> String
deadEndToString deadend =
    problemToString deadend.problem ++ " at row " ++ String.fromInt deadend.row ++ ", col " ++ String.fromInt deadend.col


problemToString : Problem -> String
problemToString p =
    case p of
        Expecting s ->
            "expecting '" ++ s ++ "'"

        ExpectingInt ->
            "expecting int"

        ExpectingHex ->
            "expecting hex"

        ExpectingOctal ->
            "expecting octal"

        ExpectingBinary ->
            "expecting binary"

        ExpectingFloat ->
            "expecting float"

        ExpectingNumber ->
            "expecting number"

        ExpectingVariable ->
            "expecting variable"

        ExpectingSymbol s ->
            "expecting symbol '" ++ s ++ "'"

        ExpectingKeyword s ->
            "expecting keyword '" ++ s ++ "'"

        ExpectingEnd ->
            "expecting end"

        UnexpectedChar ->
            "unexpected char"

        Problem s ->
            "problem " ++ s

        BadRepeat ->
            "bad repeat"
