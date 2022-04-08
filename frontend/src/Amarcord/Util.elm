module Amarcord.Util exposing (..)

import Browser.Dom
import Http
import Json.Decode as Decode
import List exposing (foldr)
import String exposing (fromInt, padLeft)
import Task
import Time exposing (Month(..), Posix, Zone, here, now, posixToMillis, toDay, toHour, toMinute, toMonth, toSecond, toYear)


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


resultToJsonDecoder : Result String a -> Decode.Decoder a
resultToJsonDecoder x =
    case x of
        Err e ->
            Decode.fail e

        Ok v ->
            Decode.succeed v


httpDelete : { a | url : String, body : Http.Body, expect : Http.Expect msg } -> Cmd msg
httpDelete { url, body, expect } =
    Http.request
        { method = "DELETE"
        , headers = []
        , url = url
        , body = body
        , expect = expect
        , timeout = Nothing
        , tracker = Nothing
        }


httpPatch : { a | url : String, body : Http.Body, expect : Http.Expect msg } -> Cmd msg
httpPatch { url, body, expect } =
    Http.request
        { method = "PATCH"
        , headers = []
        , url = url
        , body = body
        , expect = expect
        , timeout = Nothing
        , tracker = Nothing
        }


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
