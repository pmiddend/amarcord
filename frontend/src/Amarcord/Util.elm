module Amarcord.Util exposing (..)

import Dict
import Http
import Json.Decode as Decode
import List exposing (foldr)
import String exposing (padLeft)
import Time exposing (Month(..), Posix, Zone, toDay, toHour, toMinute, toMonth, toYear)


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


dictMapValues : (a -> b) -> Dict.Dict k a -> Dict.Dict k b
dictMapValues f =
    Dict.map (\_ value -> f value)


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
    in
    year ++ "/" ++ month ++ "/" ++ day ++ " " ++ hour ++ ":" ++ minute
