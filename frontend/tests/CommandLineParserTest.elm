module CommandLineParserTest exposing (..)

import Amarcord.CommandLineParser exposing (CommandLineOption(..), coparseCommandLine, parseCommandLine)
import Expect
import Fuzz exposing (Fuzzer)
import Test exposing (..)


notContains : String -> String -> Bool
notContains needle haystack =
    not (String.contains needle haystack)


goodItemFuzzer : (a -> Bool) -> Fuzzer a -> Fuzzer a
goodItemFuzzer predicate itemFuzzer =
    itemFuzzer
        |> Fuzz.andThen
            (\item ->
                if predicate item then
                    Fuzz.constant item

                else
                    goodItemFuzzer predicate itemFuzzer
            )


optionFuzzer : Fuzzer CommandLineOption
optionFuzzer =
    let
        alphabet =
            String.toList "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

        alphaNum =
            alphabet ++ String.toList "0123456789"

        specials =
            String.toList "-_."

        alphaNumAndSpecials =
            alphaNum ++ specials

        optionNameFuzzer : Fuzzer String
        optionNameFuzzer =
            Fuzz.map2
                (\c rest -> String.cons c (String.fromList rest))
                (Fuzz.oneOfValues alphaNum)
                (Fuzz.list (Fuzz.oneOfValues alphaNumAndSpecials))

        optionValueFuzzer : Fuzzer String
        optionValueFuzzer =
            Fuzz.map2
                (\c rest -> String.cons c (String.fromList rest))
                (Fuzz.oneOfValues alphaNum)
                (Fuzz.list (Fuzz.oneOfValues alphaNumAndSpecials))

        longOptionsFuzzer : Fuzzer CommandLineOption
        longOptionsFuzzer =
            Fuzz.map2 LongOption optionNameFuzzer optionValueFuzzer

        shortOptionsFuzzer : Fuzzer CommandLineOption
        shortOptionsFuzzer =
            Fuzz.map2 ShortOption (Fuzz.oneOfValues alphaNum) optionValueFuzzer
    in
    Fuzz.oneOf [ longOptionsFuzzer, shortOptionsFuzzer ]


suite : Test
suite =
    describe "Command line parsing"
        [ describe "real parsing"
            [ test "single, long parameter with alphabetic value" <| \_ -> Expect.equal (Ok [ LongOption "foo" "bar" ]) (parseCommandLine "--foo=bar")
            , test "single, long parameter with numeric value" <| \_ -> Expect.equal (Ok [ LongOption "foo" "3.0" ]) (parseCommandLine "--foo=3.0")
            , test "single, long parameter with numeric value and space" <| \_ -> Expect.equal (Ok [ LongOption "foo" "3.0" ]) (parseCommandLine "--foo 3.0")
            , test "single, long parameter with alphabetic value and space" <| \_ -> Expect.equal (Ok [ LongOption "foo" "bar" ]) (parseCommandLine "--foo bar")
            , test "single, short parameter with alphabetic value" <| \_ -> Expect.equal (Ok [ ShortOption 'f' "bar" ]) (parseCommandLine "-f bar")
            , test "single, short parameter with numeric value" <| \_ -> Expect.equal (Ok [ ShortOption 'f' "3.0" ]) (parseCommandLine "-f 3.0")
            , test "long switch" <| \_ -> Expect.equal (Ok [ LongSwitch "bar" ]) (parseCommandLine "--bar")
            , test "short switch" <| \_ -> Expect.equal (Ok [ ShortSwitch 'b' ]) (parseCommandLine "-b")
            ]
        , describe "coparsing"
            [ test "a whole test" <|
                \_ ->
                    Expect.equal "--foo=bar --baz -f b bar -q"
                        (coparseCommandLine
                            [ LongOption "foo" "bar"
                            , LongSwitch "baz"
                            , ShortOption 'f' "b"
                            , UnnamedOption "bar"
                            , ShortSwitch 'q'
                            ]
                        )
            , test "long switch at the end" <|
                \_ ->
                    Expect.equal "--foo=bar --a" (coparseCommandLine [ LongOption "foo" "bar", LongSwitch "a" ])
            , test "double option" <|
                \_ ->
                    let
                        original =
                            [ ShortOption 'A' "A", ShortOption 'A' "A" ]
                    in
                    Expect.equal (Ok original) (parseCommandLine (coparseCommandLine original))
            ]
        , describe "properties"
            [ fuzz
                (Fuzz.list optionFuzzer)
                "coparse, then parse"
              <|
                \commandLine -> Expect.equal (Ok commandLine) (parseCommandLine (coparseCommandLine commandLine))
            ]
        ]
