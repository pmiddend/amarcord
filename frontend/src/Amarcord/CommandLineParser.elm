module Amarcord.CommandLineParser exposing (CommandLineOption(..), coparseCommandLine, coparseOption, parseCommandLine)

import Char
import Parser exposing ((|.), (|=), DeadEnd, Parser, Step(..), andThen, backtrackable, chompIf, chompWhile, getChompedString, loop, map, oneOf, problem, run, spaces, succeed, symbol, variable)
import Set


type CommandLineOption
    = LongOption String String
    | ShortOption Char String
    | LongSwitch String
    | ShortSwitch Char
    | UnnamedOption String


char : Parser Char
char =
    chompIf Char.isAlphaNum
        |> getChompedString
        |> andThen
            (\str ->
                case String.toList str of
                    [ c ] ->
                        succeed c

                    _ ->
                        problem <| "expected a single character, got " ++ str
            )


sepBy : Parser () -> Parser a -> Parser (List a)
sepBy separator item =
    let
        helper : List a -> Parser (Step (List a) (List a))
        helper priorElements =
            oneOf
                [ succeed (\newItem -> Loop (newItem :: priorElements)) |= item |. separator
                , succeed () |> map (\_ -> Done (List.reverse priorElements))
                ]
    in
    loop [] helper


optionParser : Parser CommandLineOption
optionParser =
    let
        optionNameParser : Parser String
        optionNameParser =
            variable
                { start = Char.isAlphaNum
                , inner = \c -> Char.isAlphaNum c || c == '_' || c == '-' || c == '.'
                , reserved = Set.empty
                }

        optionValueParser : Parser String
        optionValueParser =
            getChompedString <| succeed () |. chompIf (\c -> c /= ' ' && c /= '\n' && c /= '\t' && c /= '-') |. chompWhile (\c -> c /= ' ' && c /= '\t' && c /= '\n')

        optionNameValueSeparator : Parser ()
        optionNameValueSeparator =
            oneOf [ symbol "=", symbol " " ]

        shortOptionParser : Parser CommandLineOption
        shortOptionParser =
            succeed ShortOption |. symbol "-" |= char |. optionNameValueSeparator |= optionValueParser

        longOptionParser : Parser CommandLineOption
        longOptionParser =
            succeed LongOption |. symbol "--" |= optionNameParser |. optionNameValueSeparator |= optionValueParser

        unnamedParser : Parser CommandLineOption
        unnamedParser =
            -- This should probably be something else
            succeed UnnamedOption |= optionNameParser

        longSwitchParser : Parser CommandLineOption
        longSwitchParser =
            succeed LongSwitch |. symbol "--" |= optionNameParser

        shortSwitchParser : Parser CommandLineOption
        shortSwitchParser =
            succeed ShortSwitch |. symbol "-" |= char
    in
    oneOf
        [ -- needs to backtrack: if it's a switch like "--foo
          -- --bar=baz", then after parsing "--foo" and seeing that
          -- it's got no "=bar" or " bar", it needs to backtrack and
          -- try just "foo" as a switch instead.
          backtrackable longOptionParser
        , longSwitchParser

        -- Same reason for short options
        , backtrackable shortOptionParser
        , shortSwitchParser
        , unnamedParser
        ]


coparseOption : CommandLineOption -> String
coparseOption opt =
    case opt of
        ShortOption name value ->
            "-" ++ String.fromChar name ++ " " ++ value

        LongOption name value ->
            "--" ++ name ++ "=" ++ value

        UnnamedOption value ->
            value

        LongSwitch value ->
            "--" ++ value

        ShortSwitch value ->
            "-" ++ String.fromChar value


coparseCommandLine : List CommandLineOption -> String
coparseCommandLine x =
    String.join " " <| List.map coparseOption x


optionsParser : Parser (List CommandLineOption)
optionsParser =
    sepBy spaces optionParser


parseCommandLine : String -> Result (List DeadEnd) (List CommandLineOption)
parseCommandLine =
    run optionsParser



-- optionComparator : CommandLineOption -> CommandLineOption -> Order
-- optionComparator x y =
--     case x of
--         LongOption key1 value1 ->
--             case y of
--                 LongOption key2 value2 ->
--                     compare ( key1, value1 ) ( key2, value2 )
--                 _ ->
--                     LT
--         ShortOption key1 value1 ->
--             case y of
--                 LongOption _ _ ->
--                     GT
--                 ShortOption key2 value2 ->
--                     compare ( key1, value1 ) ( key2, value2 )
--                 LongSwitch _ ->
--                     LT
--                 ShortSwitch _ ->
--                     LT
--                 UnnamedOption _ ->
--                     LT
--         LongSwitch a ->
--             case y of
--                 LongOption _ _ ->
--                     GT
--                 ShortOption _ _ ->
--                     GT
--                 LongSwitch b ->
--                     compare a b
--                 ShortSwitch _ ->
--                     LT
--                 UnnamedOption _ ->
--                     LT
--         ShortSwitch a ->
--             case y of
--                 LongOption _ _ ->
--                     GT
--                 ShortOption _ _ ->
--                     GT
--                 LongSwitch _ ->
--                     GT
--                 ShortSwitch b ->
--                     compare a b
--                 UnnamedOption _ ->
--                     LT
--         UnnamedOption a ->
--             case y of
--                 LongOption _ _ ->
--                     GT
--                 ShortOption _ _ ->
--                     GT
--                 LongSwitch _ ->
--                     GT
--                 ShortSwitch _ ->
--                     GT
--                 UnnamedOption b ->
--                     compare a b
-- normalizeCommandLine : List CommandLineOption -> List CommandLineOption
-- normalizeCommandLine =
--     List.sortWith optionComparator
