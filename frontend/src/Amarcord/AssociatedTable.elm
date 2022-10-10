module Amarcord.AssociatedTable exposing (..)

import Json.Decode as Decode


type AssociatedTable
    = Run
    | Chemical


associatedTableDecoder : Decode.Decoder AssociatedTable
associatedTableDecoder =
    Decode.string
        |> Decode.andThen
            (\str ->
                case str of
                    "run" ->
                        Decode.succeed Run

                    "chemical" ->
                        Decode.succeed Chemical

                    _ ->
                        Decode.fail <| "unknown table " ++ str
            )


associatedTableToString : AssociatedTable -> String
associatedTableToString x =
    case x of
        Run ->
            "Run"

        Chemical ->
            "Chemical"
