module Amarcord.AssociatedTable exposing (..)

import Api.Data exposing (AssociatedTable(..))
import Json.Decode as Decode


type AssociatedTable
    = Run
    | Chemical


fromString : String -> AssociatedTable
fromString s =
    if s == "run" then
        Run

    else
        Chemical


associatedTableFromApi : Api.Data.AssociatedTable -> AssociatedTable
associatedTableFromApi x =
    case x of
        AssociatedTableRun ->
            Run

        AssociatedTableChemical ->
            Chemical


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


associatedTableToApi : AssociatedTable -> Api.Data.AssociatedTable
associatedTableToApi x =
    case x of
        Run ->
            AssociatedTableRun

        Chemical ->
            AssociatedTableChemical
