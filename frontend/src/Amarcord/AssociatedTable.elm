module Amarcord.AssociatedTable exposing (..)

import Api.Data exposing (AssociatedTable(..))


type AssociatedTable
    = Run
    | Chemical


associatedTableFromApi : Api.Data.AssociatedTable -> AssociatedTable
associatedTableFromApi x =
    case x of
        AssociatedTableRun ->
            Run

        AssociatedTableChemical ->
            Chemical


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
