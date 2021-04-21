module App.Overview where

import App.AssociatedTable (AssociatedTable)
import App.QualifiedAttributoName (QualifiedAttributoName)
import Data.Argonaut (Json)
import Data.Array (filter, find, head, mapMaybe, null)
import Data.Eq ((==))
import Data.HeytingAlgebra ((&&))
import Data.Maybe (Maybe(..))
import Data.Tuple (Tuple(..))


type OverviewCell
  = { table :: AssociatedTable
    , source :: String
    , name :: String
    , value :: Json
    }

type OverviewRow
  = Array OverviewCell

-- Given a list of cells with source, select the "best" source
selectProperSource :: Array OverviewCell -> Maybe OverviewCell
selectProperSource [] = Nothing

selectProperSource xs =
  let
    sourceOrder :: Array String
    sourceOrder = [ "manual", "offline", "online" ]

    sources :: Array OverviewCell
    sources = mapMaybe (\source -> find (\x -> x.source == source) xs) sourceOrder
  in
    head (if null sources then xs else sources)

-- Given a row in the overview and a certain attributo, find the correct attributo cell (proper source and stuff)
findCellInRow :: QualifiedAttributoName -> OverviewRow -> Maybe OverviewCell
findCellInRow (Tuple table name) cells =
  let
    foundCells = filter (\cell -> cell.table == table && cell.name == name) cells
  in
    selectProperSource foundCells

