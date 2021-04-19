module App.TabledAttributo where

import App.AssociatedTable (AssociatedTable)

type TabledAttributo = {
    table :: AssociatedTable
  , attributoId :: String
  }
