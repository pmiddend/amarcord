module App.Bootstrap where

import App.HalogenUtils (classList)
import Data.Array (concatMap, (:))
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties as HP

container = HH.div [ classList [ "container" ] ]

fluidContainer = HH.div [ classList [ "container-fluid" ] ]

plainH1_ t = HH.h1_ [ HH.text t ]

plainH2_ t = HH.h2_ [ HH.text t ]

plainH3_ t = HH.h3_ [ HH.text t ]

data TableFlag = TableStriped
               | TableSmall
               | TableBordered

table tableId flags header body =
  let flagToClasses TableStriped = [ "table-striped" ]
      flagToClasses TableSmall = [ "table-sm" ]
      flagToClasses TableBordered = [ "table-bordered" ]
  in HH.table [ classList ("table" : concatMap flagToClasses flags), HP.id_ tableId ] [ HH.thead_ [ HH.tr_ header ],  HH.tbody_ body ]

plainTh_ x = HH.th_ [ HH.text x ]

plainTd_ x = HH.td_ [ HH.text x ]

