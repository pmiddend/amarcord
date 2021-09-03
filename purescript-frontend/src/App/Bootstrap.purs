module App.Bootstrap where

import App.HalogenUtils (classList)
import Data.Array (concatMap, (:))
import Halogen.HTML (HTML)
import Halogen.HTML as HH
import Halogen.HTML.Properties as HP

container :: forall t62 t63. Array (HTML t63 t62) -> HTML t63 t62
container = HH.div [ classList [ "container" ] ]

fluidContainer :: forall t57 t58. Array (HTML t58 t57) -> HTML t58 t57
fluidContainer = HH.div [ classList [ "container-fluid" ] ]

plainH1_ :: forall t52 t53. String -> HTML t53 t52
plainH1_ t = HH.h1_ [ HH.text t ]

plainH2_ :: forall t46 t47. String -> HTML t47 t46
plainH2_ t = HH.h2_ [ HH.text t ]

plainH3_ :: forall t40 t41. String -> HTML t41 t40
plainH3_ t = HH.h3_ [ HH.text t ]

data TableFlag = TableStriped
               | TableSmall
               | TableBordered

table :: forall t11 t12. String -> Array TableFlag -> Array (HTML t12 t11) -> Array (HTML t12 t11) -> HTML t12 t11
table tableId flags header body =
  let flagToClasses TableStriped = [ "table-striped" ]
      flagToClasses TableSmall = [ "table-sm" ]
      flagToClasses TableBordered = [ "table-bordered" ]
  in HH.table [ classList ("table" : concatMap flagToClasses flags), HP.id_ tableId ] [ HH.thead_ [ HH.tr_ header ],  HH.tbody_ body ]

plainTh_ :: forall t23 t24. String -> HTML t24 t23
plainTh_ x = HH.th_ [ HH.text x ]

plainTd_ :: forall t27 t28. String -> HTML t28 t27
plainTd_ x = HH.td_ [ HH.text x ]

