module App.Bootstrap where

import App.FontAwesome (Icon, icon)
import App.HalogenUtils (classList, classes)
import App.Utils (maybeSingleton)
import Control.Monad ((<$>))
import ConvertableOptions (class Defaults, defaults)
import Data.Array (concatMap, null, (:))
import Data.Eq (class Eq, (==))
import Data.Function (const, (<<<))
import Data.Int as Int
import Data.Maybe (Maybe(..), fromMaybe, isJust)
import Data.Semigroup ((<>))
import Data.Show (class Show, show)
import Data.Unit (Unit, unit)
import Halogen.HTML (HTML)
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
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

data TableFlag
  = TableStriped
  | TableSmall
  | TableBordered

table :: forall t11 t12. String -> Array TableFlag -> Array (HTML t12 t11) -> Array (HTML t12 t11) -> HTML t12 t11
table tableId flags header body =
  let
    flagToClasses TableStriped = [ "table-striped" ]
    flagToClasses TableSmall = [ "table-sm" ]
    flagToClasses TableBordered = [ "table-bordered" ]
  in
    HH.table [ classList ("table" : concatMap flagToClasses flags), HP.id tableId ] [ HH.thead_ [ HH.tr_ header ], HH.tbody_ body ]

plainTh_ :: forall t23 t24. String -> HTML t24 t23
plainTh_ x = HH.th_ [ HH.text x ]

plainTd_ :: forall t27 t28. String -> HTML t28 t27
plainTd_ x = HH.td_ [ HH.text x ]

type TextInputOptional w i =
  ( description :: Array (HH.HTML w i)
  , validationMessage :: String
  )

type TextInputAll w i =
  ( id :: String
  , name :: String
  , value :: String
  , valueChange :: (String -> i)
  | TextInputOptional w i
  )

renderTextInput
  :: forall w i provided
   . Defaults { | TextInputOptional w i } { | provided } { | TextInputAll w i }
  => { | provided }
  -> HH.HTML w i
renderTextInput provided =
  let
    defaultOptions :: { | TextInputOptional w i }
    defaultOptions = { description: [], validationMessage: "" }
    all = defaults defaultOptions provided
  in
    HH.div [ classes "mb-3" ]
      [ HH.label [ HP.for all.id, classes "form-label" ] [ HH.text all.name ]
      , HH.input
          [ HP.type_ HP.InputText
          , classes ("form-control" <> if all.validationMessage == "" then "" else " is-invalid")
          , HP.id all.id
          , HP.value all.value
          , HE.onValueChange all.valueChange
          ]
      , if null all.description then HH.text "" else HH.div [ classes "form-text" ] all.description
      , if all.validationMessage == "" then HH.text "" else HH.div [ classes "invalid-feedback" ] [ HH.text all.validationMessage ]
      ]

type HTMLRadioItem radioType action =
  { checked :: Boolean
  , value :: radioType
  , onValueChange :: radioType -> action
  , id :: String
  , label :: String
  }

renderRadio :: forall w radioType action. Array (HTMLRadioItem radioType action) -> HH.HTML w action
renderRadio values =
  let
    makeRadio { checked, id, onValueChange, label, value } = HH.div [ classes "form-check form-check-inline" ]
      [ HH.input
          [ HP.id id
          , classes "form-check-input"
          , HP.type_ HP.InputRadio
          , HP.checked checked
          , HE.onValueChange (const (onValueChange value))
          ]
      , HH.label [ classes "form-check-label", HP.for id ] [ HH.text label ]
      ]
  in
    HH.div_ [ HH.div [ classes "form-check form-check-inline" ] (makeRadio <$> values) ]

renderEnumSimple :: forall a w i. Eq a => Show a => a -> String -> (a -> i) -> Array a -> HH.HTML w i
renderEnumSimple selected prefix onValueChange values =
  let
    makeRadio :: a -> HTMLRadioItem a i
    makeRadio x =
      { checked: x == selected
      , id: prefix <> "-" <> show x
      , onValueChange
      , label: show x
      , value: x
      }
  in
    renderRadio (makeRadio <$> values)

renderNumericInput :: forall w i. String -> String -> Array (HH.HTML w i) -> Maybe Int -> Maybe Int -> Int -> (Maybe Int -> i) -> HH.HTML w i
renderNumericInput id name description min max value valueChange =
  HH.div [ classes "mb-3" ]
    [ HH.label [ HP.for id, classes "form-label" ] [ HH.text name ]
    , HH.input
        ( [ HP.type_ HP.InputNumber
          , classes "form-control"
          , HP.id id
          , HP.value (show value)
          , HE.onValueChange (valueChange <<< Int.fromString)
          ] <> maybeSingleton (\min' -> HP.min (Int.toNumber min')) min <> maybeSingleton (\max' -> HP.max (Int.toNumber max')) max
        )
    , if null description then HH.text "" else HH.div [ classes "form-text" ] description
    ]

type ButtonOptional =
  ( enabled :: Boolean
  , icon :: Maybe Icon
  , classes :: Maybe String
  )

type ButtonAll i = ("type" :: ButtonType, label :: String, onClick :: Unit -> i | ButtonOptional)

data ButtonType
  = ButtonPrimary
  | ButtonSecondary
  | ButtonSuccess
  | ButtonDanger
  | ButtonWarning
  | ButtonInfo
  | ButtonLight
  | ButtonDark
  | ButtonLink

instance Show ButtonType where
  show ButtonPrimary = "btn-primary"
  show ButtonSecondary = "btn-secondary"
  show ButtonSuccess = "btn-success"
  show ButtonDanger = "btn-danger"
  show ButtonWarning = "btn-warning"
  show ButtonInfo = "btn-info"
  show ButtonLight = "btn-light"
  show ButtonDark = "btn-dark"
  show ButtonLink = "btn-link"

renderButton
  :: forall w i provided
   . Defaults { | ButtonOptional } { | provided } { | ButtonAll i }
  => { | provided }
  -> HH.HTML w i
renderButton provided =
  let
    defaultOptions :: { | ButtonOptional }
    defaultOptions = { enabled: true, icon: Nothing, classes: Nothing }
    all = defaults defaultOptions provided
  in
    HH.button
      [ classes ("btn " <> show all."type" <> " " <> fromMaybe "" all.classes), HE.onClick (\_ -> all.onClick unit), HP.enabled all.enabled ]
      (maybeSingleton icon all.icon <> [ HH.text ((if isJust all.icon then " " else "") <> all.label) ])

