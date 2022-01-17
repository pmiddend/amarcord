module App.Components.Attributi (attributiComponent) where

import Affjax as AX
import Affjax.RequestBody (RequestBody(Json))
import Affjax.ResponseFormat as ResponseFormat
import App.AffjaxUtils (handleResponse)
import App.Bootstrap (ButtonType(..), TableFlag(..), renderButton, renderEnumSimple, renderNumericInput, renderRadio, renderTextInput, table)
import App.FontAwesome (mkSimpleIcon)
import App.HalogenUtils (AlertType(..), classes, makeAlertHtml, singleClass, makeAlert)
import App.RequestError (RequestError)
import App.Utils (emptyStringToMaybe, forM_, remoteDataChurch)
import Control.Alt ((<#>), (<|>))
import Control.Applicative (pure)
import Control.Apply (apply, (*>), (<*>))
import Control.Bind (void, (<$>))
import Control.Monad (class Monad, bind, (>>=))
import Control.Monad.State (class MonadState)
import Data.Argonaut (class DecodeJson, class EncodeJson, JsonDecodeError(..), decodeJson, encodeJson, fromString, jsonEmptyObject, toString)
import Data.Argonaut.Decode.Combinators ((.:), (.:?))
import Data.Argonaut.Encode.Combinators ((:=), (:=?), (~>), (~>?))
import Data.Array (filter, null, some, (:))
import Data.Bifunctor (bimap)
import Data.BooleanAlgebra (not, (&&), (||))
import Data.Bounded (class Bounded, bottom, top)
import Data.Bounded.Generic (genericBottom, genericTop)
import Data.Either (Either(..), either, hush)
import Data.Enum (class Enum, enumFromTo, class BoundedEnum)
import Data.Enum.Generic (genericCardinality, genericFromEnum, genericPred, genericSucc, genericToEnum)
import Data.Eq (class Eq, (/=), (==))
import Data.Foldable (class Foldable, foldMap, intercalate, any)
import Data.Formatter.Parser.Number (parseDigit, parseInteger)
import Data.Function (const, (#), (<<<), (>>>), flip, ($))
import Data.Functor (($>), class Functor)
import Data.Generic.Rep (class Generic)
import Data.Int as Int
import Data.Lens (Lens', Prism', prism', set, traversed, use, (%~), (.~), (^.))
import Data.Lens.Iso.Newtype (_Newtype)
import Data.Lens.Record (prop)
import Data.Maybe (Maybe(..), fromMaybe, isJust, maybe)
import Data.Newtype (class Newtype, unwrap)
import Data.Number as Number
import Data.Ord (class Ord)
import Data.Ring ((+))
import Data.Semigroup ((<>))
import Data.Show (show, class Show)
import Data.String (split, trim)
import Data.String.Pattern (Pattern(Pattern))
import Data.Traversable (class Traversable, for_, sequenceDefault, traverse)
import Data.Tuple (Tuple(Tuple))
import Data.Unfoldable1 (singleton)
import Data.Unit (Unit)
import Data.Validation.Semigroup (V(..), validation)
import Effect.Aff.Class (class MonadAff, liftAff)
import Halogen as H
import Halogen.HTML as HH
import Halogen.HTML.Events as HE
import Halogen.HTML.Properties (ScopeValue(..))
import Halogen.HTML.Properties as HP
import Network.RemoteData (RemoteData(..), isLoading)
import Prelude (discard)
import Text.Parsing.Parser (ParseError, Parser, parseErrorMessage, parseErrorPosition, runParser, fail)
import Text.Parsing.Parser.Combinators (option, try)
import Text.Parsing.Parser.Pos (Position(..))
import Text.Parsing.Parser.String (char, skipSpaces, string)
import Type.Proxy (Proxy(..))

type NumericRange =
  { minimum :: Maybe Number
  , minimumInclusive :: Boolean
  , maximum :: Maybe Number
  , maximumInclusive :: Boolean
  }

-- These are copied functions from
-- https://github.com/purescript-contrib/purescript-formatters/blob/v5.0.1/src/Data/Formatter/Parser/Number.purs#L35-L35
-- Since we're using the "comma" as a bounds separator, and not a decimal point, but formatters allows both, unfortunately, leading to weird parsing errors
-- Think of (3,5) being parsed as the decimal number "3.5"
parseFractional ∷ Parser String Number
parseFractional = do
  digitStr <- (some parseDigit) <#> (foldMap show >>> ("0." <> _))
  case Number.fromString digitStr of
    Just n -> pure n
    Nothing -> fail ("Not a number: " <> digitStr)

parseNumber ∷ Parser String Number
parseNumber = (+)
  <$> (parseInteger <#> Int.toNumber)
  <*> (option 0.0 $ try $ char '.' *> parseFractional)

rangeParser :: Parser String NumericRange
rangeParser = do
  leftDelimiter <- char '(' <|> char '['
  skipSpaces
  leftBound <- (Just <$> parseNumber) <|> (string "oo" $> Nothing)
  skipSpaces
  void (char ',')
  rightBound <- (Just <$> parseNumber) <|> (string "oo" $> Nothing)
  skipSpaces
  rightDelimiter <- char ')' <|> char ']'
  pure
    ( { minimum: leftBound
      , minimumInclusive: leftDelimiter == '['
      , maximum: rightBound
      , maximumInclusive: rightDelimiter == ']'
      }
    )

parseRange :: String -> Either ParseError NumericRange
parseRange = (flip runParser) rangeParser

parseErrorPositionCol :: Position -> Int
parseErrorPositionCol (Position { column }) = column

showRange :: NumericRange -> String
showRange { minimum, minimumInclusive, maximum, maximumInclusive } =
  let
    prefix = case show <$> minimum, minimumInclusive of
      Nothing, _ -> "(∞"
      Just minimum', true -> "[" <> minimum'
      Just minimum', false -> "(" <> minimum'
    suffix = case show <$> maximum, maximumInclusive of
      Nothing, _ -> "∞)"
      Just maximum', true -> maximum' <> "]"
      Just maximum', false -> maximum' <> ")"
  in
    prefix <> "," <> suffix

data AttributoType
  = AttributoTypeInt
  | AttributoTypeDuration
  | AttributoTypeDateTime
  | AttributoTypeSample
  | AttributoTypeString
  | AttributoTypeComments
  | AttributoTypeList { subType :: AttributoType, minLength :: Maybe Int, maxLength :: Maybe Int }
  | AttributoTypeNumber { range :: Maybe NumericRange, suffix :: Maybe String, standardUnit :: Boolean }
  | AttributoTypeChoice { values :: Array String }

derive instance Eq AttributoType
derive instance Ord AttributoType

data NormalizedSuffix = NotNeeded | InvalidSuffix | ValidSuffix String

isInvalidSuffix :: NormalizedSuffix -> Boolean
isInvalidSuffix InvalidSuffix = true
isInvalidSuffix _ = false

type AttributoAugTypeNumberProps =
  { rangeInput :: String
  , suffixInput :: String
  , suffixNormalized :: NormalizedSuffix
  , standardUnit :: Boolean
  }

data AttributoAugType
  = AttributoAugTypeSimple AttributoType
  | AttributoAugTypeNumber
      AttributoAugTypeNumberProps
  | AttributoAugTypeChoice { valuesInput :: String }

data AttributoTypeEnum = ATInt | ATDuration | ATDateTime | ATSample | ATString | ATComments | ATList | ATNumber | ATChoice

attributoTypeToEnum :: AttributoType -> AttributoTypeEnum
attributoTypeToEnum AttributoTypeInt = ATInt
attributoTypeToEnum AttributoTypeDuration = ATDuration
attributoTypeToEnum AttributoTypeDateTime = ATDateTime
attributoTypeToEnum AttributoTypeSample = ATSample
attributoTypeToEnum AttributoTypeString = ATString
attributoTypeToEnum AttributoTypeComments = ATComments
attributoTypeToEnum (AttributoTypeList _) = ATList
attributoTypeToEnum (AttributoTypeNumber _) = ATNumber
attributoTypeToEnum (AttributoTypeChoice _) = ATChoice

attributoAugTypeToEnum :: AttributoAugType -> AttributoTypeEnum
attributoAugTypeToEnum (AttributoAugTypeSimple t) = attributoTypeToEnum t
attributoAugTypeToEnum (AttributoAugTypeChoice _) = ATChoice
attributoAugTypeToEnum (AttributoAugTypeNumber _) = ATNumber

attributoAugTypeToType :: AttributoAugType -> AttributoType
attributoAugTypeToType (AttributoAugTypeSimple x) = x
attributoAugTypeToType (AttributoAugTypeChoice { valuesInput }) = AttributoTypeChoice { values: trim <$> split (Pattern ",") valuesInput }
attributoAugTypeToType (AttributoAugTypeNumber { rangeInput, suffixInput, standardUnit }) =
  AttributoTypeNumber { standardUnit, range: hush (parseRange rangeInput), suffix: emptyStringToMaybe suffixInput }

initialAttributoType :: AttributoTypeEnum -> AttributoType
initialAttributoType ATInt = AttributoTypeInt
initialAttributoType ATDuration = AttributoTypeDuration
initialAttributoType ATDateTime = AttributoTypeDateTime
initialAttributoType ATSample = AttributoTypeSample
initialAttributoType ATString = AttributoTypeString
initialAttributoType ATComments = AttributoTypeComments
initialAttributoType ATList = AttributoTypeList { minLength: Nothing, maxLength: Nothing, subType: AttributoTypeString }
initialAttributoType ATNumber = AttributoTypeNumber { range: Nothing, suffix: Nothing, standardUnit: false }
initialAttributoType ATChoice = AttributoTypeChoice { values: [] }

initialAttributoAugType :: AttributoTypeEnum -> AttributoAugType
initialAttributoAugType ATInt = AttributoAugTypeSimple AttributoTypeInt
initialAttributoAugType ATDuration = AttributoAugTypeSimple AttributoTypeDuration
initialAttributoAugType ATDateTime = AttributoAugTypeSimple AttributoTypeDateTime
initialAttributoAugType ATSample = AttributoAugTypeSimple AttributoTypeSample
initialAttributoAugType ATString = AttributoAugTypeSimple AttributoTypeString
initialAttributoAugType ATComments = AttributoAugTypeSimple AttributoTypeComments
initialAttributoAugType ATList = AttributoAugTypeSimple (AttributoTypeList { minLength: Nothing, maxLength: Nothing, subType: AttributoTypeString })
initialAttributoAugType ATNumber = AttributoAugTypeNumber
  { rangeInput: ""
  , suffixInput: ""
  , suffixNormalized: NotNeeded
  , standardUnit: false
  }
initialAttributoAugType ATChoice = AttributoAugTypeChoice { valuesInput: "" }

_AttributoAugTypeNumber :: Prism' AttributoAugType AttributoAugTypeNumberProps
_AttributoAugTypeNumber = prism' AttributoAugTypeNumber case _ of
  AttributoAugTypeNumber p -> Just p
  _ -> Nothing

_attributoAugTypeNumberSuffixNormalized :: Lens' AttributoAugTypeNumberProps NormalizedSuffix
_attributoAugTypeNumberSuffixNormalized = prop (Proxy :: Proxy "suffixNormalized")

_attributoAugTypeNumberStandardUnit :: Lens' AttributoAugTypeNumberProps Boolean
_attributoAugTypeNumberStandardUnit = prop (Proxy :: Proxy "standardUnit")

_attributoAugTypeNumberSuffixInput :: Lens' AttributoAugTypeNumberProps String
_attributoAugTypeNumberSuffixInput = prop (Proxy :: Proxy "suffixInput")

-- augTypeNumberSuffixAndStandardUnit :: Getter' AttributoAugType (Maybe (Tuple String Boolean))
-- augTypeNumberSuffixAndStandardUnit = to matcher
--   where
--   matcher t@(AttributoAugTypeNumber { standardUnit, suffixInput }) = Just (Tuple suffixInput standardUnit)
--   matcher _ = Nothing

isUserType :: AttributoTypeEnum -> Boolean
isUserType ATSample = false
isUserType ATComments = false
isUserType _ = true

derive instance Eq AttributoTypeEnum
derive instance Ord AttributoTypeEnum
derive instance Generic AttributoTypeEnum _

instance Show AttributoTypeEnum where
  show ATInt = "integer"
  show ATDuration = "duration"
  show ATDateTime = "date&time"
  show ATSample = "sample"
  show ATString = "string"
  show ATComments = "comments"
  show ATList = "list of values"
  show ATNumber = "decimal number"
  show ATChoice = "choice of values"

instance Enum AttributoTypeEnum where
  succ = genericSucc
  pred = genericPred

instance Bounded AttributoTypeEnum where
  top = genericTop
  bottom = genericBottom

instance BoundedEnum AttributoTypeEnum where
  cardinality = genericCardinality
  toEnum = genericToEnum
  fromEnum = genericFromEnum

instance Show AttributoType where
  show AttributoTypeInt = "int"
  show AttributoTypeDuration = "duration"
  show AttributoTypeDateTime = "date-time"
  show AttributoTypeSample = "Sample ID"
  show AttributoTypeString = "string"
  show AttributoTypeComments = "comments"
  show (AttributoTypeList { subType, minLength, maxLength }) = case minLength, maxLength of
    Nothing, Nothing -> "list of " <> show subType
    Just min, Nothing -> "list (at least " <> show min <> " element(s)) of " <> show subType
    Nothing, Just max -> "list (at most " <> show max <> " element(s)) of " <> show subType
    Just min, Just max -> "list (between " <> show min <> " and " <> show max <> " element(s)) of " <> show subType
  show (AttributoTypeNumber { range, suffix }) = fromMaybe "number" suffix <> (fromMaybe "" (((\x -> " ∈ " <> x) <<< showRange) <$> range))
  show (AttributoTypeChoice { values }) = "one of " <> intercalate ", " values

data JSONSchema
  = JSONSchemaInteger { format :: Maybe String }
  | JSONSchemaNumber
      { minimum :: Maybe Number
      , maximum :: Maybe Number
      , exclusiveMinimum :: Maybe Number
      , exclusiveMaximum :: Maybe Number
      , suffix :: Maybe String
      , format :: Maybe String
      }
  | JSONSchemaString { enum :: Maybe (Array String), format :: Maybe String }
  | JSONSchemaArray
      { minItems :: Maybe Int
      , maxItems :: Maybe Int
      , items :: JSONSchema
      , format :: Maybe String
      }

instance DecodeJson JSONSchema where
  decodeJson json = do
    obj <- decodeJson json
    type_ <- obj .: "type"
    case type_ of
      "integer" -> do
        format <- obj .:? "format"
        Right (JSONSchemaInteger { format })
      "number" -> do
        minimum <- obj .:? "minimum"
        maximum <- obj .:? "maximum"
        exclusiveMinimum <- obj .:? "exclusiveMinimum"
        exclusiveMaximum <- obj .:? "exclusiveMaximum"
        suffix <- obj .:? "suffix"
        format <- obj .:? "format"
        Right (JSONSchemaNumber { minimum, maximum, exclusiveMinimum, exclusiveMaximum, suffix, format })
      "string" -> do
        enum' <- obj .:? "enum"
        format <- obj .:? "format"
        Right (JSONSchemaString { "enum": enum', format })
      "array" -> do
        items <- obj .: "items"
        minItems <- obj .:? "minItems"
        maxItems <- obj .:? "maxItems"
        format <- obj .:? "format"
        Right (JSONSchemaArray { items, minItems, maxItems, format })
      _ -> Left (TypeMismatch ("type is " <> type_))

instance EncodeJson JSONSchema where
  encodeJson (JSONSchemaInteger { format }) = "type" := "integer" ~> "format" :=? format ~>? jsonEmptyObject
  encodeJson
    ( JSONSchemaNumber
        { minimum
        , maximum
        , exclusiveMinimum
        , exclusiveMaximum
        , suffix
        , format
        }
    ) = "type" := "number"
    ~> "minimum"
      :=? minimum
    ~>? "maximum" :=? maximum
    ~>? "exclusiveMaximum" :=? exclusiveMaximum
    ~>? "exclusiveMinimum" :=? exclusiveMinimum
    ~>? "suffix" :=? suffix
    ~>? "format" :=? format
    ~>? jsonEmptyObject
  encodeJson (JSONSchemaString { enum, format }) = "type" := "string" ~> "enum" :=? enum ~>? "format" :=? format ~>? jsonEmptyObject
  encodeJson (JSONSchemaArray { items, minItems, maxItems }) = "type" := "array"
    ~> "items" := (encodeJson items)
    ~> "minItems" :=? minItems
    ~>? "maxItems" :=? maxItems
    ~>? jsonEmptyObject

jsonSchemaToAttributoType :: JSONSchema -> Either String AttributoType
jsonSchemaToAttributoType (JSONSchemaNumber { minimum: Nothing, maximum: Nothing, suffix, format }) = Right
  ( AttributoTypeNumber
      { range: Nothing
      , suffix
      , standardUnit: format == Just "standard-unit"
      }
  )
jsonSchemaToAttributoType (JSONSchemaNumber { minimum, maximum, exclusiveMinimum, exclusiveMaximum, suffix, format }) = Right
  ( AttributoTypeNumber
      { range:
          Just
            { minimum: minimum <|> exclusiveMinimum
            , maximum: maximum <|> exclusiveMaximum
            , minimumInclusive: isJust minimum
            , maximumInclusive: isJust maximum
            }
      , suffix
      , standardUnit: format == Just "standard-unit"
      }
  )
jsonSchemaToAttributoType (JSONSchemaInteger { format: Just "sample-id" }) = Right AttributoTypeSample
jsonSchemaToAttributoType (JSONSchemaInteger { format: Just x }) = Left ("invalid integer format \"" <> x <> "\"")
jsonSchemaToAttributoType (JSONSchemaInteger { format: Nothing }) = Right AttributoTypeInt
jsonSchemaToAttributoType (JSONSchemaArray { minItems, maxItems, items, format: Nothing }) = (\subType -> AttributoTypeList { minLength: minItems, maxLength: maxItems, subType }) <$> jsonSchemaToAttributoType items
jsonSchemaToAttributoType (JSONSchemaArray { format: Just "comments" }) = Right AttributoTypeComments
jsonSchemaToAttributoType (JSONSchemaArray { format: Just x }) = Left ("invalid array format \"" <> x <> "\"")
jsonSchemaToAttributoType (JSONSchemaString { enum: Nothing, format: Nothing }) = Right AttributoTypeString
jsonSchemaToAttributoType (JSONSchemaString { enum: Nothing, format: Just "date-time" }) = Right AttributoTypeDateTime
jsonSchemaToAttributoType (JSONSchemaString { enum: Nothing, format: Just "duration" }) = Right AttributoTypeDuration
jsonSchemaToAttributoType (JSONSchemaString { enum: Nothing, format: Just format }) = Left ("string format \"" <> format <> "\" invalid")

jsonSchemaToAttributoType (JSONSchemaString { enum: Just _, format: Just format }) = Left ("choice with format \"" <> format <> "\" invalid")
jsonSchemaToAttributoType (JSONSchemaString { enum: Just choices, format: Nothing }) = Right (AttributoTypeChoice { values: choices })

attributoTypeToJsonSchema :: AttributoType -> JSONSchema
attributoTypeToJsonSchema AttributoTypeInt = JSONSchemaInteger { format: Nothing }
attributoTypeToJsonSchema AttributoTypeDuration = JSONSchemaString { enum: Nothing, format: Just "duration" }
attributoTypeToJsonSchema AttributoTypeDateTime = JSONSchemaString { enum: Nothing, format: Just "date-time" }
attributoTypeToJsonSchema AttributoTypeSample = JSONSchemaInteger { format: Just "sample-id" }
attributoTypeToJsonSchema AttributoTypeString = JSONSchemaString { enum: Nothing, format: Nothing }
-- FIXME: comments are obviously not integers
attributoTypeToJsonSchema AttributoTypeComments = JSONSchemaArray
  { minItems: Nothing
  , maxItems: Nothing
  , items: (JSONSchemaInteger { format: Nothing })
  , format: Just "comments"
  }
attributoTypeToJsonSchema (AttributoTypeList { subType, minLength, maxLength }) = JSONSchemaArray
  { minItems: minLength
  , maxItems: maxLength
  , items: attributoTypeToJsonSchema subType
  , format: Nothing
  }
attributoTypeToJsonSchema (AttributoTypeNumber { range: Just range, suffix, standardUnit }) = JSONSchemaNumber
  { minimum: if range.minimumInclusive then range.minimum else Nothing
  , maximum: if range.maximumInclusive then range.maximum else Nothing
  , exclusiveMinimum: if range.minimumInclusive then Nothing else range.minimum
  , exclusiveMaximum: if range.maximumInclusive then Nothing else range.maximum
  , suffix
  , format: if standardUnit then Just "standard-unit" else Nothing
  }
attributoTypeToJsonSchema (AttributoTypeNumber { range: Nothing, suffix, standardUnit }) = JSONSchemaNumber
  { minimum: Nothing
  , maximum: Nothing
  , exclusiveMinimum: Nothing
  , exclusiveMaximum: Nothing
  , suffix
  , format: if standardUnit then Just "standard-unit" else Nothing
  }
attributoTypeToJsonSchema (AttributoTypeChoice { values }) = JSONSchemaString { enum: Just values, format: Nothing }

data AssociatedTable = Run | Sample

derive instance Eq AssociatedTable
derive instance Ord AssociatedTable
derive instance Generic AssociatedTable _

instance Enum AssociatedTable where
  pred Run = Nothing
  pred Sample = Just Run
  succ Run = Just Sample
  succ Sample = Nothing

instance Bounded AssociatedTable where
  top = Run
  bottom = Sample

instance Show AssociatedTable where
  show Run = "Run"
  show Sample = "Sample"

instance DecodeJson AssociatedTable where
  decodeJson x = case toString x of
    Nothing -> Left (TypeMismatch "invalid associated table value (invalid type)")
    Just "run" -> Right Run
    Just "sample" -> Right Sample
    Just wrongValue -> Left (TypeMismatch ("invalid associated table string value: " <> wrongValue))

instance EncodeJson AssociatedTable where
  encodeJson Run = fromString "run"
  encodeJson Sample = fromString "sample"

newtype Attributo a = Attributo
  { name :: String
  , description :: String
  , associatedTable :: AssociatedTable
  , "type" :: a
  }

derive instance Functor Attributo
derive instance Generic (Attributo a) _
derive instance Newtype (Attributo a) _

_name :: forall a. Lens' (Attributo a) String
_name = prop (Proxy :: Proxy "name") >>> _Newtype

_description :: forall a. Lens' (Attributo a) String
_description = prop (Proxy :: Proxy "description") >>> _Newtype

_associatedTable :: forall a. Lens' (Attributo a) AssociatedTable
_associatedTable = prop (Proxy :: Proxy "associatedTable") >>> _Newtype

_attributoType :: forall a. Lens' (Attributo a) a
_attributoType = prop (Proxy :: Proxy "type") >>> _Newtype

instance Foldable Attributo where
  foldMap f (Attributo { "type": t }) = f t
  foldr f initValue (Attributo { "type": t }) = f t initValue
  foldl f initValue (Attributo { "type": t }) = f initValue t

instance Traversable Attributo where
  traverse f (Attributo { "type": t, name, description, associatedTable }) =
    let
      imbed :: forall a. a -> Attributo a
      imbed x = Attributo { name, description, associatedTable, "type": x }
    in
      apply (pure imbed) (f t)
  sequence = sequenceDefault

instance EncodeJson a => EncodeJson (Attributo a) where
  encodeJson (Attributo { name, description, associatedTable, "type": t }) = "type" := t ~> "name" := name ~> "description" := description ~> "associatedTable" := associatedTable ~> jsonEmptyObject

instance DecodeJson a => DecodeJson (Attributo a) where
  decodeJson json = do
    obj <- decodeJson json
    name <- obj .: "name"
    description <- obj .: "description"
    associatedTable <- obj .: "associatedTable"
    type_ <- obj .: "type"
    Right (Attributo { name, description, associatedTable, "type": type_ })

type JsonAttributo = Attributo JSONSchema

type AttributiListResponse =
  { attributi :: Array JsonAttributo
  }

type State =
  { attributiList :: RemoteData RequestError (Array (Attributo AttributoType))
  , newAttributo :: Maybe (Attributo AttributoAugType)
  , attributoRequest :: RemoteData RequestError {}
  }

_newAttributo :: Lens' State (Maybe (Attributo AttributoAugType))
_newAttributo = prop (Proxy :: Proxy "newAttributo")

_attributiList :: Lens' State (RemoteData RequestError (Array (Attributo AttributoType)))
_attributiList = prop (Proxy :: Proxy "attributiList")

_attributoRequest :: Lens' State (RemoteData RequestError {})
_attributoRequest = prop (Proxy :: Proxy "attributoRequest")

data Action
  = Initialize
  | AddAttributo
  | EditAttributoChange (Attributo AttributoAugType -> Attributo AttributoAugType)
  | EditAttributoTypeChange AttributoTypeEnum
  | EditAttributoSubmit
  | EditAttributoCancel

readAttributiList :: forall m. MonadAff m => m (Either RequestError AttributiListResponse)
readAttributiList = liftAff (AX.get ResponseFormat.json "/api/attributi") >>= handleResponse

checkStandardUnit :: forall m. MonadAff m => String -> m (Either RequestError { normalized :: String })
checkStandardUnit input =
  liftAff
    (AX.post ResponseFormat.json "/api/unit" (Just (Json (encodeJson { input })))) >>= handleResponse

addAttributo :: forall m. MonadAff m => Attributo AttributoType -> m (Either RequestError {})
addAttributo a =
  liftAff
    (AX.post ResponseFormat.json "/api/attributi" (Just (Json (encodeJson (attributoTypeToJsonSchema <$> a))))) >>= handleResponse

htmlizeRequestError :: forall w i. RequestError -> HH.HTML w i
htmlizeRequestError { code, title, description } = makeAlertHtml AlertDanger
  [ HH.h4
      [ singleClass "alert-heading" ]
      [ HH.text (title <> (maybe "" (\code' -> " (code " <> show code' <> ")") code)) ]
  , HH.pre_ [ HH.text description ]
  ]

renderTable :: forall w i. State -> Array (Attributo AttributoType) -> HH.HTML w i
renderTable _state result =
  let
    headers = [ "", "Table", "Name", "Description", "Type" ]

    makeAttributoRow :: Attributo AttributoType -> HH.HTML w i
    makeAttributoRow (Attributo { name, description, associatedTable, "type": t }) = HH.tr_
      [ HH.td_ [ HH.text "" ]
      , HH.th [ HP.scope ScopeRow ] [ HH.text (show associatedTable) ]
      , HH.th [ HP.scope ScopeRow ] [ HH.text name ]
      , HH.td_ [ HH.text description ]
      , HH.td_ [ HH.text (show t) ]
      ]
  in
    table "attributi-table"
      [ TableStriped ]
      ((HH.th_ <<< singleton <<< HH.text) <$> headers)
      (makeAttributoRow <$> result)

renderTypeForm :: forall w. AttributoAugType -> HH.HTML w Action
renderTypeForm t =
  let
    typeSpecificForm :: Array (HH.HTML w Action)
    typeSpecificForm = case t of
      AttributoAugTypeSimple (AttributoTypeList { subType, minLength, maxLength }) ->
        [ renderNumericInput
            "attributo-edit-list-min"
            "Minimum length"
            []
            (Just 0)
            Nothing
            (fromMaybe 0 minLength)
            (\newMinLength -> EditAttributoChange (set _attributoType (AttributoAugTypeSimple (AttributoTypeList { minLength: newMinLength, maxLength, subType }))))
        , renderNumericInput
            "attributo-edit-list-max"
            "Maximum length"
            [ HH.text "Leave at zero to have no limit on the number of values" ]
            (Just 0)
            Nothing
            (fromMaybe 0 maxLength)
            (\newMaxLength -> EditAttributoChange (set _attributoType (AttributoAugTypeSimple (AttributoTypeList { minLength, maxLength: newMaxLength, subType }))))
        , HH.div [ classes "mb-3" ]
            [ HH.label [ classes "form-label" ] [ HH.text "List subtype" ]
            , renderRadio
                [ { checked: attributoTypeToEnum subType == ATNumber
                  , value: ATNumber
                  , onValueChange: (\newType -> EditAttributoChange (set _attributoType (AttributoAugTypeSimple (AttributoTypeList { minLength, maxLength, subType: initialAttributoType newType }))))
                  , id: "attributo-edit-list-type-number"
                  , label: "decimal number"
                  }
                , { checked: attributoTypeToEnum subType == ATString
                  , value: ATString
                  , onValueChange: (\newType -> EditAttributoChange (set _attributoType (AttributoAugTypeSimple (AttributoTypeList { minLength, maxLength, subType: initialAttributoType newType }))))
                  , id: "attributo-edit-list-type-string"
                  , label: "string"
                  }
                ]
            , HH.div [ classes "form-text" ] [ HH.text "For simplicity reasons, only a few types are allowed as list element types." ]
            ]
        ]
      AttributoAugTypeSimple _ -> []
      AttributoAugTypeNumber { rangeInput, suffixInput, suffixNormalized, standardUnit } ->
        let
          update rangeInput' suffixInput' standardUnit' = EditAttributoChange
            ( set _attributoType
                ( AttributoAugTypeNumber
                    { rangeInput: rangeInput'
                    , suffixInput: suffixInput'
                    , standardUnit: standardUnit'
                    , suffixNormalized
                    }
                )
            )
        in
          [ renderTextInput
              { id: "attributo-edit-number-range"
              , name: "Range"
              , description: [ HH.text "Optional range in interval notation. For example, “(oo, 3]” is “x ≤ 3” (“oo” representing the hard-to-type infinity symbol “∞” here), and “(-1, 10)” is “-1 < x < 10”." ]
              , value: rangeInput
              , valueChange: (\newRangeInput -> update newRangeInput suffixInput standardUnit)
              , validationMessage:
                  if rangeInput == "" then ""
                  else either
                    (\x -> "Range is invalid at char " <> show (parseErrorPositionCol (parseErrorPosition x)) <> ": " <> parseErrorMessage x)
                    (const "")
                    (parseRange rangeInput)
              }
          , HH.div [ classes "mb-3" ]
              [ HH.label [ HP.for "attributo-edit-number-suffix", classes "form-label" ]
                  [ HH.text "Suffix or unit"
                  ]
              , HH.div [ classes "input-group" ]
                  [ HH.div [ classes "input-group-text" ]
                      [ HH.span [ classes "me-1" ] [ HH.text "Unit" ]
                      , HH.input
                          [ HP.type_ HP.InputCheckbox
                          , classes "form-check-input mt-0"
                          , HP.id "attributo-edit-number-standard-unit"
                          , HP.checked standardUnit
                          , HE.onChecked (\newStandardUnit -> update rangeInput suffixInput newStandardUnit)
                          ]
                      ]
                  , HH.input
                      [ HP.type_ HP.InputText
                      , classes ("form-control" <> if isInvalidSuffix suffixNormalized then " is-invalid" else "")
                      , HP.id "attributo-edit-number-suffix"
                      , HP.value suffixInput
                      , HE.onValueChange (\newSuffixInput -> update rangeInput newSuffixInput standardUnit)
                      ]
                  ]
              , HH.div [ classes "form-text" ] [ HH.text "Can be either a non-standard suffix (say “gummibears”) or a standard unit like “MHz” or “N/m^2”." ]
              , case suffixNormalized of
                  NotNeeded -> HH.text ""
                  ValidSuffix normalized -> HH.span [ classes "text-muted" ] [ HH.text "Normalized unit: ", HH.strong_ [ HH.text normalized ] ]
                  InvalidSuffix -> HH.text "Invalid unit"
              ]
          ]
      AttributoAugTypeChoice { valuesInput } ->
        [ renderTextInput
            { id: "attributo-edit-choice-values"
            , name: "Choices"
            , description: [ HH.text "Comma-separated list of string choices" ]
            , value: valuesInput
            , valueChange: (\newChoices -> EditAttributoChange (set _attributoType (AttributoAugTypeChoice { valuesInput: newChoices })))
            }
        ]
  in
    HH.div_
      (renderEnumSimple (attributoAugTypeToEnum t) "type" EditAttributoTypeChange (filter isUserType (enumFromTo bottom top)) : typeSpecificForm)

validateAugAttributo :: forall a. Array (Attributo a) -> Attributo AttributoAugType -> Array String
validateAugAttributo attributiList (Attributo { name, "type": t }) =
  let
    nameIsUnique = not (any (\x -> (unwrap x).name == name) attributiList)
    validateRange' = either (const [ "Invalid range" ]) (const []) <<< parseRange
    validateSuffix' InvalidSuffix = [ "Invalid unit" ]
    validateSuffix' _ = []
    validateSubtype (AttributoAugTypeNumber { rangeInput, suffixNormalized }) = validateRange' rangeInput <> validateSuffix' suffixNormalized
    validateSubtype _ = []
  in
    if name == "" then [ "Name is mandatory" ]
    else if not nameIsUnique then [ "Name is not unique" ]
    else validateSubtype t

renderNewAttributoForm :: forall w a. Array (Attributo a) -> Boolean -> Attributo AttributoAugType -> HH.HTML w Action
renderNewAttributoForm attributiList requestLoading a@(Attributo { name, description, associatedTable, "type": t }) =
  let
    nameIsUnique = not (any (\x -> (unwrap x).name == name) attributiList)
    addButtonEnabled = not requestLoading && null (validateAugAttributo attributiList a)
  in
    HH.form_
      [ HH.h4_ [ HH.text "Add new attributo" ]
      , HH.div [ classes "mb-3" ]
          [ HH.label [ HP.for "attributo-edit-associated-table", classes "form-label" ] [ HH.text "Attributo is for…" ]
          , renderEnumSimple associatedTable "attributo-edit-associated-table" (EditAttributoChange <<< set _associatedTable) (enumFromTo bottom top)
          ]
      , renderTextInput
          { id: "attributo-edit-name"
          , name: "Name"
          , description: [ HH.text "The name is displayed in the table headings and must be unique among all attributi.", HH.br_, HH.text "It cannot contain non-alphanumeric special characters except underscores." ]
          , value: name
          , valueChange: (EditAttributoChange <<< set _name)
          , validationMessage: if nameIsUnique then "" else "This name is already taken."
          }
      , renderTextInput
          { id: "attributo-edit-description"
          , name: "Description"
          , value: description
          , valueChange: (EditAttributoChange <<< set _description)
          }
      , HH.div [ classes "mb-3" ]
          [ HH.label [ HP.for "attributo-edit-type", classes "form-label" ] [ HH.text "Type" ]
          , renderTypeForm t
          ]
      , renderButton
          { icon: Just (mkSimpleIcon "plus")
          , "type": ButtonPrimary
          , label: "Add new attributo"
          , onClick: const EditAttributoSubmit
          , classes: Just "me-3"
          , enabled: addButtonEnabled
          }
      , renderButton
          { icon: Just (mkSimpleIcon "ban")
          , "type": ButtonSecondary
          , label: "Cancel"
          , onClick: const EditAttributoCancel
          , enabled: not requestLoading
          }
      ]

render :: forall m slots. State -> H.ComponentHTML Action slots m
render state =
  case state.attributiList of
    NotAsked -> HH.text ""
    Loading -> HH.text "Loading..."
    Failure e -> htmlizeRequestError e
    Success v ->
      let
        requestResult = remoteDataChurch state.attributoRequest
          (const (HH.text ""))
          (const (HH.text "Sending request..."))
          htmlizeRequestError
          (\_ -> makeAlert AlertSuccess "Request successful!")
      in
        HH.div [ classes "container" ]
          ( ( case state.newAttributo of
                Nothing ->
                  [ HH.div [ classes "mb-3" ]
                      [ renderButton
                          { icon: Just (mkSimpleIcon "plus")
                          , "type": ButtonPrimary
                          , onClick: const AddAttributo
                          , label: "Add attributo"
                          }
                      ]
                  ]
                Just newAttributo -> renderNewAttributoForm v (isLoading state.attributoRequest) newAttributo : [ requestResult ]
            )
              <> [ renderTable state v ]
          )

-- since I'll forget what the hell this stuff is about tomorrow, let me explain:
--
-- 1. with the inner traverse call, we're calling "json schema to attributo type", and afterwards turning
--
--      Attributo (Either x y)
--
--    into
--
--      Either x (Attributo y)
--
-- 2. then, we want to use foldMap to apply the function to an array
--    of Attributo - but that requires a monoid! However, V m1 m2 is a
--    monoid given m1 and m2 are monoids. So we embed both the error and
--    the successful result in an array using singleton.
jsonAttributiToRealAttributi :: Array (Attributo JSONSchema) -> V (Array String) (Array (Attributo AttributoType))
jsonAttributiToRealAttributi = foldMap (V <<< bimap singleton singleton <<< traverse jsonSchemaToAttributoType)

emptyAttributo :: Attributo AttributoAugType
emptyAttributo = Attributo
  { name: ""
  , description: ""
  , associatedTable: Run
  , "type": AttributoAugTypeSimple AttributoTypeString
  }

modifyStateLensy :: forall m x. MonadState x m => (x -> x) -> m Unit
modifyStateLensy lens = H.modify_ \state -> state # lens

readAttributiListAndModifyState :: forall m. Monad m => MonadAff m => MonadState State m => m Unit
readAttributiListAndModifyState = do
  result <- readAttributiList
  case result of
    Left e -> modifyStateLensy (set _attributiList (Failure e))
    Right { attributi } ->
      modifyStateLensy
        ( set _attributiList
            ( validation
                ( \errors -> Failure
                    { code: Nothing
                    , title: "Errors in Attributi response"
                    , description: ("errors converting JSON schema to attributo type: " <> intercalate ", " errors)
                    }
                )
                Success
                (jsonAttributiToRealAttributi attributi)
            )
        )

validateSuffix :: forall m. MonadAff m => MonadState State m => String -> m NormalizedSuffix
validateSuffix input = do
  validated <- checkStandardUnit input
  pure case validated of
    Left _ -> InvalidSuffix
    Right { normalized } -> ValidSuffix normalized

--retrieveNumberSuffix :: Getter' State (Tuple String Boolean)
-- retrieveNumberSuffix =
--   takeBoth
--     (_newAttributo <<< traversed <<< _attributoType <<< _AttributoAugTypeNumber <<< _attributoAugTypeNumberSuffixInput)
--     (_newAttributo <<< traversed <<< _attributoType <<< _AttributoAugTypeNumber <<< _attributoAugTypeNumberStandardUnit)

retrieveNumberSuffix :: State -> Maybe (Tuple String Boolean)
retrieveNumberSuffix state = case state.newAttributo of
  Nothing -> Nothing
  Just newAttributo' -> case newAttributo' ^. _attributoType of
    AttributoAugTypeNumber { standardUnit, suffixInput } -> Just (Tuple suffixInput standardUnit)
    _ -> Nothing

handleAction :: forall output m. MonadAff m => Action -> H.HalogenM State Action () output m Unit

handleAction = case _ of
  AddAttributo -> H.modify_ \state -> state { newAttributo = Just emptyAttributo }
  EditAttributoSubmit -> do
    forM_ (use _newAttributo) \newAttributo' -> do
      modifyStateLensy (_attributoRequest .~ Loading)
      addResult <- addAttributo (attributoAugTypeToType <$> newAttributo')
      case addResult of
        Left e -> modifyStateLensy (_attributoRequest .~ Failure e)
        Right _ -> do
          modifyStateLensy (_attributoRequest .~ Success {})
          readAttributiListAndModifyState
  EditAttributoCancel -> modifyStateLensy (_newAttributo .~ Nothing)
  EditAttributoChange changer -> do
    suffixBefore <- H.gets retrieveNumberSuffix
    modifyStateLensy ((_newAttributo <<< traversed) %~ changer)
    suffixAfter <- H.gets retrieveNumberSuffix
    newSuffix <- case Tuple suffixBefore suffixAfter of
      Tuple Nothing (Just (Tuple suffixInput true)) -> (Just <$> validateSuffix suffixInput)
      Tuple (Just (Tuple suffixInputBefore standardUnitBefore)) (Just (Tuple suffixInputAfter true)) ->
        if not standardUnitBefore || suffixInputAfter /= suffixInputBefore then Just <$> validateSuffix suffixInputAfter
        else pure Nothing
      _ -> do
        modifyStateLensy (set (_newAttributo <<< traversed <<< _attributoType <<< _AttributoAugTypeNumber <<< _attributoAugTypeNumberSuffixNormalized) NotNeeded)
        pure Nothing
    for_ newSuffix \newSuffix' -> modifyStateLensy (set (_newAttributo <<< traversed <<< _attributoType <<< _AttributoAugTypeNumber <<< _attributoAugTypeNumberSuffixNormalized) newSuffix')

  EditAttributoTypeChange newType -> modifyStateLensy ((_newAttributo <<< traversed <<< _attributoType) .~ (initialAttributoAugType newType))
  Initialize -> do
    H.modify_ \state -> state { attributiList = Loading }
    readAttributiListAndModifyState

attributiComponent :: forall q i o m. MonadAff m => H.Component q i o m
attributiComponent =
  H.mkComponent
    { initialState: \_ -> { attributiList: NotAsked, newAttributo: Nothing, attributoRequest: NotAsked }
    , render
    , eval: H.mkEval H.defaultEval { handleAction = handleAction, initialize = Just Initialize }
    }
