module App.AttributoUtil where

-- import App.Attributo (Attributo, _name, _typeSchema)
-- import App.AttributoValue (AttributoValue(..))
-- import App.JSONSchemaType (JSONNumberData, JSONSchemaType(..))
-- import App.Run (Source)
-- import App.Utils (findCascade, mapTuple)
-- import Control.Bind ((>>=))
-- import Control.Category ((<<<))
-- import Data.Array (mapMaybe)
-- import Data.Either (Either(..))
-- import Data.Int (toNumber)
-- import Data.Lens (Lens', (^.))
-- import Data.Lens.Record (prop)
-- import Data.List (List(..), toUnfoldable)
-- import Data.Map (Map, values)
-- import Data.Maybe (Maybe(..))
-- import Data.Monoid ((<>))
-- import Data.Show (show)
-- import Data.Symbol (SProxy(..))
-- import Data.Traversable (foldr, traverse)
-- import Data.Tuple (Tuple(..), fst)
-- import Foreign.Object as FO

-- type Attributi = FO.Object (FO.Object AttributoValue)

-- data PairedAttributo
--   = PairedNumber JSONNumberData Number
--   | PairedString String
--   | PairedInteger Int

-- type SourcedValue a
--   = { source :: Source
--     , value :: a
--     }

-- _source :: forall a. Lens' (SourcedValue a) Source
-- _source = prop (SProxy :: SProxy "source")

-- _value :: forall a. Lens' (SourcedValue a) a
-- _value = prop (SProxy :: SProxy "value")

-- type ProcessedAttributo
--   = { attributo :: Attributo, value :: Maybe (SourcedValue PairedAttributo) }

-- createPair :: Attributo -> AttributoValue -> Either String PairedAttributo
-- createPair a x = case Tuple (a ^. _typeSchema) x of
--   Tuple (JSONNumber numberData) (NumericAttributo xv) -> Right (PairedNumber numberData xv)
--   -- This is fine: we accept integers when a number is requested (but not the other way round!)
--   Tuple (JSONNumber numberData) (IntegralAttributo xv) -> Right (PairedNumber numberData (toNumber xv))
--   Tuple JSONString (StringAttributo xv) -> Right (PairedString xv)
--   Tuple JSONInteger (IntegralAttributo xv) -> Right (PairedInteger xv)
--   Tuple json _ -> Left ("invalid attributo " <> (a ^. _name) <> ": json type " <> show json <> ", value type " <> show x)

-- locateAttributo :: String -> Attributi -> Array (Tuple Source AttributoValue)
-- locateAttributo attributoName = mapMaybe (traverse (FO.lookup attributoName)) <<<  FO.toUnfoldable


-- attributiAndErrors :: Map String Attributo -> Attributi -> { errors :: List String, attributi :: List { attributo :: Attributo, value :: Maybe (SourcedValue PairedAttributo) } }
-- attributiAndErrors availableAttributi inputAttributi =
--   let
--     makeValue :: Attributo -> Maybe (SourcedValue (Either String PairedAttributo))
--     makeValue type_ =
--       findCascade
--         (locateAttributo (type_ ^. _name) inputAttributi)
--         fst
--         [ "manual", "offline", "online" ]
--         >>= \(Tuple source value) -> Just { source, value: createPair type_ value }

--     arrayOfAll :: Array (Tuple Attributo (Maybe (SourcedValue (Either String PairedAttributo))))
--     arrayOfAll = mapTuple makeValue (toUnfoldable (values availableAttributi))

--     f :: Tuple Attributo (Maybe (SourcedValue (Either String PairedAttributo))) -> { errors :: List String, attributi :: List ProcessedAttributo } -> { errors :: List String, attributi :: List ProcessedAttributo }
--     f (Tuple a Nothing) { errors, attributi } = { errors, attributi: Cons ({ attributo: a, value: Nothing }) attributi }

--     f (Tuple a (Just { value: Left e })) { errors, attributi } = { errors: Cons e errors, attributi }

--     f (Tuple a (Just { value: Right v, source })) { errors, attributi } = { errors, attributi: Cons ({ attributo: a, value: Just { source, value: v } }) attributi }
--   in
--     foldr f { errors: Nil, attributi: Nil } arrayOfAll
