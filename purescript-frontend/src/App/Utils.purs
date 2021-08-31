module App.Utils where

import Data.Argonaut.Core as Argonaut
import Data.Array (find, head, uncons)
import Data.Boolean (otherwise)
import Data.Either (Either, either)
import Data.Eq ((==))
import Data.Filterable (filter)
import Data.Foldable (foldMap)
import Data.FoldableWithIndex (foldrWithIndex)
import Data.Functor (class Functor)
import Data.Int (fromString)
import Data.List ((:))
import Data.Map (Map)
import Data.Maybe (Maybe(..), maybe)
import Data.Monoid (class Monoid, mempty)
import Data.Ord (class Ord, (>=))
import Data.Set (Set, delete, insert, member)
import Data.String (length, take)
import Data.Tuple (Tuple(..))
import Foreign.Object (Object, fromFoldable)
import Prelude (class Apply, class Eq, eq, identity, map, (<$>), (<*>), (>>>))

type Endo a = a -> a

foldMap' :: forall a m. Monoid m => Maybe a -> (a -> m) -> m
foldMap' m f = foldMap f m

fanoutApplicative :: forall t13 t6 t8. Apply t6 => t6 t13 -> t6 t8 -> t6 (Tuple t13 t8)
fanoutApplicative x' y' = Tuple <$> x' <*> y'

findCascade :: forall a b. Eq b => Array a -> (a -> b) -> Array b -> Maybe a
findCascade [] _ _ = Nothing

findCascade xs f bs = case uncons bs of
  Nothing -> head xs
  Just { head: b, tail: bs' } -> case find (f >>> eq b) xs of
    Just v -> Just v
    Nothing -> findCascade xs f bs'

mapTuple :: forall a b f. Functor f => (a -> b) -> f a -> f (Tuple a b)
mapTuple f = map (\x -> Tuple x (f x))

toggleSetElement :: forall a. Ord a => a -> Set a -> Set a
toggleSetElement v x
  | member v x = delete v x
  | otherwise = insert v x

fromStringPositive :: String -> Maybe Int
fromStringPositive = fromString >>> filter (_ >= 0)

startsWith :: String -> String -> Boolean
startsWith haystack needle = take (length needle) haystack == needle

stringMaybeMapToJsonObject :: Map String (Maybe String) -> Object Argonaut.Json
stringMaybeMapToJsonObject m =
  let folder k v prevList = Tuple k (maybe Argonaut.jsonNull Argonaut.fromString v) : prevList
  in fromFoldable (foldrWithIndex folder mempty m)

stringMapToJsonObject :: Map String String -> Object Argonaut.Json
stringMapToJsonObject m =
  let folder k v prevList = Tuple k (Argonaut.fromString v) : prevList
  in fromFoldable (foldrWithIndex folder mempty m)


dehomoEither :: forall a. Either a a -> a
dehomoEither = either identity identity
