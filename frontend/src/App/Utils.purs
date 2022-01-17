module App.Utils where

import Control.Monad (class Monad, bind)
import Data.Argonaut.Core as Argonaut
import Data.Array (find, groupAllBy, head, uncons, (:))
import Data.Array.NonEmpty as NE
import Data.Boolean (otherwise)
import Data.Either (Either(..), either)
import Data.Eq ((==))
import Data.Filterable (class Filterable, filter, partitionMap)
import Data.Foldable (foldMap, class Foldable)
import Data.FoldableWithIndex (foldrWithIndex)
import Data.Function ((<<<))
import Data.Functor (class Functor)
import Data.Int (fromString)
import Data.List as List
import Data.Map (Map)
import Data.Maybe (Maybe(..), maybe)
import Data.Monoid (class Monoid, mempty)
import Data.Ord (class Ord, comparing, (<=), (>=))
import Data.Ring ((-))
import Data.Semigroup (append)
import Data.Set (Set, delete, insert, member)
import Data.String (CodePoint)
import Data.String as String
import Data.Traversable (for_)
import Data.Tuple (Tuple(..))
import Data.Unfoldable (singleton, class Unfoldable)
import Data.Unit (Unit, unit)
import Foreign.Object (Object, fromFoldable)
import Network.RemoteData (RemoteData(..))
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
startsWith haystack needle = String.take (String.length needle) haystack == needle

stringMaybeMapToJsonObject :: Map String (Maybe String) -> Object Argonaut.Json
stringMaybeMapToJsonObject m =
  let folder k v prevList = Tuple k (maybe Argonaut.jsonNull Argonaut.fromString v) List.: prevList
  in fromFoldable (foldrWithIndex folder mempty m)

stringMapToJsonObject :: Map String String -> Object Argonaut.Json
stringMapToJsonObject m =
  let folder k v prevList = Tuple k (Argonaut.fromString v) List.: prevList
  in fromFoldable (foldrWithIndex folder mempty m)


dehomoEither :: forall a. Either a a -> a
dehomoEither = either identity identity

pairWithNextList :: forall a. List.List a -> List.List (Tuple a (Maybe a))
pairWithNextList x = case List.uncons x of
  Nothing -> mempty
  Just { head: first, tail } ->
    case List.uncons tail of
      Nothing -> List.singleton (Tuple first Nothing)
      Just { head: second, tail: _tail } -> Tuple first (Just second) List.: pairWithNextList tail

pairWithNextNonEmpty :: forall a. NE.NonEmptyArray a -> NE.NonEmptyArray (Tuple a (Maybe a))
pairWithNextNonEmpty x = case NE.uncons x of
  { head: first, tail } -> case uncons tail of
      Nothing -> NE.singleton (Tuple first Nothing)
      Just { head: second, tail: _tail } -> NE.cons' (Tuple first (Just second)) (pairWithNext tail)
          
pairWithNext :: forall a. Array a -> Array (Tuple a (Maybe a))
pairWithNext x = case uncons x of
  Nothing -> mempty
  Just { head: first, tail } ->
    case uncons tail of
      Nothing -> singleton (Tuple first Nothing)
      Just { head: second, tail: _tail } -> Tuple first (Just second) : pairWithNext tail

groupAllByTuple :: forall a b. Ord b => (a -> b) -> Array a -> Array (Tuple b (NE.NonEmptyArray a))
groupAllByTuple f xs = (\group -> Tuple (f (NE.head group)) group) <$> groupAllBy (comparing f) xs

stringCons :: CodePoint -> String -> String
stringCons = append <<< String.singleton

stringReplicate :: Int -> CodePoint -> String
stringReplicate = replicate' ""
  where
    replicate' acc n c | n <= 0 = acc
                       | otherwise = replicate' (stringCons c acc) (n - 1) c

mapEither :: forall f a r l. Filterable f => Monoid (f l) => Eq (f l) => (a -> Either l r) -> f a -> Either (f l) (f r)
mapEither f xs =
  let
    { left, right } = partitionMap f xs
  in
    if left == mempty then Right right
    else Left left

maybeSingleton :: forall a x f g. Monoid (f a) => Unfoldable f => Foldable g => (x -> a) -> g x -> f a
maybeSingleton f = foldMap (singleton <<< f)

remoteDataChurch :: forall e a x. RemoteData e a -> (Unit -> x) -> (Unit -> x) -> (e -> x) -> (a -> x) -> x
remoteDataChurch NotAsked notAsked _loading _failure _success = notAsked unit
remoteDataChurch Loading _notAsked loading _failure _success = loading unit
remoteDataChurch (Failure e) _notAsked _loading failure _success = failure e
remoteDataChurch (Success a) _notAsked _loading _failure success = success a

emptyStringToMaybe :: String -> Maybe String
emptyStringToMaybe "" = Nothing
emptyStringToMaybe x = Just x

forM_ :: forall m a b. Monad m => m (Maybe a) -> (a -> m b) -> m Unit
forM_ v f = do
  r <- v
  for_ r f

