module App.Utils where

import Data.Array (find, head, uncons)
import Data.Foldable (foldMap)
import Data.Functor (class Functor)
import Data.Maybe (Maybe(..))
import Data.Monoid (class Monoid)
import Data.Tuple (Tuple(..))
import Prelude (class Apply, class Eq, eq, map, (<$>), (<*>), (>>>))

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
