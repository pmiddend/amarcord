module App.Utils where

import Data.Foldable (foldMap)
import Data.Maybe (Maybe)
import Data.Monoid (class Monoid)

foldMap' :: forall a m. Monoid m => Maybe a -> (a -> m) -> m
foldMap' m f = foldMap f m

