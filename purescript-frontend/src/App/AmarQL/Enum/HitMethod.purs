module App.AmarQL.Enum.HitMethod where

import Data.Generic.Rep (class Generic)
import Data.Show (class Show)
import Data.Generic.Rep.Show (genericShow)
import Prelude (class Eq, class Ord)
import Data.Tuple (Tuple(..))
import GraphQLClient
  ( class GraphQLDefaultResponseScalarDecoder
  , enumDecoder
  , class ToGraphQLArgumentValue
  , ArgumentValue(..)
  )

-- | original name - HitMethod
data HitMethod = Pandda | DimpleBlob | Manual

derive instance genericHitMethod :: Generic HitMethod _

instance showHitMethod :: Show HitMethod where
  show = genericShow

derive instance eqHitMethod :: Eq HitMethod

derive instance ordHitMethod :: Ord HitMethod

fromToMap :: Array (Tuple String HitMethod)
fromToMap = [ Tuple "PANDDA" Pandda
            , Tuple "DIMPLE_BLOB" DimpleBlob
            , Tuple "MANUAL" Manual
            ]

instance hitMethodGraphQLDefaultResponseScalarDecoder :: GraphQLDefaultResponseScalarDecoder
                                                         HitMethod where
  graphqlDefaultResponseScalarDecoder = enumDecoder "HitMethod" fromToMap

instance hitMethodToGraphQLArgumentValue :: ToGraphQLArgumentValue HitMethod where
  toGraphQLArgumentValue =
    case _ of
      Pandda -> ArgumentValueEnum "PANDDA"
      DimpleBlob -> ArgumentValueEnum "DIMPLE_BLOB"
      Manual -> ArgumentValueEnum "MANUAL"
