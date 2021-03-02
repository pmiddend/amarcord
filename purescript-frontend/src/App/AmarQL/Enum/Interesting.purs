module App.AmarQL.Enum.Interesting where

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

-- | original name - Interesting
data Interesting = Yes | No

derive instance genericInteresting :: Generic Interesting _

instance showInteresting :: Show Interesting where
  show = genericShow

derive instance eqInteresting :: Eq Interesting

derive instance ordInteresting :: Ord Interesting

fromToMap :: Array (Tuple String Interesting)
fromToMap = [ Tuple "YES" Yes, Tuple "NO" No ]

instance interestingGraphQLDefaultResponseScalarDecoder :: GraphQLDefaultResponseScalarDecoder
                                                           Interesting where
  graphqlDefaultResponseScalarDecoder = enumDecoder "Interesting" fromToMap

instance interestingToGraphQLArgumentValue :: ToGraphQLArgumentValue Interesting where
  toGraphQLArgumentValue =
    case _ of
      Yes -> ArgumentValueEnum "YES"
      No -> ArgumentValueEnum "NO"
