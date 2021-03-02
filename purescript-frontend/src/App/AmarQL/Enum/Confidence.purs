module App.AmarQL.Enum.Confidence where

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

-- | original name - Confidence
data Confidence = Low | Medium | High

derive instance genericConfidence :: Generic Confidence _

instance showConfidence :: Show Confidence where
  show = genericShow

derive instance eqConfidence :: Eq Confidence

derive instance ordConfidence :: Ord Confidence

fromToMap :: Array (Tuple String Confidence)
fromToMap = [ Tuple "LOW" Low, Tuple "MEDIUM" Medium, Tuple "HIGH" High ]

instance confidenceGraphQLDefaultResponseScalarDecoder :: GraphQLDefaultResponseScalarDecoder
                                                          Confidence where
  graphqlDefaultResponseScalarDecoder = enumDecoder "Confidence" fromToMap

instance confidenceToGraphQLArgumentValue :: ToGraphQLArgumentValue Confidence where
  toGraphQLArgumentValue =
    case _ of
      Low -> ArgumentValueEnum "LOW"
      Medium -> ArgumentValueEnum "MEDIUM"
      High -> ArgumentValueEnum "HIGH"
