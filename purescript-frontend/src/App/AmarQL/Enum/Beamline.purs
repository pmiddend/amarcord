module App.AmarQL.Enum.Beamline where

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

-- | original name - Beamline
data Beamline = P11 | P13 | P14

derive instance genericBeamline :: Generic Beamline _

instance showBeamline :: Show Beamline where
  show = genericShow

derive instance eqBeamline :: Eq Beamline

derive instance ordBeamline :: Ord Beamline

fromToMap :: Array (Tuple String Beamline)
fromToMap = [ Tuple "P11" P11, Tuple "P13" P13, Tuple "P14" P14 ]

instance beamlineGraphQLDefaultResponseScalarDecoder :: GraphQLDefaultResponseScalarDecoder
                                                        Beamline where
  graphqlDefaultResponseScalarDecoder = enumDecoder "Beamline" fromToMap

instance beamlineToGraphQLArgumentValue :: ToGraphQLArgumentValue Beamline where
  toGraphQLArgumentValue =
    case _ of
      P11 -> ArgumentValueEnum "P11"
      P13 -> ArgumentValueEnum "P13"
      P14 -> ArgumentValueEnum "P14"
