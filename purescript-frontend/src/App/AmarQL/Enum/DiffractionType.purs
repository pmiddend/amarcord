module App.AmarQL.Enum.DiffractionType where

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

-- | original name - DiffractionType
data DiffractionType = NoDiffraction | NoCrystal | IceSalt | Success

derive instance genericDiffractionType :: Generic DiffractionType _

instance showDiffractionType :: Show DiffractionType where
  show = genericShow

derive instance eqDiffractionType :: Eq DiffractionType

derive instance ordDiffractionType :: Ord DiffractionType

fromToMap :: Array (Tuple String DiffractionType)
fromToMap = [ Tuple "NO_DIFFRACTION" NoDiffraction
            , Tuple "NO_CRYSTAL" NoCrystal
            , Tuple "ICE_SALT" IceSalt
            , Tuple "SUCCESS" Success
            ]

instance diffractionTypeGraphQLDefaultResponseScalarDecoder :: GraphQLDefaultResponseScalarDecoder
                                                               DiffractionType where
  graphqlDefaultResponseScalarDecoder = enumDecoder "DiffractionType" fromToMap

instance diffractionTypeToGraphQLArgumentValue :: ToGraphQLArgumentValue
                                                  DiffractionType where
  toGraphQLArgumentValue =
    case _ of
      NoDiffraction -> ArgumentValueEnum "NO_DIFFRACTION"
      NoCrystal -> ArgumentValueEnum "NO_CRYSTAL"
      IceSalt -> ArgumentValueEnum "ICE_SALT"
      Success -> ArgumentValueEnum "SUCCESS"
