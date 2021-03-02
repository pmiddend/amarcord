module App.AmarQL.Enum.BindingSite where

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

-- | original name - BindingSite
data BindingSite = ActivePocket | OtherPocket | CrystalInterface | Surface

derive instance genericBindingSite :: Generic BindingSite _

instance showBindingSite :: Show BindingSite where
  show = genericShow

derive instance eqBindingSite :: Eq BindingSite

derive instance ordBindingSite :: Ord BindingSite

fromToMap :: Array (Tuple String BindingSite)
fromToMap = [ Tuple "ACTIVE_POCKET" ActivePocket
            , Tuple "OTHER_POCKET" OtherPocket
            , Tuple "CRYSTAL_INTERFACE" CrystalInterface
            , Tuple "SURFACE" Surface
            ]

instance bindingSiteGraphQLDefaultResponseScalarDecoder :: GraphQLDefaultResponseScalarDecoder
                                                           BindingSite where
  graphqlDefaultResponseScalarDecoder = enumDecoder "BindingSite" fromToMap

instance bindingSiteToGraphQLArgumentValue :: ToGraphQLArgumentValue BindingSite where
  toGraphQLArgumentValue =
    case _ of
      ActivePocket -> ArgumentValueEnum "ACTIVE_POCKET"
      OtherPocket -> ArgumentValueEnum "OTHER_POCKET"
      CrystalInterface -> ArgumentValueEnum "CRYSTAL_INTERFACE"
      Surface -> ArgumentValueEnum "SURFACE"
