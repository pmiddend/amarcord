module App.AmarQL.Enum.ReductionMethod where

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

-- | original name - ReductionMethod
data ReductionMethod
  = XdsPre
  | XdsFull
  | XdsReindex1
  | XdsReinder1Noice
  | DialsDials
  | Dials1P7ADials
  | Staraniso
  | Other

derive instance genericReductionMethod :: Generic ReductionMethod _

instance showReductionMethod :: Show ReductionMethod where
  show = genericShow

derive instance eqReductionMethod :: Eq ReductionMethod

derive instance ordReductionMethod :: Ord ReductionMethod

fromToMap :: Array (Tuple String ReductionMethod)
fromToMap = [ Tuple "XDS_PRE" XdsPre
            , Tuple "XDS_FULL" XdsFull
            , Tuple "XDS_REINDEX1" XdsReindex1
            , Tuple "XDS_REINDER1_NOICE" XdsReinder1Noice
            , Tuple "DIALS_DIALS" DialsDials
            , Tuple "DIALS_1_P7_A_DIALS" Dials1P7ADials
            , Tuple "STARANISO" Staraniso
            , Tuple "OTHER" Other
            ]

instance reductionMethodGraphQLDefaultResponseScalarDecoder :: GraphQLDefaultResponseScalarDecoder
                                                               ReductionMethod where
  graphqlDefaultResponseScalarDecoder = enumDecoder "ReductionMethod" fromToMap

instance reductionMethodToGraphQLArgumentValue :: ToGraphQLArgumentValue
                                                  ReductionMethod where
  toGraphQLArgumentValue =
    case _ of
      XdsPre -> ArgumentValueEnum "XDS_PRE"
      XdsFull -> ArgumentValueEnum "XDS_FULL"
      XdsReindex1 -> ArgumentValueEnum "XDS_REINDEX1"
      XdsReinder1Noice -> ArgumentValueEnum "XDS_REINDER1_NOICE"
      DialsDials -> ArgumentValueEnum "DIALS_DIALS"
      Dials1P7ADials -> ArgumentValueEnum "DIALS_1_P7_A_DIALS"
      Staraniso -> ArgumentValueEnum "STARANISO"
      Other -> ArgumentValueEnum "OTHER"
