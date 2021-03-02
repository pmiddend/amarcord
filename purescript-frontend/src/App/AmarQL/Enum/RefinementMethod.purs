module App.AmarQL.Enum.RefinementMethod where

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

-- | original name - RefinementMethod
data RefinementMethod = Hzb | Dmpl | Dmpl2 | Dmpl2Aligned | Dmpl2Qfit

derive instance genericRefinementMethod :: Generic RefinementMethod _

instance showRefinementMethod :: Show RefinementMethod where
  show = genericShow

derive instance eqRefinementMethod :: Eq RefinementMethod

derive instance ordRefinementMethod :: Ord RefinementMethod

fromToMap :: Array (Tuple String RefinementMethod)
fromToMap = [ Tuple "HZB" Hzb
            , Tuple "DMPL" Dmpl
            , Tuple "DMPL2" Dmpl2
            , Tuple "DMPL2_ALIGNED" Dmpl2Aligned
            , Tuple "DMPL2_QFIT" Dmpl2Qfit
            ]

instance refinementMethodGraphQLDefaultResponseScalarDecoder :: GraphQLDefaultResponseScalarDecoder
                                                                RefinementMethod where
  graphqlDefaultResponseScalarDecoder = enumDecoder "RefinementMethod" fromToMap

instance refinementMethodToGraphQLArgumentValue :: ToGraphQLArgumentValue
                                                   RefinementMethod where
  toGraphQLArgumentValue =
    case _ of
      Hzb -> ArgumentValueEnum "HZB"
      Dmpl -> ArgumentValueEnum "DMPL"
      Dmpl2 -> ArgumentValueEnum "DMPL2"
      Dmpl2Aligned -> ArgumentValueEnum "DMPL2_ALIGNED"
      Dmpl2Qfit -> ArgumentValueEnum "DMPL2_QFIT"
