module App.AmarQL.InputObject where

import GraphQLClient
  (Optional, class ToGraphQLArgumentValue, toGraphQLArgumentValue)
import Data.Generic.Rep (class Generic)
import Data.Newtype (class Newtype)

-- | original name - HitInput
newtype HitInput = HitInput { hitId :: Optional String
                            , refinementId :: String
                            , method :: String
                            , comment :: String
                            , bindingSite :: String
                            , confidence :: String
                            , isInteresting :: Boolean
                            , inspectedBy :: String
                            , metadataColumn :: String
                            }

derive instance genericHitInput :: Generic HitInput _

derive instance newtypeHitInput :: Newtype HitInput _

instance toGraphQLArgumentValueHitInput :: ToGraphQLArgumentValue HitInput where
  toGraphQLArgumentValue (HitInput x) = toGraphQLArgumentValue x
