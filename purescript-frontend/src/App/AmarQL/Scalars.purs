module App.AmarQL.Scalars where

import Data.Newtype (class Newtype)
import Prelude (class Eq, class Ord, class Show)
import GraphQLClient
  (class GraphQLDefaultResponseScalarDecoder, class ToGraphQLArgumentValue)

-- | original name - DateTime
newtype DateTime = DateTime String

derive instance newtypeDateTime :: Newtype DateTime _

derive newtype instance eqDateTime :: Eq DateTime

derive newtype instance ordDateTime :: Ord DateTime

derive newtype instance showDateTime :: Show DateTime

derive newtype instance graphQlDefaultResponseScalarDecoderDateTime :: GraphQLDefaultResponseScalarDecoder DateTime

derive newtype instance toGraphQlArgumentValueDateTime :: ToGraphQLArgumentValue DateTime

-- | original name - ID
newtype Id = Id String

derive instance newtypeId :: Newtype Id _

derive newtype instance eqId :: Eq Id

derive newtype instance ordId :: Ord Id

derive newtype instance showId :: Show Id

derive newtype instance graphQlDefaultResponseScalarDecoderId :: GraphQLDefaultResponseScalarDecoder Id

derive newtype instance toGraphQlArgumentValueId :: ToGraphQLArgumentValue Id
