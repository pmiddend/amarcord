module App.AmarQL.Object.Target where

import GraphQLClient
  ( SelectionSet
  , selectionForField
  , graphqlDefaultResponseScalarDecoder
  , selectionForCompositeField
  , graphqlDefaultResponseFunctorOrScalarDecoderTransformer
  )
import App.AmarQL.Scopes (Scope__Target, Scope__Crystal, Scope__Plasmid)
import Data.Maybe (Maybe)
import App.AmarQL.Scalars (DateTime)

targetId :: SelectionSet Scope__Target String
targetId = selectionForField "targetId" [] graphqlDefaultResponseScalarDecoder

created :: SelectionSet Scope__Target (Maybe DateTime)
created = selectionForField "created" [] graphqlDefaultResponseScalarDecoder

name :: SelectionSet Scope__Target String
name = selectionForField "name" [] graphqlDefaultResponseScalarDecoder

crystals :: forall r . SelectionSet
                       Scope__Crystal
                       r -> SelectionSet
                            Scope__Target
                            (Maybe
                             (Array
                              (Maybe
                               r)))
crystals = selectionForCompositeField
           "crystals"
           []
           graphqlDefaultResponseFunctorOrScalarDecoderTransformer

plasmids :: forall r . SelectionSet
                       Scope__Plasmid
                       r -> SelectionSet
                            Scope__Target
                            (Maybe
                             (Array
                              (Maybe
                               r)))
plasmids = selectionForCompositeField
           "plasmids"
           []
           graphqlDefaultResponseFunctorOrScalarDecoderTransformer
