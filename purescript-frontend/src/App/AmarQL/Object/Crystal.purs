module App.AmarQL.Object.Crystal where

import GraphQLClient
  ( SelectionSet
  , selectionForField
  , graphqlDefaultResponseScalarDecoder
  , selectionForCompositeField
  , graphqlDefaultResponseFunctorOrScalarDecoderTransformer
  )
import App.AmarQL.Scopes
  (Scope__Crystal, Scope__Target, Scope__CompoundDrop, Scope__Diffraction)
import Data.Maybe (Maybe)
import App.AmarQL.Scalars (DateTime)

crystalId :: SelectionSet Scope__Crystal String
crystalId = selectionForField "crystalId" [] graphqlDefaultResponseScalarDecoder

created :: SelectionSet Scope__Crystal (Maybe DateTime)
created = selectionForField "created" [] graphqlDefaultResponseScalarDecoder

targetId :: SelectionSet Scope__Crystal (Maybe String)
targetId = selectionForField "targetId" [] graphqlDefaultResponseScalarDecoder

compoundDropId :: SelectionSet Scope__Crystal (Maybe Int)
compoundDropId = selectionForField
                 "compoundDropId"
                 []
                 graphqlDefaultResponseScalarDecoder

puckId :: SelectionSet Scope__Crystal (Maybe String)
puckId = selectionForField "puckId" [] graphqlDefaultResponseScalarDecoder

puckPositionId :: SelectionSet Scope__Crystal (Maybe Int)
puckPositionId = selectionForField
                 "puckPositionId"
                 []
                 graphqlDefaultResponseScalarDecoder

creatorId :: SelectionSet Scope__Crystal String
creatorId = selectionForField "creatorId" [] graphqlDefaultResponseScalarDecoder

target :: forall r . SelectionSet
                     Scope__Target
                     r -> SelectionSet
                          Scope__Crystal
                          (Maybe
                           r)
target = selectionForCompositeField
         "target"
         []
         graphqlDefaultResponseFunctorOrScalarDecoderTransformer

compoundDrop :: forall r . SelectionSet
                           Scope__CompoundDrop
                           r -> SelectionSet
                                Scope__Crystal
                                (Maybe
                                 r)
compoundDrop = selectionForCompositeField
               "compoundDrop"
               []
               graphqlDefaultResponseFunctorOrScalarDecoderTransformer

diffractions :: forall r . SelectionSet
                           Scope__Diffraction
                           r -> SelectionSet
                                Scope__Crystal
                                (Maybe
                                 (Array
                                  (Maybe
                                   r)))
diffractions = selectionForCompositeField
               "diffractions"
               []
               graphqlDefaultResponseFunctorOrScalarDecoderTransformer
