module App.AmarQL.Object.CompoundDrop where

import GraphQLClient
  ( SelectionSet
  , selectionForField
  , graphqlDefaultResponseScalarDecoder
  , selectionForCompositeField
  , graphqlDefaultResponseFunctorOrScalarDecoderTransformer
  )
import App.AmarQL.Scopes (Scope__CompoundDrop, Scope__Crystal, Scope__Compound)
import App.AmarQL.Scalars (Id, DateTime)
import Data.Maybe (Maybe)

compoundDropId :: SelectionSet Scope__CompoundDrop Id
compoundDropId = selectionForField
                 "compoundDropId"
                 []
                 graphqlDefaultResponseScalarDecoder

created :: SelectionSet Scope__CompoundDrop (Maybe DateTime)
created = selectionForField "created" [] graphqlDefaultResponseScalarDecoder

compoundId :: SelectionSet Scope__CompoundDrop String
compoundId = selectionForField
             "compoundId"
             []
             graphqlDefaultResponseScalarDecoder

wellId :: SelectionSet Scope__CompoundDrop (Maybe String)
wellId = selectionForField "wellId" [] graphqlDefaultResponseScalarDecoder

crystalTrayId :: SelectionSet Scope__CompoundDrop (Maybe String)
crystalTrayId = selectionForField
                "crystalTrayId"
                []
                graphqlDefaultResponseScalarDecoder

dropId :: SelectionSet Scope__CompoundDrop (Maybe Int)
dropId = selectionForField "dropId" [] graphqlDefaultResponseScalarDecoder

crystal :: forall r . SelectionSet
                      Scope__Crystal
                      r -> SelectionSet
                           Scope__CompoundDrop
                           (Maybe
                            (Array
                             (Maybe
                              r)))
crystal = selectionForCompositeField
          "crystal"
          []
          graphqlDefaultResponseFunctorOrScalarDecoderTransformer

compound :: forall r . SelectionSet
                       Scope__Compound
                       r -> SelectionSet
                            Scope__CompoundDrop
                            (Maybe
                             r)
compound = selectionForCompositeField
           "compound"
           []
           graphqlDefaultResponseFunctorOrScalarDecoderTransformer
