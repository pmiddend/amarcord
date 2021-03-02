module App.AmarQL.Object.Plasmid where

import GraphQLClient
  ( SelectionSet
  , selectionForField
  , graphqlDefaultResponseScalarDecoder
  , selectionForCompositeField
  , graphqlDefaultResponseFunctorOrScalarDecoderTransformer
  )
import App.AmarQL.Scopes (Scope__Plasmid, Scope__Target)
import Data.Maybe (Maybe)
import App.AmarQL.Scalars (DateTime)

plasmidId :: SelectionSet Scope__Plasmid String
plasmidId = selectionForField "plasmidId" [] graphqlDefaultResponseScalarDecoder

created :: SelectionSet Scope__Plasmid (Maybe DateTime)
created = selectionForField "created" [] graphqlDefaultResponseScalarDecoder

name :: SelectionSet Scope__Plasmid String
name = selectionForField "name" [] graphqlDefaultResponseScalarDecoder

expression :: SelectionSet Scope__Plasmid (Maybe String)
expression = selectionForField
             "expression"
             []
             graphqlDefaultResponseScalarDecoder

purification :: SelectionSet Scope__Plasmid (Maybe String)
purification = selectionForField
               "purification"
               []
               graphqlDefaultResponseScalarDecoder

crystallization :: SelectionSet Scope__Plasmid (Maybe String)
crystallization = selectionForField
                  "crystallization"
                  []
                  graphqlDefaultResponseScalarDecoder

expressionSystem :: SelectionSet Scope__Plasmid (Maybe String)
expressionSystem = selectionForField
                   "expressionSystem"
                   []
                   graphqlDefaultResponseScalarDecoder

expressionLab :: SelectionSet Scope__Plasmid (Maybe String)
expressionLab = selectionForField
                "expressionLab"
                []
                graphqlDefaultResponseScalarDecoder

expressionProtocol :: SelectionSet Scope__Plasmid (Maybe String)
expressionProtocol = selectionForField
                     "expressionProtocol"
                     []
                     graphqlDefaultResponseScalarDecoder

targets :: forall r . SelectionSet
                      Scope__Target
                      r -> SelectionSet
                           Scope__Plasmid
                           (Maybe
                            (Array
                             (Maybe
                              r)))
targets = selectionForCompositeField
          "targets"
          []
          graphqlDefaultResponseFunctorOrScalarDecoderTransformer
