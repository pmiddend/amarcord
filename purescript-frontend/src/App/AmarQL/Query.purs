module App.AmarQL.Query where

import GraphQLClient
  ( Optional
  , SelectionSet
  , Scope__RootQuery
  , selectionForCompositeField
  , toGraphQLArguments
  , graphqlDefaultResponseFunctorOrScalarDecoderTransformer
  )
import Type.Row (type (+))
import App.AmarQL.Scopes (Scope__Crystal, Scope__DataReduction, Scope__Hit)
import Data.Maybe (Maybe)

type CrystalsInputRowOptional r = ( crystalId :: Optional String | r )

type CrystalsInput = { | CrystalsInputRowOptional + () }

crystals :: forall r . CrystalsInput -> SelectionSet
                                        Scope__Crystal
                                        r -> SelectionSet
                                             Scope__RootQuery
                                             (Array
                                              r)
crystals input = selectionForCompositeField
                 "crystals"
                 (toGraphQLArguments
                  input)
                 graphqlDefaultResponseFunctorOrScalarDecoderTransformer

type ReductionsInputRowRequired r = ( crystalId :: String
                                    , runIds :: Array String
                                    | r
                                    )

type ReductionsInput = { | ReductionsInputRowRequired + () }

reductions :: forall r . ReductionsInput -> SelectionSet
                                            Scope__DataReduction
                                            r -> SelectionSet
                                                 Scope__RootQuery
                                                 (Maybe
                                                  (Array
                                                   r))
reductions input = selectionForCompositeField
                   "reductions"
                   (toGraphQLArguments
                    input)
                   graphqlDefaultResponseFunctorOrScalarDecoderTransformer

hits :: forall r . SelectionSet
                   Scope__Hit
                   r -> SelectionSet
                        Scope__RootQuery
                        (Array
                         r)
hits = selectionForCompositeField
       "hits"
       []
       graphqlDefaultResponseFunctorOrScalarDecoderTransformer
