module App.AmarQL.Mutation where

import App.AmarQL.InputObject (HitInput) as App.AmarQL.InputObject
import Type.Row (type (+))
import GraphQLClient
  ( SelectionSet
  , Scope__RootMutation
  , selectionForCompositeField
  , toGraphQLArguments
  , graphqlDefaultResponseFunctorOrScalarDecoderTransformer
  )
import App.AmarQL.Scopes (Scope__UpsertHit, Scope__RemoveHit)
import Data.Maybe (Maybe)

type UpsertHitInputRowRequired r = ( "data" :: App.AmarQL.InputObject.HitInput
                                   | r
                                   )

type UpsertHitInput = { | UpsertHitInputRowRequired + () }

upsertHit :: forall r . UpsertHitInput -> SelectionSet
                                          Scope__UpsertHit
                                          r -> SelectionSet
                                               Scope__RootMutation
                                               (Maybe
                                                r)
upsertHit input = selectionForCompositeField
                  "upsertHit"
                  (toGraphQLArguments
                   input)
                  graphqlDefaultResponseFunctorOrScalarDecoderTransformer

type RemoveHitInputRowRequired r = ( entityId :: Int | r )

type RemoveHitInput = { | RemoveHitInputRowRequired + () }

removeHit :: forall r . RemoveHitInput -> SelectionSet
                                          Scope__RemoveHit
                                          r -> SelectionSet
                                               Scope__RootMutation
                                               (Maybe
                                                r)
removeHit input = selectionForCompositeField
                  "removeHit"
                  (toGraphQLArguments
                   input)
                  graphqlDefaultResponseFunctorOrScalarDecoderTransformer
