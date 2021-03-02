module App.AmarQL.Object.UpsertHit where

import GraphQLClient
  (SelectionSet, selectionForField, graphqlDefaultResponseScalarDecoder)
import App.AmarQL.Scopes (Scope__UpsertHit)
import Data.Maybe (Maybe)

errorMessage :: SelectionSet Scope__UpsertHit (Maybe String)
errorMessage = selectionForField
               "errorMessage"
               []
               graphqlDefaultResponseScalarDecoder
