module App.AmarQL.Object.RemoveHit where

import GraphQLClient
  (SelectionSet, selectionForField, graphqlDefaultResponseScalarDecoder)
import App.AmarQL.Scopes (Scope__RemoveHit)
import Data.Maybe (Maybe)

errorMessage :: SelectionSet Scope__RemoveHit (Maybe String)
errorMessage = selectionForField
               "errorMessage"
               []
               graphqlDefaultResponseScalarDecoder
