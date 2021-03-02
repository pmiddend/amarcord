module App.AmarQL.Object.Hit where

import GraphQLClient
  (SelectionSet, selectionForField, graphqlDefaultResponseScalarDecoder)
import App.AmarQL.Scopes (Scope__Hit)
import Data.Maybe (Maybe)
import App.AmarQL.Scalars (Id)
import App.AmarQL.Enum.HitMethod (HitMethod)
import App.AmarQL.Enum.BindingSite (BindingSite)
import App.AmarQL.Enum.Confidence (Confidence)
import App.AmarQL.Enum.Interesting (Interesting)

metadataColumn :: SelectionSet Scope__Hit (Maybe String)
metadataColumn = selectionForField
                 "metadataColumn"
                 []
                 graphqlDefaultResponseScalarDecoder

hitId :: SelectionSet Scope__Hit Id
hitId = selectionForField "hitId" [] graphqlDefaultResponseScalarDecoder

refinementId :: SelectionSet Scope__Hit Int
refinementId = selectionForField
               "refinementId"
               []
               graphqlDefaultResponseScalarDecoder

method :: SelectionSet Scope__Hit HitMethod
method = selectionForField "method" [] graphqlDefaultResponseScalarDecoder

comment :: SelectionSet Scope__Hit (Maybe String)
comment = selectionForField "comment" [] graphqlDefaultResponseScalarDecoder

bindingSite :: SelectionSet Scope__Hit (Maybe BindingSite)
bindingSite = selectionForField
              "bindingSite"
              []
              graphqlDefaultResponseScalarDecoder

closestResidue :: SelectionSet Scope__Hit (Maybe Int)
closestResidue = selectionForField
                 "closestResidue"
                 []
                 graphqlDefaultResponseScalarDecoder

confidence :: SelectionSet Scope__Hit (Maybe Confidence)
confidence = selectionForField
             "confidence"
             []
             graphqlDefaultResponseScalarDecoder

isInteresting :: SelectionSet Scope__Hit (Maybe Interesting)
isInteresting = selectionForField
                "isInteresting"
                []
                graphqlDefaultResponseScalarDecoder

inspectedBy :: SelectionSet Scope__Hit (Maybe String)
inspectedBy = selectionForField
              "inspectedBy"
              []
              graphqlDefaultResponseScalarDecoder
