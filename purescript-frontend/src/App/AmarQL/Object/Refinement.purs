module App.AmarQL.Object.Refinement where

import GraphQLClient
  ( SelectionSet
  , selectionForField
  , graphqlDefaultResponseScalarDecoder
  , selectionForCompositeField
  , graphqlDefaultResponseFunctorOrScalarDecoderTransformer
  )
import App.AmarQL.Scopes (Scope__Refinement, Scope__DataReduction)
import App.AmarQL.Scalars (Id, DateTime)
import Data.Maybe (Maybe)
import App.AmarQL.Enum.RefinementMethod (RefinementMethod)

refinementId :: SelectionSet Scope__Refinement Id
refinementId = selectionForField
               "refinementId"
               []
               graphqlDefaultResponseScalarDecoder

dataReductionId :: SelectionSet Scope__Refinement Int
dataReductionId = selectionForField
                  "dataReductionId"
                  []
                  graphqlDefaultResponseScalarDecoder

analysisTime :: SelectionSet Scope__Refinement DateTime
analysisTime = selectionForField
               "analysisTime"
               []
               graphqlDefaultResponseScalarDecoder

folderPath :: SelectionSet Scope__Refinement (Maybe String)
folderPath = selectionForField
             "folderPath"
             []
             graphqlDefaultResponseScalarDecoder

initialPdbPath :: SelectionSet Scope__Refinement (Maybe String)
initialPdbPath = selectionForField
                 "initialPdbPath"
                 []
                 graphqlDefaultResponseScalarDecoder

finalPdbPath :: SelectionSet Scope__Refinement (Maybe String)
finalPdbPath = selectionForField
               "finalPdbPath"
               []
               graphqlDefaultResponseScalarDecoder

refinementMtzPath :: SelectionSet Scope__Refinement (Maybe String)
refinementMtzPath = selectionForField
                    "refinementMtzPath"
                    []
                    graphqlDefaultResponseScalarDecoder

method :: SelectionSet Scope__Refinement RefinementMethod
method = selectionForField "method" [] graphqlDefaultResponseScalarDecoder

comment :: SelectionSet Scope__Refinement (Maybe String)
comment = selectionForField "comment" [] graphqlDefaultResponseScalarDecoder

resolutionCut :: SelectionSet Scope__Refinement (Maybe Number)
resolutionCut = selectionForField
                "resolutionCut"
                []
                graphqlDefaultResponseScalarDecoder

rfree :: SelectionSet Scope__Refinement (Maybe Number)
rfree = selectionForField "rfree" [] graphqlDefaultResponseScalarDecoder

rwork :: SelectionSet Scope__Refinement (Maybe Number)
rwork = selectionForField "rwork" [] graphqlDefaultResponseScalarDecoder

rmsBondLength :: SelectionSet Scope__Refinement (Maybe Number)
rmsBondLength = selectionForField
                "rmsBondLength"
                []
                graphqlDefaultResponseScalarDecoder

rmsBondAngle :: SelectionSet Scope__Refinement (Maybe Number)
rmsBondAngle = selectionForField
               "rmsBondAngle"
               []
               graphqlDefaultResponseScalarDecoder

numBlobs :: SelectionSet Scope__Refinement (Maybe Int)
numBlobs = selectionForField "numBlobs" [] graphqlDefaultResponseScalarDecoder

averageModelB :: SelectionSet Scope__Refinement (Maybe Number)
averageModelB = selectionForField
                "averageModelB"
                []
                graphqlDefaultResponseScalarDecoder

dataReduction :: forall r . SelectionSet
                            Scope__DataReduction
                            r -> SelectionSet
                                 Scope__Refinement
                                 (Maybe
                                  r)
dataReduction = selectionForCompositeField
                "dataReduction"
                []
                graphqlDefaultResponseFunctorOrScalarDecoderTransformer
