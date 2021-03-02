module App.AmarQL.Object.DataReduction where

import GraphQLClient
  ( SelectionSet
  , selectionForField
  , graphqlDefaultResponseScalarDecoder
  , selectionForCompositeField
  , graphqlDefaultResponseFunctorOrScalarDecoderTransformer
  )
import App.AmarQL.Scopes (Scope__DataReduction, Scope__Refinement)
import Data.Maybe (Maybe)
import App.AmarQL.Scalars (Id, DateTime)
import App.AmarQL.Enum.ReductionMethod (ReductionMethod)

wilsonB :: SelectionSet Scope__DataReduction (Maybe Number)
wilsonB = selectionForField "wilsonB" [] graphqlDefaultResponseScalarDecoder

dataReductionId :: SelectionSet Scope__DataReduction Id
dataReductionId = selectionForField
                  "dataReductionId"
                  []
                  graphqlDefaultResponseScalarDecoder

crystalId :: SelectionSet Scope__DataReduction String
crystalId = selectionForField "crystalId" [] graphqlDefaultResponseScalarDecoder

runId :: SelectionSet Scope__DataReduction Int
runId = selectionForField "runId" [] graphqlDefaultResponseScalarDecoder

analysisTime :: SelectionSet Scope__DataReduction DateTime
analysisTime = selectionForField
               "analysisTime"
               []
               graphqlDefaultResponseScalarDecoder

folderPath :: SelectionSet Scope__DataReduction String
folderPath = selectionForField
             "folderPath"
             []
             graphqlDefaultResponseScalarDecoder

mtzPath :: SelectionSet Scope__DataReduction (Maybe String)
mtzPath = selectionForField "mtzPath" [] graphqlDefaultResponseScalarDecoder

comment :: SelectionSet Scope__DataReduction (Maybe String)
comment = selectionForField "comment" [] graphqlDefaultResponseScalarDecoder

method :: SelectionSet Scope__DataReduction ReductionMethod
method = selectionForField "method" [] graphqlDefaultResponseScalarDecoder

resolutionCc :: SelectionSet Scope__DataReduction (Maybe Number)
resolutionCc = selectionForField
               "resolutionCc"
               []
               graphqlDefaultResponseScalarDecoder

resolutionIsigma :: SelectionSet Scope__DataReduction (Maybe Number)
resolutionIsigma = selectionForField
                   "resolutionIsigma"
                   []
                   graphqlDefaultResponseScalarDecoder

a :: SelectionSet Scope__DataReduction (Maybe Number)
a = selectionForField "a" [] graphqlDefaultResponseScalarDecoder

b :: SelectionSet Scope__DataReduction (Maybe Number)
b = selectionForField "b" [] graphqlDefaultResponseScalarDecoder

c :: SelectionSet Scope__DataReduction (Maybe Number)
c = selectionForField "c" [] graphqlDefaultResponseScalarDecoder

alpha :: SelectionSet Scope__DataReduction (Maybe Number)
alpha = selectionForField "alpha" [] graphqlDefaultResponseScalarDecoder

beta :: SelectionSet Scope__DataReduction (Maybe Number)
beta = selectionForField "beta" [] graphqlDefaultResponseScalarDecoder

gamma :: SelectionSet Scope__DataReduction (Maybe Number)
gamma = selectionForField "gamma" [] graphqlDefaultResponseScalarDecoder

spaceGroup :: SelectionSet Scope__DataReduction (Maybe Int)
spaceGroup = selectionForField
             "spaceGroup"
             []
             graphqlDefaultResponseScalarDecoder

isigi :: SelectionSet Scope__DataReduction (Maybe Number)
isigi = selectionForField "isigi" [] graphqlDefaultResponseScalarDecoder

rmeas :: SelectionSet Scope__DataReduction (Maybe Number)
rmeas = selectionForField "rmeas" [] graphqlDefaultResponseScalarDecoder

cchalf :: SelectionSet Scope__DataReduction (Maybe Number)
cchalf = selectionForField "cchalf" [] graphqlDefaultResponseScalarDecoder

rfactor :: SelectionSet Scope__DataReduction (Maybe Number)
rfactor = selectionForField "rfactor" [] graphqlDefaultResponseScalarDecoder

refinements :: forall r . SelectionSet
                          Scope__Refinement
                          r -> SelectionSet
                               Scope__DataReduction
                               (Maybe
                                (Array
                                 (Maybe
                                  r)))
refinements = selectionForCompositeField
              "refinements"
              []
              graphqlDefaultResponseFunctorOrScalarDecoderTransformer
