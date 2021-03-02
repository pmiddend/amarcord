module App.AmarQL.Object.Compound where

import GraphQLClient
  ( SelectionSet
  , selectionForField
  , graphqlDefaultResponseScalarDecoder
  , selectionForCompositeField
  , graphqlDefaultResponseFunctorOrScalarDecoderTransformer
  )
import App.AmarQL.Scopes (Scope__Compound, Scope__CompoundDrop)
import Data.Maybe (Maybe)
import App.AmarQL.Scalars (DateTime)

compoundId :: SelectionSet Scope__Compound String
compoundId = selectionForField
             "compoundId"
             []
             graphqlDefaultResponseScalarDecoder

created :: SelectionSet Scope__Compound (Maybe DateTime)
created = selectionForField "created" [] graphqlDefaultResponseScalarDecoder

crystalTrayId :: SelectionSet Scope__Compound String
crystalTrayId = selectionForField
                "crystalTrayId"
                []
                graphqlDefaultResponseScalarDecoder

wellId :: SelectionSet Scope__Compound String
wellId = selectionForField "wellId" [] graphqlDefaultResponseScalarDecoder

name :: SelectionSet Scope__Compound (Maybe String)
name = selectionForField "name" [] graphqlDefaultResponseScalarDecoder

smiles :: SelectionSet Scope__Compound (Maybe String)
smiles = selectionForField "smiles" [] graphqlDefaultResponseScalarDecoder

iupacName :: SelectionSet Scope__Compound (Maybe String)
iupacName = selectionForField "iupacName" [] graphqlDefaultResponseScalarDecoder

target :: SelectionSet Scope__Compound (Maybe String)
target = selectionForField "target" [] graphqlDefaultResponseScalarDecoder

phase :: SelectionSet Scope__Compound (Maybe String)
phase = selectionForField "phase" [] graphqlDefaultResponseScalarDecoder

intraSurface :: SelectionSet Scope__Compound (Maybe String)
intraSurface = selectionForField
               "intraSurface"
               []
               graphqlDefaultResponseScalarDecoder

subCategory :: SelectionSet Scope__Compound (Maybe String)
subCategory = selectionForField
              "subCategory"
              []
              graphqlDefaultResponseScalarDecoder

pubchemCid :: SelectionSet Scope__Compound (Maybe Int)
pubchemCid = selectionForField
             "pubchemCid"
             []
             graphqlDefaultResponseScalarDecoder

sourcePlateBarcode :: SelectionSet Scope__Compound (Maybe String)
sourcePlateBarcode = selectionForField
                     "sourcePlateBarcode"
                     []
                     graphqlDefaultResponseScalarDecoder

sourcePlateWell :: SelectionSet Scope__Compound (Maybe String)
sourcePlateWell = selectionForField
                  "sourcePlateWell"
                  []
                  graphqlDefaultResponseScalarDecoder

molWeight :: SelectionSet Scope__Compound (Maybe Number)
molWeight = selectionForField "molWeight" [] graphqlDefaultResponseScalarDecoder

saltData :: SelectionSet Scope__Compound (Maybe String)
saltData = selectionForField "saltData" [] graphqlDefaultResponseScalarDecoder

molFormula :: SelectionSet Scope__Compound (Maybe String)
molFormula = selectionForField
             "molFormula"
             []
             graphqlDefaultResponseScalarDecoder

stereochemistry :: SelectionSet Scope__Compound (Maybe String)
stereochemistry = selectionForField
                  "stereochemistry"
                  []
                  graphqlDefaultResponseScalarDecoder

compoundDrop :: forall r . SelectionSet
                           Scope__CompoundDrop
                           r -> SelectionSet
                                Scope__Compound
                                (Maybe
                                 (Array
                                  (Maybe
                                   r)))
compoundDrop = selectionForCompositeField
               "compoundDrop"
               []
               graphqlDefaultResponseFunctorOrScalarDecoderTransformer
