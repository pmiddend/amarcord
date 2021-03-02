module App.AmarQL.Object.Diffraction where

import GraphQLClient
  ( SelectionSet
  , selectionForField
  , graphqlDefaultResponseScalarDecoder
  , selectionForCompositeField
  , graphqlDefaultResponseFunctorOrScalarDecoderTransformer
  )
import App.AmarQL.Scopes (Scope__Diffraction, Scope__Crystal)
import Data.Maybe (Maybe)
import App.AmarQL.Scalars (Id, DateTime)
import App.AmarQL.Enum.Beamline (Beamline)
import App.AmarQL.Enum.DiffractionType (DiffractionType)

metadataColumn :: SelectionSet Scope__Diffraction (Maybe String)
metadataColumn = selectionForField
                 "metadataColumn"
                 []
                 graphqlDefaultResponseScalarDecoder

crystalId :: SelectionSet Scope__Diffraction String
crystalId = selectionForField "crystalId" [] graphqlDefaultResponseScalarDecoder

runId :: SelectionSet Scope__Diffraction Id
runId = selectionForField "runId" [] graphqlDefaultResponseScalarDecoder

created :: SelectionSet Scope__Diffraction (Maybe DateTime)
created = selectionForField "created" [] graphqlDefaultResponseScalarDecoder

dewarPosition :: SelectionSet Scope__Diffraction (Maybe Int)
dewarPosition = selectionForField
                "dewarPosition"
                []
                graphqlDefaultResponseScalarDecoder

beamline :: SelectionSet Scope__Diffraction (Maybe Beamline)
beamline = selectionForField "beamline" [] graphqlDefaultResponseScalarDecoder

beamIntensity :: SelectionSet Scope__Diffraction (Maybe String)
beamIntensity = selectionForField
                "beamIntensity"
                []
                graphqlDefaultResponseScalarDecoder

pinhole :: SelectionSet Scope__Diffraction (Maybe String)
pinhole = selectionForField "pinhole" [] graphqlDefaultResponseScalarDecoder

focusing :: SelectionSet Scope__Diffraction (Maybe String)
focusing = selectionForField "focusing" [] graphqlDefaultResponseScalarDecoder

diffraction :: SelectionSet Scope__Diffraction DiffractionType
diffraction = selectionForField
              "diffraction"
              []
              graphqlDefaultResponseScalarDecoder

comment :: SelectionSet Scope__Diffraction (Maybe String)
comment = selectionForField "comment" [] graphqlDefaultResponseScalarDecoder

angleStart :: SelectionSet Scope__Diffraction (Maybe Number)
angleStart = selectionForField
             "angleStart"
             []
             graphqlDefaultResponseScalarDecoder

numberOfFrames :: SelectionSet Scope__Diffraction (Maybe Int)
numberOfFrames = selectionForField
                 "numberOfFrames"
                 []
                 graphqlDefaultResponseScalarDecoder

angleStep :: SelectionSet Scope__Diffraction (Maybe Number)
angleStep = selectionForField "angleStep" [] graphqlDefaultResponseScalarDecoder

exposureTime :: SelectionSet Scope__Diffraction (Maybe Number)
exposureTime = selectionForField
               "exposureTime"
               []
               graphqlDefaultResponseScalarDecoder

xrayEnergy :: SelectionSet Scope__Diffraction (Maybe Number)
xrayEnergy = selectionForField
             "xrayEnergy"
             []
             graphqlDefaultResponseScalarDecoder

xrayWavelength :: SelectionSet Scope__Diffraction (Maybe Number)
xrayWavelength = selectionForField
                 "xrayWavelength"
                 []
                 graphqlDefaultResponseScalarDecoder

detectorName :: SelectionSet Scope__Diffraction (Maybe String)
detectorName = selectionForField
               "detectorName"
               []
               graphqlDefaultResponseScalarDecoder

detectorDistance :: SelectionSet Scope__Diffraction (Maybe Number)
detectorDistance = selectionForField
                   "detectorDistance"
                   []
                   graphqlDefaultResponseScalarDecoder

detectorEdgeResolution :: SelectionSet Scope__Diffraction (Maybe Number)
detectorEdgeResolution = selectionForField
                         "detectorEdgeResolution"
                         []
                         graphqlDefaultResponseScalarDecoder

apertureRadius :: SelectionSet Scope__Diffraction (Maybe Number)
apertureRadius = selectionForField
                 "apertureRadius"
                 []
                 graphqlDefaultResponseScalarDecoder

filterTransmission :: SelectionSet Scope__Diffraction (Maybe Number)
filterTransmission = selectionForField
                     "filterTransmission"
                     []
                     graphqlDefaultResponseScalarDecoder

ringCurrent :: SelectionSet Scope__Diffraction (Maybe Number)
ringCurrent = selectionForField
              "ringCurrent"
              []
              graphqlDefaultResponseScalarDecoder

dataRawFilenamePattern :: SelectionSet Scope__Diffraction (Maybe String)
dataRawFilenamePattern = selectionForField
                         "dataRawFilenamePattern"
                         []
                         graphqlDefaultResponseScalarDecoder

microscopeImageFilenamePattern :: SelectionSet Scope__Diffraction (Maybe String)
microscopeImageFilenamePattern = selectionForField
                                 "microscopeImageFilenamePattern"
                                 []
                                 graphqlDefaultResponseScalarDecoder

apertureHorizontal :: SelectionSet Scope__Diffraction (Maybe Number)
apertureHorizontal = selectionForField
                     "apertureHorizontal"
                     []
                     graphqlDefaultResponseScalarDecoder

apertureVertical :: SelectionSet Scope__Diffraction (Maybe Number)
apertureVertical = selectionForField
                   "apertureVertical"
                   []
                   graphqlDefaultResponseScalarDecoder

crystal :: forall r . SelectionSet
                      Scope__Crystal
                      r -> SelectionSet
                           Scope__Diffraction
                           (Maybe
                            r)
crystal = selectionForCompositeField
          "crystal"
          []
          graphqlDefaultResponseFunctorOrScalarDecoderTransformer
