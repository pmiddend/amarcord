import { GraphQLClient } from 'graphql-request';
import { print } from 'graphql';
import gql from 'graphql-tag';
export type Maybe<T> = T | null;
export type Exact<T extends { [key: string]: unknown }> = { [K in keyof T]: T[K] };
/** All built-in and custom scalars, mapped to their actual values */
export type Scalars = {
  ID: string;
  String: string;
  Boolean: boolean;
  Int: number;
  Float: number;
  /**
   * The `DateTime` scalar type represents a DateTime
   * value as specified by
   * [iso8601](https://en.wikipedia.org/wiki/ISO_8601).
   */
  DateTime: any;
};

export type Query = {
  __typename?: 'Query';
  crystals?: Maybe<Array<Crystal>>;
  targets?: Maybe<Array<Target>>;
  plasmids?: Maybe<Array<Plasmid>>;
  reductions?: Maybe<Array<DataReduction>>;
  pucks?: Maybe<Array<Puck>>;
  metadatas: Array<MetadataOutput>;
  finalModels: Array<FinalModelOutput>;
  hits: Array<Hit>;
};


export type QueryCrystalsArgs = {
  crystalId?: Maybe<Scalars['String']>;
};


export type QueryReductionsArgs = {
  crystalId: Scalars['String'];
  runIds: Array<Scalars['String']>;
};

export type Crystal = {
  __typename?: 'Crystal';
  crystalId: Scalars['String'];
  created?: Maybe<Scalars['DateTime']>;
  targetId?: Maybe<Scalars['String']>;
  compoundDropId?: Maybe<Scalars['Int']>;
  puckId?: Maybe<Scalars['String']>;
  puckPositionId?: Maybe<Scalars['Int']>;
  creatorId: Scalars['String'];
  target?: Maybe<Target>;
  compoundDrop?: Maybe<CompoundDrop>;
  diffractions?: Maybe<Array<Maybe<Diffraction>>>;
};


export type Target = {
  __typename?: 'Target';
  targetId: Scalars['String'];
  created?: Maybe<Scalars['DateTime']>;
  name: Scalars['String'];
  crystals?: Maybe<Array<Maybe<Crystal>>>;
  plasmids?: Maybe<Array<Maybe<Plasmid>>>;
};

export type Plasmid = {
  __typename?: 'Plasmid';
  plasmidId: Scalars['String'];
  created?: Maybe<Scalars['DateTime']>;
  name: Scalars['String'];
  expression?: Maybe<Scalars['String']>;
  purification?: Maybe<Scalars['String']>;
  crystallization?: Maybe<Scalars['String']>;
  expressionSystem?: Maybe<Scalars['String']>;
  expressionLab?: Maybe<Scalars['String']>;
  expressionProtocol?: Maybe<Scalars['String']>;
  targets?: Maybe<Array<Maybe<Target>>>;
};

export type CompoundDrop = {
  __typename?: 'CompoundDrop';
  compoundDropId: Scalars['ID'];
  created?: Maybe<Scalars['DateTime']>;
  compoundId: Scalars['String'];
  wellId?: Maybe<Scalars['String']>;
  crystalTrayId?: Maybe<Scalars['String']>;
  dropId?: Maybe<Scalars['Int']>;
  crystal?: Maybe<Array<Maybe<Crystal>>>;
  compound?: Maybe<Compound>;
};

export type Compound = {
  __typename?: 'Compound';
  compoundId: Scalars['String'];
  created?: Maybe<Scalars['DateTime']>;
  crystalTrayId: Scalars['String'];
  wellId: Scalars['String'];
  name?: Maybe<Scalars['String']>;
  smiles?: Maybe<Scalars['String']>;
  iupacName?: Maybe<Scalars['String']>;
  target?: Maybe<Scalars['String']>;
  phase?: Maybe<Scalars['String']>;
  intraSurface?: Maybe<Scalars['String']>;
  subCategory?: Maybe<Scalars['String']>;
  pubchemCid?: Maybe<Scalars['Int']>;
  sourcePlateBarcode?: Maybe<Scalars['String']>;
  sourcePlateWell?: Maybe<Scalars['String']>;
  molWeight?: Maybe<Scalars['Float']>;
  saltData?: Maybe<Scalars['String']>;
  molFormula?: Maybe<Scalars['String']>;
  stereochemistry?: Maybe<Scalars['String']>;
  compoundDrop?: Maybe<Array<Maybe<CompoundDrop>>>;
};

export type Diffraction = {
  __typename?: 'Diffraction';
  metadataColumn?: Maybe<Scalars['String']>;
  crystalId: Scalars['String'];
  runId: Scalars['ID'];
  created?: Maybe<Scalars['DateTime']>;
  dewarPosition?: Maybe<Scalars['Int']>;
  beamline?: Maybe<Beamline>;
  beamIntensity?: Maybe<Scalars['String']>;
  pinhole?: Maybe<Scalars['String']>;
  focusing?: Maybe<Scalars['String']>;
  diffraction: DiffractionType;
  comment?: Maybe<Scalars['String']>;
  angleStart?: Maybe<Scalars['Float']>;
  numberOfFrames?: Maybe<Scalars['Int']>;
  angleStep?: Maybe<Scalars['Float']>;
  exposureTime?: Maybe<Scalars['Float']>;
  xrayEnergy?: Maybe<Scalars['Float']>;
  xrayWavelength?: Maybe<Scalars['Float']>;
  detectorName?: Maybe<Scalars['String']>;
  detectorDistance?: Maybe<Scalars['Float']>;
  detectorEdgeResolution?: Maybe<Scalars['Float']>;
  apertureRadius?: Maybe<Scalars['Float']>;
  filterTransmission?: Maybe<Scalars['Float']>;
  ringCurrent?: Maybe<Scalars['Float']>;
  dataRawFilenamePattern?: Maybe<Scalars['String']>;
  microscopeImageFilenamePattern?: Maybe<Scalars['String']>;
  apertureHorizontal?: Maybe<Scalars['Float']>;
  apertureVertical?: Maybe<Scalars['Float']>;
  crystal?: Maybe<Crystal>;
};

/** An enumeration. */
export enum Beamline {
  P11 = 'P11',
  P13 = 'P13',
  P14 = 'P14'
}

/** An enumeration. */
export enum DiffractionType {
  NoDiffraction = 'NO_DIFFRACTION',
  NoCrystal = 'NO_CRYSTAL',
  IceSalt = 'ICE_SALT',
  Success = 'SUCCESS'
}

export type DataReduction = {
  __typename?: 'DataReduction';
  dataReductionId: Scalars['ID'];
  crystalId: Scalars['String'];
  runId: Scalars['Int'];
  analysisTime: Scalars['DateTime'];
  folderPath: Scalars['String'];
  mtzPath?: Maybe<Scalars['String']>;
  comment?: Maybe<Scalars['String']>;
  method: ReductionMethod;
  resolutionCc?: Maybe<Scalars['Float']>;
  resolutionIsigma?: Maybe<Scalars['Float']>;
  a?: Maybe<Scalars['Float']>;
  b?: Maybe<Scalars['Float']>;
  c?: Maybe<Scalars['Float']>;
  alpha?: Maybe<Scalars['Float']>;
  beta?: Maybe<Scalars['Float']>;
  gamma?: Maybe<Scalars['Float']>;
  spaceGroup?: Maybe<Scalars['Int']>;
  isigi?: Maybe<Scalars['Float']>;
  rmeas?: Maybe<Scalars['Float']>;
  cchalf?: Maybe<Scalars['Float']>;
  rfactor?: Maybe<Scalars['Float']>;
  WilsonB?: Maybe<Scalars['Float']>;
  refinements?: Maybe<Array<Maybe<Refinement>>>;
};

/** An enumeration. */
export enum ReductionMethod {
  XdsPre = 'XDS_PRE',
  XdsFull = 'XDS_FULL',
  XdsReindex1 = 'XDS_REINDEX1',
  XdsReinder1Noice = 'XDS_REINDER1_NOICE',
  DialsDials = 'DIALS_DIALS',
  Dials_1P7ADials = 'DIALS_1_P7_A_DIALS',
  Staraniso = 'STARANISO',
  Other = 'OTHER'
}

export type Refinement = {
  __typename?: 'Refinement';
  refinementId: Scalars['ID'];
  dataReductionId: Scalars['Int'];
  analysisTime: Scalars['DateTime'];
  folderPath?: Maybe<Scalars['String']>;
  initialPdbPath?: Maybe<Scalars['String']>;
  finalPdbPath?: Maybe<Scalars['String']>;
  refinementMtzPath?: Maybe<Scalars['String']>;
  method: RefinementMethod;
  comment?: Maybe<Scalars['String']>;
  resolutionCut?: Maybe<Scalars['Float']>;
  rfree?: Maybe<Scalars['Float']>;
  rwork?: Maybe<Scalars['Float']>;
  rmsBondLength?: Maybe<Scalars['Float']>;
  rmsBondAngle?: Maybe<Scalars['Float']>;
  numBlobs?: Maybe<Scalars['Int']>;
  averageModelB?: Maybe<Scalars['Float']>;
  dataReduction?: Maybe<DataReduction>;
};

/** An enumeration. */
export enum RefinementMethod {
  Hzb = 'HZB',
  Dmpl = 'DMPL',
  Dmpl2 = 'DMPL2',
  Dmpl2Aligned = 'DMPL2_ALIGNED',
  Dmpl2Qfit = 'DMPL2_QFIT'
}

export type Puck = {
  __typename?: 'Puck';
  puckId: Scalars['String'];
  created?: Maybe<Scalars['DateTime']>;
  puckType?: Maybe<PuckType>;
  owner?: Maybe<Scalars['String']>;
};

/** An enumeration. */
export enum PuckType {
  Uni = 'UNI',
  Spine = 'SPINE'
}

export type MetadataOutput = {
  __typename?: 'MetadataOutput';
  metadataId: Scalars['String'];
  created: Scalars['DateTime'];
  title: Scalars['String'];
};

export type FinalModelOutput = {
  __typename?: 'FinalModelOutput';
  metadataId: Scalars['String'];
  metadataTitle: Scalars['String'];
  refinementId: Scalars['String'];
};

export type Hit = {
  __typename?: 'Hit';
  metadataColumn?: Maybe<Scalars['String']>;
  hitId: Scalars['ID'];
  refinementId: Scalars['Int'];
  method: HitMethod;
  comment?: Maybe<Scalars['String']>;
  bindingSite?: Maybe<BindingSite>;
  closestResidue?: Maybe<Scalars['Int']>;
  confidence?: Maybe<Confidence>;
  isInteresting?: Maybe<Interesting>;
  inspectedBy?: Maybe<Scalars['String']>;
  refinement?: Maybe<Refinement>;
};

/** An enumeration. */
export enum HitMethod {
  Pandda = 'PANDDA',
  DimpleBlob = 'DIMPLE_BLOB',
  Manual = 'MANUAL'
}

/** An enumeration. */
export enum BindingSite {
  ActivePocket = 'ACTIVE_POCKET',
  OtherPocket = 'OTHER_POCKET',
  CrystalInterface = 'CRYSTAL_INTERFACE',
  Surface = 'SURFACE'
}

/** An enumeration. */
export enum Confidence {
  Low = 'LOW',
  Medium = 'MEDIUM',
  High = 'HIGH'
}

/** An enumeration. */
export enum Interesting {
  Yes = 'YES',
  No = 'NO'
}

export type Mutations = {
  __typename?: 'Mutations';
  addTarget?: Maybe<AddTarget>;
  modifyTarget?: Maybe<ModifyTarget>;
  addPlasmid?: Maybe<AddPlasmid>;
  modifyPlasmid?: Maybe<ModifyPlasmid>;
  addPuck?: Maybe<AddPuck>;
  modifyPuck?: Maybe<ModifyPuck>;
  addMetadata?: Maybe<AddMetadata>;
  removeMetadata?: Maybe<RemoveMetadata>;
  modifyMetadata?: Maybe<ModifyMetadata>;
  upsertHit?: Maybe<UpsertHit>;
  removeHit?: Maybe<RemoveHit>;
};


export type MutationsAddTargetArgs = {
  id?: Maybe<Scalars['String']>;
  name?: Maybe<Scalars['String']>;
};


export type MutationsModifyTargetArgs = {
  id?: Maybe<Scalars['String']>;
  name?: Maybe<Scalars['String']>;
};


export type MutationsAddPlasmidArgs = {
  plasmidData: PlasmidInput;
};


export type MutationsModifyPlasmidArgs = {
  plasmidData: PlasmidInput;
};


export type MutationsAddPuckArgs = {
  puckData: PuckInput;
};


export type MutationsModifyPuckArgs = {
  puckData: PuckInput;
};


export type MutationsAddMetadataArgs = {
  metadataData: MetadataInput;
};


export type MutationsRemoveMetadataArgs = {
  metadataId: Scalars['Int'];
};


export type MutationsModifyMetadataArgs = {
  metadataData: MetadataInput;
};


export type MutationsUpsertHitArgs = {
  data: HitInput;
};


export type MutationsRemoveHitArgs = {
  entityId: Scalars['Int'];
};

export type AddTarget = {
  __typename?: 'AddTarget';
  errorMessage?: Maybe<Scalars['String']>;
};

export type ModifyTarget = {
  __typename?: 'ModifyTarget';
  errorMessage?: Maybe<Scalars['String']>;
};

export type AddPlasmid = {
  __typename?: 'AddPlasmid';
  errorMessage?: Maybe<Scalars['String']>;
};

export type PlasmidInput = {
  plasmidId: Scalars['String'];
  name: Scalars['String'];
  targetId?: Maybe<Scalars['String']>;
  expression?: Maybe<Scalars['String']>;
  expressionSystem?: Maybe<Scalars['String']>;
  expressionLab?: Maybe<Scalars['String']>;
  expressionProtocol?: Maybe<Scalars['String']>;
  purification?: Maybe<Scalars['String']>;
  crystallization?: Maybe<Scalars['String']>;
};

export type ModifyPlasmid = {
  __typename?: 'ModifyPlasmid';
  errorMessage?: Maybe<Scalars['String']>;
};

export type AddPuck = {
  __typename?: 'AddPuck';
  errorMessage?: Maybe<Scalars['String']>;
};

export type PuckInput = {
  puckId: Scalars['String'];
  puckType: Scalars['String'];
  owner?: Maybe<Scalars['String']>;
};

export type ModifyPuck = {
  __typename?: 'ModifyPuck';
  errorMessage?: Maybe<Scalars['String']>;
};

export type AddMetadata = {
  __typename?: 'AddMetadata';
  errorMessage?: Maybe<Scalars['String']>;
};

export type MetadataInput = {
  metadataId?: Maybe<Scalars['String']>;
  title: Scalars['String'];
};

export type RemoveMetadata = {
  __typename?: 'RemoveMetadata';
  errorMessage?: Maybe<Scalars['String']>;
};

export type ModifyMetadata = {
  __typename?: 'ModifyMetadata';
  errorMessage?: Maybe<Scalars['String']>;
};

export type UpsertHit = {
  __typename?: 'UpsertHit';
  errorMessage?: Maybe<Scalars['String']>;
};

export type HitInput = {
  hitId?: Maybe<Scalars['String']>;
  refinementId: Scalars['String'];
  method: Scalars['String'];
  comment: Scalars['String'];
  bindingSite: Scalars['String'];
  confidence: Scalars['String'];
  isInteresting: Scalars['Boolean'];
  inspectedBy: Scalars['String'];
  metadataColumn: Scalars['String'];
};

export type RemoveHit = {
  __typename?: 'RemoveHit';
  errorMessage?: Maybe<Scalars['String']>;
};

export type AddMetadataMutationVariables = Exact<{
  metadataData: MetadataInput;
}>;


export type AddMetadataMutation = (
  { __typename?: 'Mutations' }
  & { addMetadata?: Maybe<(
    { __typename?: 'AddMetadata' }
    & Pick<AddMetadata, 'errorMessage'>
  )> }
);

export type AddPlasmidMutationVariables = Exact<{
  plasmidData: PlasmidInput;
}>;


export type AddPlasmidMutation = (
  { __typename?: 'Mutations' }
  & { addPlasmid?: Maybe<(
    { __typename?: 'AddPlasmid' }
    & Pick<AddPlasmid, 'errorMessage'>
  )> }
);

export type AddPuckMutationVariables = Exact<{
  puckData: PuckInput;
}>;


export type AddPuckMutation = (
  { __typename?: 'Mutations' }
  & { addPuck?: Maybe<(
    { __typename?: 'AddPuck' }
    & Pick<AddPuck, 'errorMessage'>
  )> }
);

export type AddTargetMutationVariables = Exact<{
  id: Scalars['String'];
  name: Scalars['String'];
}>;


export type AddTargetMutation = (
  { __typename?: 'Mutations' }
  & { addTarget?: Maybe<(
    { __typename?: 'AddTarget' }
    & Pick<AddTarget, 'errorMessage'>
  )> }
);

export type CrystalIdsQueryVariables = Exact<{ [key: string]: never; }>;


export type CrystalIdsQuery = (
  { __typename?: 'Query' }
  & { crystals?: Maybe<Array<(
    { __typename?: 'Crystal' }
    & Pick<Crystal, 'crystalId'>
  )>> }
);

export type HitsQueryVariables = Exact<{ [key: string]: never; }>;


export type HitsQuery = (
  { __typename?: 'Query' }
  & { hits: Array<(
    { __typename?: 'Hit' }
    & Pick<Hit, 'hitId' | 'refinementId' | 'method' | 'comment' | 'bindingSite' | 'closestResidue' | 'confidence' | 'isInteresting' | 'inspectedBy' | 'metadataColumn'>
  )> }
);

export type UpsertHitMutationVariables = Exact<{
  hitData: HitInput;
}>;


export type UpsertHitMutation = (
  { __typename?: 'Mutations' }
  & { upsertHit?: Maybe<(
    { __typename?: 'UpsertHit' }
    & Pick<UpsertHit, 'errorMessage'>
  )> }
);

export type RemoveHitMutationVariables = Exact<{
  entityId: Scalars['Int'];
}>;


export type RemoveHitMutation = (
  { __typename?: 'Mutations' }
  & { removeHit?: Maybe<(
    { __typename?: 'RemoveHit' }
    & Pick<RemoveHit, 'errorMessage'>
  )> }
);

export type MetadatasQueryVariables = Exact<{ [key: string]: never; }>;


export type MetadatasQuery = (
  { __typename?: 'Query' }
  & { metadatas: Array<(
    { __typename?: 'MetadataOutput' }
    & Pick<MetadataOutput, 'metadataId' | 'created' | 'title'>
  )> }
);

export type ModifyMetadataMutationVariables = Exact<{
  metadataData: MetadataInput;
}>;


export type ModifyMetadataMutation = (
  { __typename?: 'Mutations' }
  & { modifyMetadata?: Maybe<(
    { __typename?: 'ModifyMetadata' }
    & Pick<ModifyMetadata, 'errorMessage'>
  )> }
);

export type ModifyPlasmidMutationVariables = Exact<{
  plasmidData: PlasmidInput;
}>;


export type ModifyPlasmidMutation = (
  { __typename?: 'Mutations' }
  & { modifyPlasmid?: Maybe<(
    { __typename?: 'ModifyPlasmid' }
    & Pick<ModifyPlasmid, 'errorMessage'>
  )> }
);

export type ModifyPuckMutationVariables = Exact<{
  puckData: PuckInput;
}>;


export type ModifyPuckMutation = (
  { __typename?: 'Mutations' }
  & { modifyPuck?: Maybe<(
    { __typename?: 'ModifyPuck' }
    & Pick<ModifyPuck, 'errorMessage'>
  )> }
);

export type ModifyTargetMutationVariables = Exact<{
  id: Scalars['String'];
  name: Scalars['String'];
}>;


export type ModifyTargetMutation = (
  { __typename?: 'Mutations' }
  & { modifyTarget?: Maybe<(
    { __typename?: 'ModifyTarget' }
    & Pick<ModifyTarget, 'errorMessage'>
  )> }
);

export type PlasmidsQueryVariables = Exact<{ [key: string]: never; }>;


export type PlasmidsQuery = (
  { __typename?: 'Query' }
  & { plasmids?: Maybe<Array<(
    { __typename?: 'Plasmid' }
    & Pick<Plasmid, 'plasmidId' | 'created' | 'name' | 'expression' | 'purification' | 'crystallization' | 'expressionSystem' | 'expressionLab' | 'expressionProtocol'>
    & { targets?: Maybe<Array<Maybe<(
      { __typename?: 'Target' }
      & Pick<Target, 'targetId' | 'name'>
    )>>> }
  )>> }
);

export type PucksQueryVariables = Exact<{ [key: string]: never; }>;


export type PucksQuery = (
  { __typename?: 'Query' }
  & { pucks?: Maybe<Array<(
    { __typename?: 'Puck' }
    & Pick<Puck, 'puckId' | 'puckType' | 'owner' | 'created'>
  )>> }
);

export type ReductionsQueryVariables = Exact<{
  crystalId: Scalars['String'];
  runIds: Array<Scalars['String']>;
}>;


export type ReductionsQuery = (
  { __typename?: 'Query' }
  & { reductions?: Maybe<Array<(
    { __typename?: 'DataReduction' }
    & Pick<DataReduction, 'dataReductionId' | 'runId' | 'method' | 'analysisTime' | 'folderPath' | 'mtzPath' | 'comment' | 'resolutionCc' | 'resolutionIsigma' | 'a' | 'b' | 'c' | 'alpha' | 'beta' | 'gamma' | 'spaceGroup' | 'isigi' | 'rmeas' | 'cchalf' | 'rfactor' | 'WilsonB'>
    & { refinements?: Maybe<Array<Maybe<(
      { __typename?: 'Refinement' }
      & Pick<Refinement, 'refinementId' | 'method' | 'analysisTime' | 'folderPath' | 'initialPdbPath' | 'finalPdbPath' | 'refinementMtzPath' | 'comment' | 'resolutionCut' | 'rfree' | 'rwork' | 'rmsBondLength' | 'rmsBondAngle' | 'numBlobs' | 'averageModelB'>
    )>>> }
  )>> }
);

export type SingleCrystalQueryVariables = Exact<{
  crystalId: Scalars['String'];
}>;


export type SingleCrystalQuery = (
  { __typename?: 'Query' }
  & { crystals?: Maybe<Array<(
    { __typename?: 'Crystal' }
    & Pick<Crystal, 'created' | 'creatorId' | 'puckPositionId' | 'puckId'>
    & { compoundDrop?: Maybe<(
      { __typename?: 'CompoundDrop' }
      & Pick<CompoundDrop, 'compoundId'>
      & { compound?: Maybe<(
        { __typename?: 'Compound' }
        & Pick<Compound, 'created' | 'crystalTrayId' | 'wellId' | 'name' | 'iupacName' | 'target' | 'phase' | 'intraSurface' | 'subCategory' | 'pubchemCid' | 'sourcePlateBarcode' | 'sourcePlateWell' | 'molWeight' | 'saltData' | 'molFormula' | 'stereochemistry' | 'smiles'>
      )> }
    )>, target?: Maybe<(
      { __typename?: 'Target' }
      & Pick<Target, 'targetId' | 'created' | 'name'>
      & { plasmids?: Maybe<Array<Maybe<(
        { __typename?: 'Plasmid' }
        & Pick<Plasmid, 'plasmidId' | 'created' | 'name' | 'expression' | 'purification' | 'crystallization' | 'expressionSystem' | 'expressionLab' | 'expressionProtocol'>
      )>>> }
    )>, diffractions?: Maybe<Array<Maybe<(
      { __typename?: 'Diffraction' }
      & Pick<Diffraction, 'runId' | 'created' | 'beamline' | 'beamIntensity' | 'pinhole' | 'focusing' | 'diffraction' | 'comment' | 'metadataColumn' | 'angleStart' | 'numberOfFrames' | 'angleStep' | 'exposureTime' | 'xrayEnergy' | 'xrayWavelength' | 'detectorName' | 'detectorDistance' | 'detectorEdgeResolution' | 'apertureRadius' | 'filterTransmission' | 'ringCurrent' | 'dataRawFilenamePattern' | 'microscopeImageFilenamePattern' | 'apertureHorizontal' | 'apertureVertical' | 'dewarPosition'>
    )>>> }
  )>> }
);

export type TargetsQueryVariables = Exact<{ [key: string]: never; }>;


export type TargetsQuery = (
  { __typename?: 'Query' }
  & { targets?: Maybe<Array<(
    { __typename?: 'Target' }
    & Pick<Target, 'targetId' | 'name' | 'created'>
    & { plasmids?: Maybe<Array<Maybe<(
      { __typename?: 'Plasmid' }
      & Pick<Plasmid, 'plasmidId' | 'name'>
    )>>> }
  )>> }
);


export const AddMetadataDocument = gql`
    mutation addMetadata($metadataData: MetadataInput!) {
  addMetadata(metadataData: $metadataData) {
    errorMessage
  }
}
    `;
export const AddPlasmidDocument = gql`
    mutation addPlasmid($plasmidData: PlasmidInput!) {
  addPlasmid(plasmidData: $plasmidData) {
    errorMessage
  }
}
    `;
export const AddPuckDocument = gql`
    mutation addPuck($puckData: PuckInput!) {
  addPuck(puckData: $puckData) {
    errorMessage
  }
}
    `;
export const AddTargetDocument = gql`
    mutation addTarget($id: String!, $name: String!) {
  addTarget(id: $id, name: $name) {
    errorMessage
  }
}
    `;
export const CrystalIdsDocument = gql`
    query crystalIds {
  crystals {
    crystalId
  }
}
    `;
export const HitsDocument = gql`
    query hits {
  hits {
    hitId
    refinementId
    method
    comment
    bindingSite
    closestResidue
    confidence
    isInteresting
    inspectedBy
    metadataColumn
  }
}
    `;
export const UpsertHitDocument = gql`
    mutation upsertHit($hitData: HitInput!) {
  upsertHit(data: $hitData) {
    errorMessage
  }
}
    `;
export const RemoveHitDocument = gql`
    mutation removeHit($entityId: Int!) {
  removeHit(entityId: $entityId) {
    errorMessage
  }
}
    `;
export const MetadatasDocument = gql`
    query metadatas {
  metadatas {
    metadataId
    created
    title
  }
}
    `;
export const ModifyMetadataDocument = gql`
    mutation modifyMetadata($metadataData: MetadataInput!) {
  modifyMetadata(metadataData: $metadataData) {
    errorMessage
  }
}
    `;
export const ModifyPlasmidDocument = gql`
    mutation modifyPlasmid($plasmidData: PlasmidInput!) {
  modifyPlasmid(plasmidData: $plasmidData) {
    errorMessage
  }
}
    `;
export const ModifyPuckDocument = gql`
    mutation modifyPuck($puckData: PuckInput!) {
  modifyPuck(puckData: $puckData) {
    errorMessage
  }
}
    `;
export const ModifyTargetDocument = gql`
    mutation modifyTarget($id: String!, $name: String!) {
  modifyTarget(id: $id, name: $name) {
    errorMessage
  }
}
    `;
export const PlasmidsDocument = gql`
    query plasmids {
  plasmids {
    plasmidId
    created
    name
    expression
    purification
    crystallization
    expressionSystem
    expressionLab
    expressionProtocol
    targets {
      targetId
      name
    }
  }
}
    `;
export const PucksDocument = gql`
    query pucks {
  pucks {
    puckId
    puckType
    owner
    created
  }
}
    `;
export const ReductionsDocument = gql`
    query reductions($crystalId: String!, $runIds: [String!]!) {
  reductions(crystalId: $crystalId, runIds: $runIds) {
    dataReductionId
    runId
    method
    analysisTime
    folderPath
    mtzPath
    comment
    resolutionCc
    resolutionIsigma
    a
    b
    c
    alpha
    beta
    gamma
    spaceGroup
    isigi
    rmeas
    cchalf
    rfactor
    WilsonB
    refinements {
      refinementId
      method
      analysisTime
      folderPath
      initialPdbPath
      finalPdbPath
      refinementMtzPath
      comment
      resolutionCut
      rfree
      rwork
      rmsBondLength
      rmsBondAngle
      numBlobs
      averageModelB
    }
  }
}
    `;
export const SingleCrystalDocument = gql`
    query singleCrystal($crystalId: String!) {
  crystals(crystalId: $crystalId) {
    created
    creatorId
    puckPositionId
    puckId
    compoundDrop {
      compoundId
      compound {
        created
        crystalTrayId
        wellId
        name
        iupacName
        target
        phase
        intraSurface
        subCategory
        pubchemCid
        sourcePlateBarcode
        sourcePlateWell
        molWeight
        saltData
        molFormula
        stereochemistry
        smiles
      }
    }
    target {
      targetId
      created
      name
      plasmids {
        plasmidId
        created
        name
        expression
        purification
        crystallization
        expressionSystem
        expressionLab
        expressionProtocol
      }
    }
    diffractions {
      runId
      created
      beamline
      beamIntensity
      pinhole
      focusing
      diffraction
      comment
      metadataColumn
      angleStart
      numberOfFrames
      angleStep
      exposureTime
      xrayEnergy
      xrayWavelength
      detectorName
      detectorDistance
      detectorEdgeResolution
      apertureRadius
      filterTransmission
      ringCurrent
      dataRawFilenamePattern
      microscopeImageFilenamePattern
      apertureHorizontal
      apertureVertical
      dewarPosition
      comment
    }
  }
}
    `;
export const TargetsDocument = gql`
    query targets {
  targets {
    targetId
    name
    created
    plasmids {
      plasmidId
      name
    }
  }
}
    `;

export type SdkFunctionWrapper = <T>(action: () => Promise<T>) => Promise<T>;


const defaultWrapper: SdkFunctionWrapper = sdkFunction => sdkFunction();
export function getSdk(client: GraphQLClient, withWrapper: SdkFunctionWrapper = defaultWrapper) {
  return {
    addMetadata(variables: AddMetadataMutationVariables): Promise<AddMetadataMutation> {
      return withWrapper(() => client.request<AddMetadataMutation>(print(AddMetadataDocument), variables));
    },
    addPlasmid(variables: AddPlasmidMutationVariables): Promise<AddPlasmidMutation> {
      return withWrapper(() => client.request<AddPlasmidMutation>(print(AddPlasmidDocument), variables));
    },
    addPuck(variables: AddPuckMutationVariables): Promise<AddPuckMutation> {
      return withWrapper(() => client.request<AddPuckMutation>(print(AddPuckDocument), variables));
    },
    addTarget(variables: AddTargetMutationVariables): Promise<AddTargetMutation> {
      return withWrapper(() => client.request<AddTargetMutation>(print(AddTargetDocument), variables));
    },
    crystalIds(variables?: CrystalIdsQueryVariables): Promise<CrystalIdsQuery> {
      return withWrapper(() => client.request<CrystalIdsQuery>(print(CrystalIdsDocument), variables));
    },
    hits(variables?: HitsQueryVariables): Promise<HitsQuery> {
      return withWrapper(() => client.request<HitsQuery>(print(HitsDocument), variables));
    },
    upsertHit(variables: UpsertHitMutationVariables): Promise<UpsertHitMutation> {
      return withWrapper(() => client.request<UpsertHitMutation>(print(UpsertHitDocument), variables));
    },
    removeHit(variables: RemoveHitMutationVariables): Promise<RemoveHitMutation> {
      return withWrapper(() => client.request<RemoveHitMutation>(print(RemoveHitDocument), variables));
    },
    metadatas(variables?: MetadatasQueryVariables): Promise<MetadatasQuery> {
      return withWrapper(() => client.request<MetadatasQuery>(print(MetadatasDocument), variables));
    },
    modifyMetadata(variables: ModifyMetadataMutationVariables): Promise<ModifyMetadataMutation> {
      return withWrapper(() => client.request<ModifyMetadataMutation>(print(ModifyMetadataDocument), variables));
    },
    modifyPlasmid(variables: ModifyPlasmidMutationVariables): Promise<ModifyPlasmidMutation> {
      return withWrapper(() => client.request<ModifyPlasmidMutation>(print(ModifyPlasmidDocument), variables));
    },
    modifyPuck(variables: ModifyPuckMutationVariables): Promise<ModifyPuckMutation> {
      return withWrapper(() => client.request<ModifyPuckMutation>(print(ModifyPuckDocument), variables));
    },
    modifyTarget(variables: ModifyTargetMutationVariables): Promise<ModifyTargetMutation> {
      return withWrapper(() => client.request<ModifyTargetMutation>(print(ModifyTargetDocument), variables));
    },
    plasmids(variables?: PlasmidsQueryVariables): Promise<PlasmidsQuery> {
      return withWrapper(() => client.request<PlasmidsQuery>(print(PlasmidsDocument), variables));
    },
    pucks(variables?: PucksQueryVariables): Promise<PucksQuery> {
      return withWrapper(() => client.request<PucksQuery>(print(PucksDocument), variables));
    },
    reductions(variables: ReductionsQueryVariables): Promise<ReductionsQuery> {
      return withWrapper(() => client.request<ReductionsQuery>(print(ReductionsDocument), variables));
    },
    singleCrystal(variables: SingleCrystalQueryVariables): Promise<SingleCrystalQuery> {
      return withWrapper(() => client.request<SingleCrystalQuery>(print(SingleCrystalDocument), variables));
    },
    targets(variables?: TargetsQueryVariables): Promise<TargetsQuery> {
      return withWrapper(() => client.request<TargetsQuery>(print(TargetsDocument), variables));
    }
  };
}
export type Sdk = ReturnType<typeof getSdk>;