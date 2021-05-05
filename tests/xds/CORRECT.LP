
 ***** CORRECT ***** (VERSION Feb 5, 2021  BUILT=20210205)   3-May-2021

 INPUT PARAMETER VALUES
 ----------------------
 SPACE_GROUP_NUMBER=    5 as used in the INTEGRATE step
 UNIT_CELL_CONSTANTS=   113.764    53.648    44.611  90.000 101.841  90.000 as used by INTEGRATE
 FRIEDEL'S_LAW=TRUE 
 PROFILE_FITTING= TRUE 
 OVERLOAD=    283649     MINPK=  75.00000    WFAC1=   1.0
 INCLUDE_RESOLUTION_RANGE=    50.000     1.350
 NAME_TEMPLATE_OF_DATA_FRAMES=/gpfs/current/raw/Mpro/bmx001_pos06/bmx001_pos06_001/bmx001_pos06_001_??????.h5 GENERIC
 DATA_RANGE=       1    1000
 ROTATION_AXIS=  0.999982  0.000745  0.005858
 OSCILLATION_RANGE=  0.200000
 STARTING_ANGLE=     0.000     STARTING_FRAME=       1
 X-RAY_WAVELENGTH=  1.033200
 INCIDENT_BEAM_DIRECTION=  0.003463 -0.000987  0.999994
 FRACTION_OF_POLARIZATION=   0.990
 POLARIZATION_PLANE_NORMAL=  0.000000  1.000000  0.000000
 AIR=   0.00100
 DETECTOR=Dectris
 SILICON=  4.345242 SENSOR_THICKNESS=  0.450000
 NUMBER OF DETECTOR SEGMENTS   1
 NX=  4148  NY=  4362    QX=  0.075000  QY=  0.075000
 ORGX=   2121.30  ORGY=   2188.04
 DETECTOR_DISTANCE=   156.026
 DIRECTION_OF_DETECTOR_X-AXIS=   1.00000   0.00000   0.00000
 DIRECTION_OF_DETECTOR_Y-AXIS=   0.00000   1.00000   0.00000
 BEAM_DIVERGENCE_E.S.D.=   0.041
 REFLECTING_RANGE_E.S.D.=   0.826
 MINIMUM_ZETA=     0.050
 MAXIMUM_ERROR_OF_SPOT_POSITION=   3.0
 MAXIMUM_ERROR_OF_SPINDLE_POSITION=   2.0
 MINIMUM_I/SIGMA=   3.0
 REFLECTIONS/CORRECTION_FACTOR=      50
 SNRC=  0.10 I/sigma for use in scaling factor determination
 STRICT_ABSORPTION_CORRECTION=FALSE 
 CORRECTIONS= DECAY MODULATION ABSORPTION                      
 REFERENCE_DATA_SET=                                                  
 FIT_B-FACTOR_TO_REFERENCE_DATA_SET=FALSE 

  159369 REFLECTIONS ON FILE "INTEGRATE.HKL"
       0 CORRUPTED REFLECTION RECORDS (IGNORED)
       0 MATCHING REFLECTIONS SPECIFIED IN FILE "FILTER.HKL"
       0 REFLECTIONS INCOMPLETE OR OUTSIDE IMAGE RANGE       1 ...    1000
       0 OVERLOADED REFLECTIONS (IGNORED)
      87 REFLECTIONS OUTSIDE ACCEPTED RESOLUTION RANGES
                OR TOO CLOSE TO ROTATION AXIS (IGNORED)
  159282 REFLECTIONS ACCEPTED


 NUMBER OF UNIQUE REFLECTIONS IN FILE "REMOVE.HKL"       1



 ******************************************************************************
                      AUTOMATIC SPACE GROUP ASSIGNMENT
 ******************************************************************************

 XDS adopts the following approach.
 (1) it looks for possible symmetries of the crystal lattice
 (2) it computes a redundancy independent R-factor for all enantiomorphous
     point groups compatible with the observed lattice symmetry.
 (3) it selects the group which explains the intensity data at an acceptable,
     redundancy-independent R-factor (Rmeas, Rrim) using a minimum number of
     unique reflections.

 This approach does not test for the presence of screw axes. Consequently,
 orthorhombic cell axes will be specified in increasing length (following
 conventions), despite the possibility that different assignments for the
 cell axes could become necessary for space groups P222(1) and P2(1)2(1)2
 containing one or two screw axes, respectively.

 The user can always override the automatic decisions by specifying the
 correct space group number and unit cell constants in XDS.INP and repeating
 the CORRECT step of XDS. This provides a simple way to rename orthorhombic
 cell constants if screw axes are present.

 In addition, the user has the option to specify in XDS.INP
 (a) a reference data set or
 (b) a reindexing transformation or
 (c) the three basis vectors (if known from processing a previous data set
 taken at the same crystal orientation in a multi-wavelength experiment).
 These features of XDS are useful for resolving the issue of alternative
 settings of polar or rhombohedral cells (like P4, P6, R3).

 =====> Specifications provided by the user in XDS.INP:
 SPACE_GROUP_NUMBER=    5 (0: unknown symmetry and unit cell)
 UNIT_CELL_CONSTANTS=   113.93    53.73    44.67  90.000 101.840  90.000
 UNIT_CELL_A-AXIS=     0.000     0.000     0.000 (0 0 0 : unknown)
 UNIT_CELL_B-AXIS=     0.000     0.000     0.000 (0 0 0 : unknown)
 UNIT_CELL_C-AXIS=     0.000     0.000     0.000 (0 0 0 : unknown)
 REIDX=  0  0  0  0  0  0  0  0  0  0  0  0 (all 0 : not specified)
 TEST_RESOLUTION_RANGE=   10.00    5.00 (Angstrom) for space group determination
 NUMBER OF ACCEPTED REFLECTIONS FOR CALCULATING Rmeas             4189
 NUMBER OF ACCEPTED UNIQUE REFLECTIONS FROM REFERENCE DATA SET       0


 *********** DETERMINATION OF LATTICE CHARACTER AND BRAVAIS LATTICE ***********

 The CHARACTER OF A LATTICE is defined by the metrical parameters of its
 reduced cell as described in the INTERNATIONAL TABLES FOR CRYSTALLOGRAPHY
 Volume A, p. 746 (KLUWER ACADEMIC PUBLISHERS, DORDRECHT/BOSTON/LONDON, 1989).
 Note that more than one lattice character may have the same BRAVAIS LATTICE.

 A lattice character is marked "*" to indicate a lattice consistent with the
 observed locations of the diffraction spots. These marked lattices must have
 low values for the QUALITY OF FIT and their implicated UNIT CELL CONSTANTS
 should not violate the ideal values by more than
 MAX_CELL_AXIS_ERROR=  0.03
 MAX_CELL_ANGLE_ERROR=           2.0 (Degrees)

 The REINDEXING TRANSFORMATION REIDX() consists of 12 integers that relate
 the original indices H,K,L from file INTEGRATE.HKL to the indices H',K',L'
 with respect to the new cell.
    H'=(REIDX(1)*H+REIDX( 2)*K+REIDX( 3)*L)/IDXV+REIDX( 4)
    K'=(REIDX(5)*H+REIDX( 6)*K+REIDX( 7)*L)/IDXV+REIDX( 8)
    L'=(REIDX(9)*H+REIDX(10)*K+REIDX(11)*L)/IDXV+REIDX(12)
 The value of the integer IDXV depends on the lattice type used for
 specifying reflections on file INTEGRATE.HKL; IDXV is
 1 for a primitive,
 2 for a face or body centred,
 3 for a rhombohedral,
 4 for a lattice centred on all faces.
 IDXV is set by XDS and cannot be input by the user.

  LATTICE-  BRAVAIS-   QUALITY  UNIT CELL CONSTANTS (ANGSTROEM & DEGREES)    REINDEXING TRANSFORMATION
 CHARACTER  LATTICE     OF FIT      a      b      c   alpha  beta gamma

 *  44        aP          0.0      44.6   53.6   62.9 115.2 100.7  90.0    0  0 -2  0  0 -2  0  0 -1  1  0  0
 *  31        aP          0.0      44.6   53.6   62.9  64.8  79.3  90.0    0  0  2  0  0  2  0  0 -1  1  0  0
 *  41        mC          0.0     113.8   53.6   44.6  90.0 101.8  90.0    2  0  0  0  0  2  0  0  0  0  2  0
    37        mC        119.5     125.4   44.6   53.6  90.0 115.3  80.2   -2  2 -2  0  0  0 -2  0  0 -2  0  0
    42        oI        119.5      44.6   53.6  113.4  90.0 100.8  90.0    0  0  2  0  0  2  0  0 -2  0 -2  0
    35        mP        130.5      53.6   44.6   62.9 100.7 115.2  90.0    0  2  0  0  0  0  2  0  1 -1  0  0
    40        oC        130.5      53.6  113.8   44.6  78.2  90.0  90.0    0  2  0  0 -2  0  0  0  0  0  2  0
    30        mC        130.5      53.6  113.8   44.6  78.2  90.0  90.0    0  2  0  0  2  0  0  0  0  0 -2  0
    33        mP        361.4      44.6   53.6   62.9 115.2 100.7  90.0    0  0 -2  0  0 -2  0  0 -1  1  0  0
    27        mC        369.5     116.2   44.6   62.9 100.7 117.7  67.4    0  4 -2  0  0  0 -2  0 -1 -1  0  0
    39        mC        380.5     116.2   44.6   62.9 100.7 117.7  67.4    0  4  2  0  0  0  2  0  1 -1  0  0
    25        mC        401.0      62.9   98.5   44.6  96.8 100.7  80.0   -1 -1  0  0 -1  3  0  0  0  0 -2  0
    23        oC        401.0      62.9   98.5   44.6  96.8 100.7  80.0   -1 -1  0  0 -1  3  0  0  0  0 -2  0
    20        mC        401.0      98.5   62.9   44.6  79.3  96.8 100.0   -1  3  0  0  1  1  0  0  0  0 -2  0
    22        hP        401.0      53.6   62.9   44.6 100.7  90.0 115.2    0 -2  0  0 -1  1  0  0  0  0 -2  0
    10        mC        453.7      69.8   69.8   62.9  77.9 116.5 100.5    0  2  2  0  0 -2  2  0  1 -1  0  0
    14        mC        453.7      69.8   69.8   62.9 102.1 116.5  79.5    0 -2 -2  0  0 -2  2  0 -1  1  0  0
    15        tI        453.7      44.6   53.6  113.4  90.0  79.2  90.0    0  0 -2  0  0 -2  0  0 -2  0 -2  0
    43        mI        477.8      69.8  113.4   53.6  90.0 140.2  83.1    0 -2  2  0  2  0  2  0  0  2  0  0
    36        oC        480.9      44.6  125.4   53.6  64.7  90.0  99.8    0  0 -2  0  2 -2  2  0  0 -2  0  0
    28        mC        480.9      44.6  125.4   53.6  64.7  90.0  80.2    0  0 -2  0 -2  2 -2  0  0  2  0  0
    34        mP        491.9      44.6   62.9   53.6 115.2  90.0 100.7    0  0  2  0  1 -1  0  0  0  2  0  0
    32        oP        491.9      44.6   53.6   62.9 115.2 100.7  90.0    0  0 -2  0  0 -2  0  0 -1  1  0  0
    29        mC        546.1      44.6  116.2   62.9  71.2 100.7  67.4    0  0  2  0  0 -4  2  0  1 -1  0  0
    26        oF        605.9      44.6  116.2  125.4  62.6  99.8 112.6    0  0  2  0  0  4 -2  0 -2  2 -2  0
    19        oI        640.0      44.6   62.9  103.2  85.1  71.4 100.7    0  0 -2  0 -1 -1  0  0 -1  3 -2  0
    24        hR        676.1     103.2   98.5   44.6  96.8  71.4 111.7   -1 -3 -2  0 -1  3  0  0  0  0 -2  0
     9        hR        703.7      44.6   69.8  169.4  80.9  93.3 129.8    0  0  2  0  0  2 -2  0 -3  1 -2  0
    13        oC        714.7      69.8   69.8   62.9 102.1 116.5  79.5    0 -2 -2  0  0 -2  2  0 -1  1  0  0
    11        tP        714.8      44.6   53.6   62.9 115.2 100.7  90.0    0  0 -2  0  0 -2  0  0 -1  1  0  0
    38        oC        741.9      44.6  116.2   62.9  62.3 100.7 112.6    0  0  2  0  0 -4 -2  0  1 -1  0  0
    21        tP        762.4      53.6   62.9   44.6 100.7  90.0 115.2    0 -2  0  0 -1  1  0  0  0  0 -2  0
     4        hR        854.7      69.8   83.6   70.0  95.1  90.4 100.6    0  2 -2  0 -1  1  2  0  1  1  2  0
     2        hR        854.7      69.8   70.0  112.9 115.5  76.7 126.5    0 -2  2  0 -1  1 -2  0  1 -3 -2  0
    18        tI        876.4      62.9  103.2   44.6  71.4  79.3  94.9   -1 -1  0  0  1 -3  2  0  0  0  2  0
    17        mC        931.5      69.8   69.8   70.0  89.6 126.5  79.5    0  2 -2  0  0  2  2  0  1 -1  2  0
    12        hP        964.7      44.6   53.6   62.9 115.2 100.7  90.0    0  0 -2  0  0 -2  0  0 -1  1  0  0
     1        cF        966.1      83.6   70.0  103.2 106.3  96.1 109.6   -1 -1  2  0  1  1  2  0 -1  3 -2  0
     3        cP        999.0      44.6   53.6   62.9 115.2 100.7  90.0    0  0 -2  0  0 -2  0  0 -1  1  0  0
     5        cI        999.0      70.0   69.8   62.9  77.9  63.1  89.6   -1  1 -2  0  0 -2 -2  0 -1 -1  0  0
     6        tI        999.0      62.9   70.0   69.8  89.6  77.9  63.1   -1 -1  0  0 -1  1 -2  0  0 -2 -2  0
     7        tI        999.0      70.0   69.8   62.9  77.9  63.1  89.6   -1  1 -2  0  0 -2 -2  0 -1 -1  0  0
     8        oI        999.0      69.8   70.0   62.9  63.1  77.9  89.6    0  2  2  0  1 -1  2  0  1  1  0  0
    16        oF        999.0      69.8   69.8  113.4  83.1  96.9  79.5    0  2  2  0  0  2 -2  0 -2  0 -2  0

 ****** LATTICE SYMMETRY IMPLICATED BY SPACE GROUP SYMMETRY ******

BRAVAIS-            POSSIBLE SPACE-GROUPS FOR PROTEIN CRYSTALS
  TYPE                     [SPACE GROUP NUMBER,SYMBOL]
  aP      [1,P1]
  mP      [3,P2] [4,P2(1)]
 mC,mI    [5,C2]
  oP      [16,P222] [17,P222(1)] [18,P2(1)2(1)2] [19,P2(1)2(1)2(1)]
  oC      [21,C222] [20,C222(1)]
  oF      [22,F222]
  oI      [23,I222] [24,I2(1)2(1)2(1)]
  tP      [75,P4] [76,P4(1)] [77,P4(2)] [78,P4(3)] [89,P422] [90,P42(1)2]
          [91,P4(1)22] [92,P4(1)2(1)2] [93,P4(2)22] [94,P4(2)2(1)2]
          [95,P4(3)22] [96,P4(3)2(1)2]
  tI      [79,I4] [80,I4(1)] [97,I422] [98,I4(1)22]
  hP      [143,P3] [144,P3(1)] [145,P3(2)] [149,P312] [150,P321] [151,P3(1)12]
          [152,P3(1)21] [153,P3(2)12] [154,P3(2)21] [168,P6] [169,P6(1)]
          [170,P6(5)] [171,P6(2)] [172,P6(4)] [173,P6(3)] [177,P622]
          [178,P6(1)22] [179,P6(5)22] [180,P6(2)22] [181,P6(4)22] [182,P6(3)22]
  hR      [146,R3] [155,R32]
  cP      [195,P23] [198,P2(1)3] [207,P432] [208,P4(2)32] [212,P4(3)32]
          [213,P4(1)32]
  cF      [196,F23] [209,F432] [210,F4(1)32]
  cI      [197,I23] [199,I2(1)3] [211,I432] [214,I4(1)32]


 Several solutions exist for the given space group that explain the data. The
 selected solution (marked "*") correlates best with the given reference data set.
 The independent settings of the cell can be obtained by application of the
 REINDEXING TRANSFORMATION to the original indices H,K,L from file INTEGRATE.HKL

 CORRELATION = correlation factor with the reference data set
 NPAIR       = number of unique reflection pairs correlated 
 Rmeas=Rrim  = redundancy independent R-factor (intensities)
               For definition see:
               Rmeas : Diederichs & Karplus,
                       Nature Struct. Biol. 4, 269-275 (1997);
               Rrim  : Weiss & Hilgenfeld,
                       J.Appl.Cryst. 30,203-205 (1997).
 COMPARED    = number of reflections used for calculating Rmeas
 ESD         = Agreement with given unit cell geometry

 CORRELATION  NPAIR  Rmeas  COMPARED  ESD       REINDEX TRANSFORMATION

  *   0.00        0    5.8    3165    0.00  2  0  0  0  0  2  0  0  0  0  2  0

 ************ SELECTED SPACE GROUP AND UNIT CELL FOR THIS DATA SET ************

 SPACE_GROUP_NUMBER=    5
 UNIT_CELL_CONSTANTS=   113.93    53.73    44.67  90.000 101.840  90.000
 UNIT_CELL_A-AXIS=    17.990    73.244   -85.154
 UNIT_CELL_B-AXIS=    -2.093   -40.417   -35.206
 UNIT_CELL_C-AXIS=   -44.531    -0.075     2.733
 REFLECTIONS H,K,L FROM "INTEGRATE.HKL" WILL BE REINDEXED BY
 H' = (  2*H +  0*K +  0*L)/ 2 +  0
 K' = (  0*H +  2*K +  0*L)/ 2 +  0
 L' = (  0*H +  0*K +  2*L)/ 2 +  0

 *****  4 EQUIVALENT POSITIONS IN SPACE GROUP #  5 *****

    If x',y',z' is an equivalent position to x,y,z, then
        x'=x*ML(1)+y*ML( 2)+z*ML( 3)+ML( 4)/12.0
        y'=x*ML(5)+y*ML( 6)+z*ML( 7)+ML( 8)/12.0
        z'=x*ML(9)+y*ML(10)+z*ML(11)+ML(12)/12.0

    #    1  2  3  4    5  6  7  8    9 10 11 12 
    1    1  0  0  0    0  1  0  0    0  0  1  0
    2   -1  0  0  0    0  1  0  0    0  0 -1  0
    3    1  0  0  6    0  1  0  6    0  0  1  0
    4   -1  0  0  6    0  1  0  6    0  0 -1  0

 ******************************************************************************
      MEAN DISCREPANCIES BETWEEN OBSERVED AND CALCULATED SPOT LOCATIONS
 ******************************************************************************

 The discrepancies in X- and Y-coordinates of the spots are depicted in the
 two images DX-CORRECTIONS.cbf and DY-CORRECTIONS.cbf for inspection with
 the XDS-Viewer.



 ******************************************************************************
  REFINEMENT OF DIFFRACTION PARAMETERS USING ALL IMAGES
 ******************************************************************************


 REFINED VALUES OF DIFFRACTION PARAMETERS DERIVED FROM     26066 INDEXED SPOTS
 REFINED PARAMETERS:   AXIS BEAM ORIENTATION CELL POSITION
 STANDARD DEVIATION OF SPOT    POSITION (PIXELS)     0.61
 STANDARD DEVIATION OF SPINDLE POSITION (DEGREES)    0.21
 SPACE GROUP NUMBER      5
 UNIT CELL PARAMETERS    113.926    53.729    44.673  90.000 101.837  90.000
 E.S.D. OF CELL PARAMETERS  6.6E-02 2.2E-02 2.1E-02 0.0E+00 2.1E-02 0.0E+00
 REC. CELL PARAMETERS   0.008968  0.018612  0.022871  90.000  78.163  90.000
 COORDINATES OF UNIT CELL A-AXIS    18.095    73.288   -85.327
 COORDINATES OF UNIT CELL B-AXIS    -2.089   -40.508   -35.236
 COORDINATES OF UNIT CELL C-AXIS   -44.590    -0.068     2.721
 CRYSTAL MOSAICITY (DEGREES)     0.826
 LAB COORDINATES OF ROTATION AXIS  0.999981  0.000634  0.006074
 DIRECT BEAM COORDINATES (REC. ANGSTROEM)   0.003265 -0.000690  0.967861
 DETECTOR COORDINATES (PIXELS) OF DIRECT BEAM    2128.45   2185.94
 DETECTOR ORIGIN (PIXELS) AT                     2121.42   2187.43
 CRYSTAL TO DETECTOR DISTANCE (mm)       156.27
 LAB COORDINATES OF DETECTOR X-AXIS  1.000000  0.000000  0.000000
 LAB COORDINATES OF DETECTOR Y-AXIS  0.000000  1.000000  0.000000


 THE DATA COLLECTION STATISTICS REPORTED BELOW ASSUMES:
 SPACE_GROUP_NUMBER=    5
 UNIT_CELL_CONSTANTS=   113.93    53.73    44.67  90.000 101.837  90.000


 ******************************************************************************
        MEAN INTENSITY AS FUNCTION OF SPINDLE POSITION WITHIN DATA IMAGE
 ******************************************************************************

 This statistics could serve as a diagnostic tool for detecting
 shutter problems (suggested by Kay Diederichs).
 Data are corrected for this effect if PATCH_SHUTTER_PROBLEM=TRUE in XDS.INP.
 ===> Selected : PATCH_SHUTTER_PROBLEM=FALSE

 INTERVAL = Angular interval in units of the oscillation range
            covered by a data image.
 NUMBER   = Number of reflections in each interval
 INTENSITY= Mean intensity of the reflections in each interval
 FACTOR   = Correction factor applied to intensities;
            INTENSITY(overall)/INTENSITY(interval)

   INTERVAL     NUMBER      INTENSITY      FACTOR
 -0.05  0.05     15953        347.055       1.015
  0.05  0.15     15825        362.777       0.971
  0.15  0.25     16056        349.611       1.008
  0.25  0.35     16024        339.048       1.039
  0.35  0.45     15947        359.200       0.981
  0.45  0.55     15954        360.006       0.978
  0.55  0.65     16047        364.787       0.966
  0.65  0.75     15833        345.051       1.021
  0.75  0.85     15820        345.623       1.019
  0.85  0.95     15822        349.334       1.008
 
 -0.05  0.95    159281        352.254       1.000
 

 ******************************************************************************
           CORRECTION FACTORS AS FUNCTION OF IMAGE NUMBER & RESOLUTION
 ******************************************************************************

 RECIPROCAL CORRECTION FACTORS FOR INPUT DATA SETS MERGED TO
 OUTPUT FILE: XDS_ASCII.HKL                                     

 THE CALCULATIONS ASSUME         FRIEDEL'S_LAW= TRUE
 TOTAL NUMBER OF CORRECTION FACTORS DEFINED      800
 DEGREES OF FREEDOM OF CHI^2 FIT             61370.9
 CHI^2-VALUE OF FIT OF CORRECTION FACTORS      0.837
 NUMBER OF CYCLES CARRIED OUT                      5

 CORRECTION FACTORS for visual inspection by XDS-Viewer DECAY.cbf           
 XMIN=     2.7 XMAX=   997.0 NXBIN=   40
 YMIN= 0.00043 YMAX= 0.54600 NYBIN=   20
 NUMBER OF REFLECTIONS USED FOR DETERMINING CORRECTION FACTORS      93031


 ******************************************************************************
  CORRECTION FACTORS AS FUNCTION OF X (fast) & Y(slow) IN THE DETECTOR PLANE
 ******************************************************************************

 RECIPROCAL CORRECTION FACTORS FOR INPUT DATA SETS MERGED TO
 OUTPUT FILE: XDS_ASCII.HKL                                     

 THE CALCULATIONS ASSUME         FRIEDEL'S_LAW= TRUE
 TOTAL NUMBER OF CORRECTION FACTORS DEFINED     1892
 DEGREES OF FREEDOM OF CHI^2 FIT             61248.5
 CHI^2-VALUE OF FIT OF CORRECTION FACTORS      0.819
 NUMBER OF CYCLES CARRIED OUT                      3

 CORRECTION FACTORS for visual inspection by XDS-Viewer MODPIX.cbf          
 XMIN=    93.7 XMAX=  4112.5 NXBIN=   43
 YMIN=   110.5 YMAX=  4249.6 NYBIN=   44
 NUMBER OF REFLECTIONS USED FOR DETERMINING CORRECTION FACTORS      93031


 ******************************************************************************
   CORRECTION FACTORS AS FUNCTION OF IMAGE NUMBER & DETECTOR SURFACE POSITION
 ******************************************************************************

 RECIPROCAL CORRECTION FACTORS FOR INPUT DATA SETS MERGED TO
 OUTPUT FILE: XDS_ASCII.HKL                                     

 THE CALCULATIONS ASSUME         FRIEDEL'S_LAW= TRUE
 TOTAL NUMBER OF CORRECTION FACTORS DEFINED      520
 DEGREES OF FREEDOM OF CHI^2 FIT             61392.0
 CHI^2-VALUE OF FIT OF CORRECTION FACTORS      0.720
 NUMBER OF CYCLES CARRIED OUT                      4

 CORRECTION FACTORS for visual inspection by XDS-Viewer ABSORP.cbf          
 XMIN=     2.7 XMAX=   997.0 NXBIN=   40
 DETECTOR_SURFACE_POSITION=    2103    2180
 DETECTOR_SURFACE_POSITION=    2786    2883
 DETECTOR_SURFACE_POSITION=    1421    2883
 DETECTOR_SURFACE_POSITION=    1421    1477
 DETECTOR_SURFACE_POSITION=    2786    1477
 DETECTOR_SURFACE_POSITION=    3648    2839
 DETECTOR_SURFACE_POSITION=    2743    3771
 DETECTOR_SURFACE_POSITION=    1463    3771
 DETECTOR_SURFACE_POSITION=     558    2839
 DETECTOR_SURFACE_POSITION=     558    1521
 DETECTOR_SURFACE_POSITION=    1463     589
 DETECTOR_SURFACE_POSITION=    2743     589
 DETECTOR_SURFACE_POSITION=    3648    1521
 NUMBER OF REFLECTIONS USED FOR DETERMINING CORRECTION FACTORS      93031


 ******************************************************************************
    CORRECTION PARAMETERS FOR THE STANDARD ERROR OF REFLECTION INTENSITIES
 ******************************************************************************

 The variance v0(I) of the intensity I obtained from counting statistics is
 replaced by v(I)=a*(v0(I)+b*I^2). The model parameters a, b are chosen to
 minimize the discrepancies between v(I) and the variance estimated from
 sample statistics of symmetry related reflections. This model implicates
 an asymptotic limit ISa=1/SQRT(a*b) for the highest I/Sigma(I) that the
 experimental setup can produce (Diederichs (2010) Acta Cryst D66, 733-740).

     a        b          ISa
 8.752E-01  1.362E-03   28.96


 ******************************************************************************
      STANDARD ERROR OF REFLECTION INTENSITIES AS FUNCTION OF RESOLUTION
      FOR DATA SET  XDS_ASCII.HKL                                     
 ******************************************************************************

  I/Sigma  = mean intensity/Sigma of a reflection in shell
  Chi^2    = goodness of fit between sample variances of
             symmetry-related intensities and their errors
             (Chi^2 = 1 for perfect agreement)
  R-FACTOR
  observed = (SUM(ABS(I(h,i)-I(h))))/(SUM(I(h,i)))
  expected = expected R-FACTOR derived from Sigma(I)

   NUMBER  = number of reflections in resolution shell
             used for calculation of R-FACTOR
  ACCEPTED = number of accepted reflections
  REJECTED = number of rejected reflections (MISFITS),
             recognized by comparison with symmetry-related
             reflections.

 RESOLUTION RANGE  I/Sigma  Chi^2  R-FACTOR  R-FACTOR  NUMBER ACCEPTED REJECTED
                                   observed  expected

   48.403  13.038    28.27   0.77      1.59      2.31     197     202      34
   13.038   9.391    28.17   0.63      1.35      2.35     415     417      27
    9.391   7.717    26.68   0.98      1.77      2.40     550     551      20
    7.717   6.704    26.06   1.02      1.99      2.45     690     690      27
    6.704   6.008    24.63   1.27      2.58      2.50     731     731      38
    6.008   5.491    24.16   1.35      2.62      2.51     805     805      45
    5.491   5.089    24.29   1.28      2.40      2.50     909     909      59
    5.089   4.763    24.98   1.24      2.36      2.48     924     924      71
    4.763   4.493    24.94   1.24      2.27      2.49     950     950      83
    4.493   4.265    25.53   1.26      2.37      2.48    1101    1102     108
    4.265   4.068    24.28   1.37      2.65      2.51     946     955     104
    4.068   3.896    23.25   1.50      2.75      2.53     757     781      76
    3.896   3.744    21.34   1.50      3.03      2.59     867     889     125
    3.744   3.608    20.79   1.65      3.36      2.69     952     968     176
    3.608   3.487    18.64   1.68      3.53      2.83    1004    1015     185
    3.487   3.376    18.17   1.70      3.64      2.90    1100    1116     189
    3.376   3.276    17.31   1.65      3.85      3.03    1169    1177     221
    3.276   3.184    15.18   1.65      4.53      3.49    1126    1134     182
    3.184   3.100    13.41   1.61      5.23      3.85    1337    1346     180
    3.100   3.021    12.71   1.65      5.61      4.17    1295    1301     186
    3.021   2.949    11.05   1.59      6.14      4.76    1395    1402     159
    2.949   2.881    10.79   1.47      6.26      5.06    1426    1430     123
    2.881   2.818     9.71   1.41      7.39      5.96    1498    1501     107
    2.818   2.759     8.46   1.41      7.71      6.30    1550    1558      99
    2.759   2.703     7.85   1.26      8.32      7.25    1611    1614      75
    2.703   2.651     7.41   1.18      8.22      7.43    1726    1732      71
    2.651   2.602     6.48   1.13      9.94      9.09    1663    1665      42
    2.602   2.555     6.41   1.11      9.67      9.19    1829    1833      34
    2.555   2.511     5.70   1.03     10.87     10.58    1781    1782      34
    2.511   2.469     4.80   0.97     13.02     13.23    1937    1937      18
    2.469   2.429     4.41   0.98     14.37     14.57    1844    1849      16
    2.429   2.390     4.01   0.92     15.40     15.93    1993    1996      25
    2.390   2.354     3.57   0.92     17.62     18.30    1917    1919      17
    2.354   2.319     3.55   0.87     16.65     18.09    1964    1968      15
    2.319   2.286     3.09   0.89     20.53     21.78    2119    2124      16
    2.286   2.254     2.89   0.92     22.38     23.45    1900    1902       8
    2.254   2.223     2.77   0.94     24.52     25.07    2177    2181      56
    2.223   2.194     2.60   0.94     26.29     26.79    2002    2004       4
    2.194   2.166     2.12   0.83     29.79     32.87    2070    2076      13
    2.166   2.139     2.11   0.91     31.49     32.53    1767    1789      12
    2.139   2.112     1.71   0.82     36.93     40.61    1613    1636       5
    2.112   2.087     1.62   0.86     40.16     43.25    1844    1882       4
    2.087   2.063     1.46   0.94     47.20     48.55    1787    1821      31
    2.063   2.039     1.33   0.84     49.12     53.52    2052    2076       9
    2.039   2.016     1.17   0.79     53.16     59.47    2044    2068       6
    2.016   1.994     1.10   0.83     57.49     62.89    2058    2073       1
    1.994   1.973     0.97   0.82     66.29     72.61    2165    2181       5
    1.973   1.953     0.91   0.79     67.51     76.23    2187    2193       2
    1.953   1.933     0.94   0.84     67.01     73.36    2231    2235       9
    1.933   1.913     0.88   0.92     75.60     80.02    2245    2253      21
    1.913   1.894     0.63   0.78     97.11    110.20    2195    2214       8
    1.894   1.876     0.53   0.81    121.31    135.19    2345    2361      14
    1.876   1.858     0.46   0.75    130.58    149.80    2240    2249       2
    1.858   1.841     0.45   0.82    140.82    154.14    2356    2362       2
    1.841   1.824     0.42   0.75    143.52    166.05    2364    2371       4
    1.824   1.808     0.40   0.79    149.66    168.74    2317    2331       2
    1.808   1.792     0.34   0.78    180.29    203.40    2300    2307       1
    1.792   1.777     0.37   0.78    165.15    186.82    2487    2496       2
    1.777   1.761     0.26   0.81    240.51    267.01    2270    2274       0
    1.761   1.747     0.26   0.79    235.93    265.38    2330    2341       3
    1.747   1.732     0.23   0.74    251.38    295.23    2387    2407       1
    1.732   1.718     0.25   0.77    242.67    278.97    2305    2321       1
    1.718   1.705     0.22   0.81    277.71    306.59    2294    2315       3
    1.705   1.691     0.19   0.74    305.37    356.49    2326    2360       0
    1.691   1.678     0.19   0.73    308.79    363.02    2143    2182      10
    1.678   1.665     0.15   0.77    417.46    472.99    2030    2078       2
    1.665   1.653     0.14   0.81    422.76    468.80    2079    2152       2
    1.653   1.641     0.07   0.75    732.39    847.99    1975    2050       1
    1.641   1.629     0.13   0.73    434.25    507.52    1967    2053       0
    1.629   1.617     0.13   0.76    467.45    531.31    1860    1961       1
    1.617   1.606     0.09   0.75    643.83    740.30    1883    2001       1
    1.606   1.595     0.11   0.76    546.54    624.45    1596    1720       0
    1.595   1.584     0.08   0.77    821.86    922.37    1577    1738       1
    1.584   1.573     0.06   0.70    783.26    932.82    1339    1507       0
    1.573   1.562     0.10   0.67    534.59    649.72    1272    1406       0
    1.562   1.552     0.04   0.70   1405.76   1674.18    1131    1269       1
    1.552   1.542     0.10   0.82    504.02    559.65    1247    1439       5
    1.542   1.532     0.07   0.73    763.46    889.23    1199    1389       0
    1.532   1.522     0.04   0.82   1167.12   1290.00    1043    1246       6
    1.522   1.513     0.01   0.76    -99.90    -99.90    1154    1391       7
    1.513   1.504     0.05   0.73   1140.79   1317.14    1037    1266       0
    1.504   1.494     0.02   0.75   3596.85   4120.48     953    1193       0
    1.494   1.485     0.09   0.76    697.48    810.14     967    1262       3
    1.485   1.476     0.06   0.75   1335.76   1523.35     972    1250       1
    1.476   1.468     0.04   0.76   1121.10   1314.07     865    1148       3
    1.468   1.459     0.05   0.74    790.40    898.07     776    1056       3
    1.459   1.451     0.02   0.76   2747.75   3137.40     844    1162       0
    1.451   1.443    -0.00   0.78  17706.62  20100.31     819    1127       2
    1.443   1.434    -0.00   0.76    -99.90    -99.90     768    1075       0
    1.434   1.426     0.02   0.75   3587.18   4061.26     782    1075       0
    1.426   1.419     0.00   0.71    -99.90    -99.90     790    1078       1
    1.419   1.411     0.05   0.75   1042.12   1201.79     765    1071       0
    1.411   1.403    -0.03   0.67    -99.90    -99.90     693     988       0
    1.403   1.396    -0.03   0.61    -99.90    -99.90     638     930       0
    1.396   1.388    -0.01   0.68   3852.24   4700.41     717    1018       0
    1.388   1.381     0.01   0.70    -99.90    -99.90     741    1016       0
    1.381   1.374    -0.02   0.76    -99.90    -99.90     665     925       0
    1.374   1.367    -0.01   0.76   2554.12   2985.46     700     984       3
    1.367   1.360     0.03   0.81    892.08    975.44     633     921      10
    1.360   1.353    -0.04   0.63    -99.90    -99.90     289     523       0
   --------------------------------------------------------------------------
   48.403   1.353     3.87   0.93      6.67      6.96  147645  156002    3279
 


 ******************************************************************************
  SUMMARY OF DATA SET STATISTICS FOR VARIOUS SUBSETS OF INCLUDED DATA IMAGES
 ******************************************************************************

 Data set statistics is reported below several times, each with a different
 upper limit on the number of images included. This provides the user with
 the information for deciding which data images should be excluded from the
 final data set because of radiation damage or other defects. If the user
 decides for a subset of "good" images that differs from the specification
 as given by the input parameter (see DATA_RANGE= in XDS.INP) the CORRECT-
 step of XDS must be repeated with the new parameter values for DATA_RANGE=.


 ******************************************************************************
  SUMMARY OF DATA SET STATISTICS FOR IMAGE   DATA_RANGE=       1     112
 ******************************************************************************


          COMPLETENESS AND QUALITY OF DATA SET
          ------------------------------------

 R-FACTOR
 observed = (SUM(ABS(I(h,i)-I(h))))/(SUM(I(h,i)))
 expected = expected R-FACTOR derived from Sigma(I)

 COMPARED = number of reflections used for calculating R-FACTOR
 I/SIGMA  = mean of intensity/Sigma(I) of unique reflections
            (after merging symmetry-related observations)
 Sigma(I) = standard deviation of reflection intensity I
            estimated from sample statistics

 R-meas   = redundancy independent R-factor (intensities)
            Diederichs & Karplus (1997), Nature Struct. Biol. 4, 269-275.

 CC(1/2)  = percentage of correlation between intensities from
            random half-datasets. Correlation significant at
            the 0.1% level is marked by an asterisk.
            Karplus & Diederichs (2012), Science 336, 1030-33
 Anomal   = percentage of correlation between random half-sets
  Corr      of anomalous intensity differences. Correlation
            significant at the 0.1% level is marked.
 SigAno   = mean anomalous difference in units of its estimated
            standard deviation (|F(+)-F(-)|/Sigma). F(+), F(-)
            are structure factor estimates obtained from the
            merged intensity observations in each parity class.
  Nano    = Number of unique reflections used to calculate
            Anomal_Corr & SigAno. At least two observations
            for each (+ and -) parity are required.


 SUBSET OF INTENSITY DATA WITH SIGNAL/NOISE >= -3.0 AS FUNCTION OF RESOLUTION
 RESOLUTION     NUMBER OF REFLECTIONS    COMPLETENESS R-FACTOR  R-FACTOR COMPARED I/SIGMA   R-meas  CC(1/2)  Anomal  SigAno   Nano
   LIMIT     OBSERVED  UNIQUE  POSSIBLE     OF DATA   observed  expected                                      Corr

     4.05         891     746      2243       33.3%       1.8%      2.0%      290   27.11      2.6%    99.9*     0    0.000       0
     2.87        1387    1355      3960       34.2%       5.8%      3.2%       64   15.35      8.3%    99.2*     0    0.000       0
     2.34        2157    2147      5068       42.4%       6.0%     10.3%       20    5.25      8.5%    98.6*     0    0.000       0
     2.03        2378    2361      5989       39.4%      47.1%     34.7%       34    1.89     66.7%    74.2*     0    0.000       0
     1.82        2704    2670      6813       39.2%      59.3%     71.5%       68    0.53     83.8%    43.6      0    0.000       0
     1.66        3047    2996      7448       40.2%     370.7%    432.6%      102    0.17    524.2%   -11.7      0    0.000       0
     1.53        2959    2892      8120       35.6%     602.6%    700.7%      134    0.06    852.3%    -3.9      0    0.000       0
     1.44        2677    2613      8733       29.9%     674.5%    695.6%      128   -0.00    953.9%     1.9      0    0.000       0
     1.35        2253    2208      9253       23.9%    2705.8%   3404.7%       90   -0.03   3826.5%     1.6      0    0.000       0
    total       20453   19988     57627       34.7%       2.5%      2.6%      930    2.94      3.6%    99.9*     0    0.000       0


 NUMBER OF REFLECTIONS IN SELECTED SUBSET OF IMAGES     20801
 NUMBER OF REJECTED MISFITS                               345
 NUMBER OF SYSTEMATIC ABSENT REFLECTIONS                    0
 NUMBER OF ACCEPTED OBSERVATIONS                        20456
 NUMBER OF UNIQUE ACCEPTED REFLECTIONS                  19991



 ******************************************************************************
  SUMMARY OF DATA SET STATISTICS FOR IMAGE   DATA_RANGE=       1     223
 ******************************************************************************


          COMPLETENESS AND QUALITY OF DATA SET
          ------------------------------------

 R-FACTOR
 observed = (SUM(ABS(I(h,i)-I(h))))/(SUM(I(h,i)))
 expected = expected R-FACTOR derived from Sigma(I)

 COMPARED = number of reflections used for calculating R-FACTOR
 I/SIGMA  = mean of intensity/Sigma(I) of unique reflections
            (after merging symmetry-related observations)
 Sigma(I) = standard deviation of reflection intensity I
            estimated from sample statistics

 R-meas   = redundancy independent R-factor (intensities)
            Diederichs & Karplus (1997), Nature Struct. Biol. 4, 269-275.

 CC(1/2)  = percentage of correlation between intensities from
            random half-datasets. Correlation significant at
            the 0.1% level is marked by an asterisk.
            Karplus & Diederichs (2012), Science 336, 1030-33
 Anomal   = percentage of correlation between random half-sets
  Corr      of anomalous intensity differences. Correlation
            significant at the 0.1% level is marked.
 SigAno   = mean anomalous difference in units of its estimated
            standard deviation (|F(+)-F(-)|/Sigma). F(+), F(-)
            are structure factor estimates obtained from the
            merged intensity observations in each parity class.
  Nano    = Number of unique reflections used to calculate
            Anomal_Corr & SigAno. At least two observations
            for each (+ and -) parity are required.


 SUBSET OF INTENSITY DATA WITH SIGNAL/NOISE >= -3.0 AS FUNCTION OF RESOLUTION
 RESOLUTION     NUMBER OF REFLECTIONS    COMPLETENESS R-FACTOR  R-FACTOR COMPARED I/SIGMA   R-meas  CC(1/2)  Anomal  SigAno   Nano
   LIMIT     OBSERVED  UNIQUE  POSSIBLE     OF DATA   observed  expected                                      Corr

     4.05        1812    1247      2243       55.6%       1.8%      2.0%     1130   29.28      2.5%    99.9*     0    0.000       0
     2.87        2842    2154      3960       54.4%       4.3%      3.1%     1376   16.20      6.1%    99.6*     0    0.000       0
     2.34        4408    3283      5068       64.8%      13.0%     12.8%     2250    5.30     18.4%    96.6*     0    0.000       0
     2.03        4787    3696      5989       61.7%      36.8%     40.3%     2182    1.79     52.1%    84.3*     0    0.000       0
     1.82        5561    4362      6813       64.0%     137.8%    153.9%     2398    0.48    194.9%    23.7*     0    0.000       0
     1.66        6029    4854      7448       65.2%     437.6%    472.7%     2350    0.16    618.8%    -4.5      0    0.000       0
     1.53        4654    4151      8120       51.1%    1023.5%   1177.0%     1006    0.05   1447.4%     0.7      0    0.000       0
     1.44        3135    3033      8733       34.7%     740.3%    797.9%      204    0.01   1046.9%     3.3      0    0.000       0
     1.35        2351    2305      9253       24.9%    2538.2%   3151.1%       92   -0.04   3589.6%     0.3      0    0.000       0
    total       35579   29085     57627       50.5%       4.8%      4.9%    12988    3.39      6.8%    99.9*     0    0.000       0


 NUMBER OF REFLECTIONS IN SELECTED SUBSET OF IMAGES     36293
 NUMBER OF REJECTED MISFITS                               709
 NUMBER OF SYSTEMATIC ABSENT REFLECTIONS                    0
 NUMBER OF ACCEPTED OBSERVATIONS                        35584
 NUMBER OF UNIQUE ACCEPTED REFLECTIONS                  29089



 ******************************************************************************
  SUMMARY OF DATA SET STATISTICS FOR IMAGE   DATA_RANGE=       1     334
 ******************************************************************************


          COMPLETENESS AND QUALITY OF DATA SET
          ------------------------------------

 R-FACTOR
 observed = (SUM(ABS(I(h,i)-I(h))))/(SUM(I(h,i)))
 expected = expected R-FACTOR derived from Sigma(I)

 COMPARED = number of reflections used for calculating R-FACTOR
 I/SIGMA  = mean of intensity/Sigma(I) of unique reflections
            (after merging symmetry-related observations)
 Sigma(I) = standard deviation of reflection intensity I
            estimated from sample statistics

 R-meas   = redundancy independent R-factor (intensities)
            Diederichs & Karplus (1997), Nature Struct. Biol. 4, 269-275.

 CC(1/2)  = percentage of correlation between intensities from
            random half-datasets. Correlation significant at
            the 0.1% level is marked by an asterisk.
            Karplus & Diederichs (2012), Science 336, 1030-33
 Anomal   = percentage of correlation between random half-sets
  Corr      of anomalous intensity differences. Correlation
            significant at the 0.1% level is marked.
 SigAno   = mean anomalous difference in units of its estimated
            standard deviation (|F(+)-F(-)|/Sigma). F(+), F(-)
            are structure factor estimates obtained from the
            merged intensity observations in each parity class.
  Nano    = Number of unique reflections used to calculate
            Anomal_Corr & SigAno. At least two observations
            for each (+ and -) parity are required.


 SUBSET OF INTENSITY DATA WITH SIGNAL/NOISE >= -3.0 AS FUNCTION OF RESOLUTION
 RESOLUTION     NUMBER OF REFLECTIONS    COMPLETENESS R-FACTOR  R-FACTOR COMPARED I/SIGMA   R-meas  CC(1/2)  Anomal  SigAno   Nano
   LIMIT     OBSERVED  UNIQUE  POSSIBLE     OF DATA   observed  expected                                      Corr

     4.05        2678    1317      2243       58.7%       2.1%      2.2%     2256   34.15      2.7%    99.9*   -28    0.744      97
     2.87        4251    2280      3960       57.6%       4.7%      3.5%     3219   18.59      6.0%    99.7*   -22    0.793     102
     2.34        6611    3398      5068       67.0%      15.2%     14.8%     5095    6.09     19.2%    97.7*   -18    0.659     115
     2.03        7183    3885      5989       64.9%      46.6%     51.2%     5234    1.97     58.8%    85.1*   -20    0.594      51
     1.82        8374    4643      6813       68.1%     163.8%    186.3%     6044    0.54    208.3%    26.3*    15    0.612      19
     1.66        8975    5190      7448       69.7%     611.0%    698.1%     6297    0.16    786.3%     4.9      0    0.000       0
     1.53        6606    4568      8120       56.3%    1702.1%   2012.1%     3641    0.05   2277.5%     4.4      0    0.000       0
     1.44        3682    3333      8733       38.2%    1564.2%   1735.0%      677    0.01   2180.9%    -3.2      0    0.000       0
     1.35        2424    2368      9253       25.6%     -99.9%    -99.9%      110    0.00    -99.9%     0.5      0    0.000       0
    total       50784   30982     57627       53.8%       6.1%      6.3%    32573    3.85      7.9%    99.9*   -20    0.705     384


 NUMBER OF REFLECTIONS IN SELECTED SUBSET OF IMAGES     51943
 NUMBER OF REJECTED MISFITS                              1157
 NUMBER OF SYSTEMATIC ABSENT REFLECTIONS                    0
 NUMBER OF ACCEPTED OBSERVATIONS                        50786
 NUMBER OF UNIQUE ACCEPTED REFLECTIONS                  30984



 ******************************************************************************
  SUMMARY OF DATA SET STATISTICS FOR IMAGE   DATA_RANGE=       1     445
 ******************************************************************************


          COMPLETENESS AND QUALITY OF DATA SET
          ------------------------------------

 R-FACTOR
 observed = (SUM(ABS(I(h,i)-I(h))))/(SUM(I(h,i)))
 expected = expected R-FACTOR derived from Sigma(I)

 COMPARED = number of reflections used for calculating R-FACTOR
 I/SIGMA  = mean of intensity/Sigma(I) of unique reflections
            (after merging symmetry-related observations)
 Sigma(I) = standard deviation of reflection intensity I
            estimated from sample statistics

 R-meas   = redundancy independent R-factor (intensities)
            Diederichs & Karplus (1997), Nature Struct. Biol. 4, 269-275.

 CC(1/2)  = percentage of correlation between intensities from
            random half-datasets. Correlation significant at
            the 0.1% level is marked by an asterisk.
            Karplus & Diederichs (2012), Science 336, 1030-33
 Anomal   = percentage of correlation between random half-sets
  Corr      of anomalous intensity differences. Correlation
            significant at the 0.1% level is marked.
 SigAno   = mean anomalous difference in units of its estimated
            standard deviation (|F(+)-F(-)|/Sigma). F(+), F(-)
            are structure factor estimates obtained from the
            merged intensity observations in each parity class.
  Nano    = Number of unique reflections used to calculate
            Anomal_Corr & SigAno. At least two observations
            for each (+ and -) parity are required.


 SUBSET OF INTENSITY DATA WITH SIGNAL/NOISE >= -3.0 AS FUNCTION OF RESOLUTION
 RESOLUTION     NUMBER OF REFLECTIONS    COMPLETENESS R-FACTOR  R-FACTOR COMPARED I/SIGMA   R-meas  CC(1/2)  Anomal  SigAno   Nano
   LIMIT     OBSERVED  UNIQUE  POSSIBLE     OF DATA   observed  expected                                      Corr

     4.05        3633    1338      2243       59.7%       2.2%      2.3%     3396   39.54      2.6%    99.9*   -30    0.696     473
     2.87        5661    2387      3960       60.3%       4.7%      3.4%     5060   21.27      5.9%    99.6*   -20    0.883     557
     2.34        8800    3485      5068       68.8%      14.6%     14.3%     8141    7.13     18.1%    98.1*   -10    0.721    1002
     2.03        9631    4020      5989       67.1%      42.0%     45.8%     8747    2.30     52.6%    87.0*    -2    0.638    1000
     1.82       11152    4831      6813       70.9%     148.1%    167.3%    10025    0.63    186.8%    53.3*    -1    0.562    1022
     1.66       11546    5358      7448       71.9%     532.0%    607.3%     9991    0.19    675.3%     7.3      0    0.526     812
     1.53        8198    4738      8120       58.3%    1046.1%   1223.9%     6101    0.06   1392.3%     0.7      1    0.509     135
     1.44        4710    3520      8733       40.3%    8754.0%   9821.6%     2310    0.02  12233.5%     1.2    -32    0.315       2
     1.35        3316    2507      9253       27.1%    2975.5%   3649.4%     1596   -0.02   4187.6%     1.2      0    0.000       0
    total       66647   32184     57627       55.8%       6.7%      7.0%    55367    4.42      8.5%    99.9*    -6    0.650    5003


 NUMBER OF REFLECTIONS IN SELECTED SUBSET OF IMAGES     68125
 NUMBER OF REJECTED MISFITS                              1474
 NUMBER OF SYSTEMATIC ABSENT REFLECTIONS                    0
 NUMBER OF ACCEPTED OBSERVATIONS                        66651
 NUMBER OF UNIQUE ACCEPTED REFLECTIONS                  32186



 ******************************************************************************
  SUMMARY OF DATA SET STATISTICS FOR IMAGE   DATA_RANGE=       1     556
 ******************************************************************************


          COMPLETENESS AND QUALITY OF DATA SET
          ------------------------------------

 R-FACTOR
 observed = (SUM(ABS(I(h,i)-I(h))))/(SUM(I(h,i)))
 expected = expected R-FACTOR derived from Sigma(I)

 COMPARED = number of reflections used for calculating R-FACTOR
 I/SIGMA  = mean of intensity/Sigma(I) of unique reflections
            (after merging symmetry-related observations)
 Sigma(I) = standard deviation of reflection intensity I
            estimated from sample statistics

 R-meas   = redundancy independent R-factor (intensities)
            Diederichs & Karplus (1997), Nature Struct. Biol. 4, 269-275.

 CC(1/2)  = percentage of correlation between intensities from
            random half-datasets. Correlation significant at
            the 0.1% level is marked by an asterisk.
            Karplus & Diederichs (2012), Science 336, 1030-33
 Anomal   = percentage of correlation between random half-sets
  Corr      of anomalous intensity differences. Correlation
            significant at the 0.1% level is marked.
 SigAno   = mean anomalous difference in units of its estimated
            standard deviation (|F(+)-F(-)|/Sigma). F(+), F(-)
            are structure factor estimates obtained from the
            merged intensity observations in each parity class.
  Nano    = Number of unique reflections used to calculate
            Anomal_Corr & SigAno. At least two observations
            for each (+ and -) parity are required.


 SUBSET OF INTENSITY DATA WITH SIGNAL/NOISE >= -3.0 AS FUNCTION OF RESOLUTION
 RESOLUTION     NUMBER OF REFLECTIONS    COMPLETENESS R-FACTOR  R-FACTOR COMPARED I/SIGMA   R-meas  CC(1/2)  Anomal  SigAno   Nano
   LIMIT     OBSERVED  UNIQUE  POSSIBLE     OF DATA   observed  expected                                      Corr

     4.05        4629    1656      2243       73.8%       2.2%      2.4%     4323   40.50      2.7%    99.9*   -25    0.710     629
     2.87        7137    2903      3960       73.3%       4.4%      3.3%     6407   22.62      5.5%    99.7*   -14    0.909     734
     2.34       11001    4170      5068       82.3%      13.7%     13.3%    10128    8.09     16.8%    98.6*    -8    0.734    1364
     2.03       12073    4797      5989       80.1%      39.1%     42.4%    10947    2.74     48.0%    89.4*     0    0.656    1403
     1.82       14018    5764      6813       84.6%     132.9%    148.8%    12620    0.82    164.7%    56.2*    -1    0.572    1451
     1.66       14722    6422      7448       86.2%     445.9%    507.6%    12998    0.26    558.4%     9.0      0    0.532    1294
     1.53       11414    6081      8120       74.9%     948.3%   1108.8%     8961    0.09   1234.5%     1.4      2    0.517     409
     1.44        7800    5329      8733       61.0%    3065.4%   3426.6%     4611    0.03   4207.5%    -2.9     18    0.544      30
     1.35        6051    4301      9253       46.5%     -99.9%    -99.9%     3332    0.00    -99.9%    -0.4     -1    0.738       6
    total       88845   41423     57627       71.9%       7.0%      7.2%    74327    4.51      8.7%    99.9*    -4    0.654    7320


 NUMBER OF REFLECTIONS IN SELECTED SUBSET OF IMAGES     90620
 NUMBER OF REJECTED MISFITS                              1762
 NUMBER OF SYSTEMATIC ABSENT REFLECTIONS                    0
 NUMBER OF ACCEPTED OBSERVATIONS                        88858
 NUMBER OF UNIQUE ACCEPTED REFLECTIONS                  41430



 ******************************************************************************
  SUMMARY OF DATA SET STATISTICS FOR IMAGE   DATA_RANGE=       1     667
 ******************************************************************************


          COMPLETENESS AND QUALITY OF DATA SET
          ------------------------------------

 R-FACTOR
 observed = (SUM(ABS(I(h,i)-I(h))))/(SUM(I(h,i)))
 expected = expected R-FACTOR derived from Sigma(I)

 COMPARED = number of reflections used for calculating R-FACTOR
 I/SIGMA  = mean of intensity/Sigma(I) of unique reflections
            (after merging symmetry-related observations)
 Sigma(I) = standard deviation of reflection intensity I
            estimated from sample statistics

 R-meas   = redundancy independent R-factor (intensities)
            Diederichs & Karplus (1997), Nature Struct. Biol. 4, 269-275.

 CC(1/2)  = percentage of correlation between intensities from
            random half-datasets. Correlation significant at
            the 0.1% level is marked by an asterisk.
            Karplus & Diederichs (2012), Science 336, 1030-33
 Anomal   = percentage of correlation between random half-sets
  Corr      of anomalous intensity differences. Correlation
            significant at the 0.1% level is marked.
 SigAno   = mean anomalous difference in units of its estimated
            standard deviation (|F(+)-F(-)|/Sigma). F(+), F(-)
            are structure factor estimates obtained from the
            merged intensity observations in each parity class.
  Nano    = Number of unique reflections used to calculate
            Anomal_Corr & SigAno. At least two observations
            for each (+ and -) parity are required.


 SUBSET OF INTENSITY DATA WITH SIGNAL/NOISE >= -3.0 AS FUNCTION OF RESOLUTION
 RESOLUTION     NUMBER OF REFLECTIONS    COMPLETENESS R-FACTOR  R-FACTOR COMPARED I/SIGMA   R-meas  CC(1/2)  Anomal  SigAno   Nano
   LIMIT     OBSERVED  UNIQUE  POSSIBLE     OF DATA   observed  expected                                      Corr

     4.05        5598    2121      2243       94.6%       2.2%      2.3%     5290   39.78      2.7%    99.9*   -25    0.710     629
     2.87        8555    3522      3960       88.9%       4.1%      3.1%     7770   23.68      5.2%    99.7*   -14    0.909     734
     2.34       13155    4849      5068       95.7%      11.9%     11.1%    12391    9.28     14.7%    99.0*    -8    0.734    1364
     2.03       14514    5415      5989       90.4%      32.3%     34.2%    13610    3.38     39.7%    92.5*     0    0.656    1403
     1.82       16807    6369      6813       93.5%     100.3%    110.9%    15733    1.09    123.6%    68.5*    -1    0.572    1451
     1.66       17744    7016      7448       94.2%     295.8%    336.3%    16437    0.35    366.8%    19.0*    -1    0.532    1294
     1.53       13955    6841      8120       84.2%     771.2%    890.1%    11486    0.11    983.7%     3.6      2    0.517     409
     1.44       10281    6597      8733       75.5%    1420.8%   1607.4%     6451    0.05   1887.4%     6.2     18    0.544      30
     1.35        8409    5793      9253       62.6%   16375.8%  19290.4%     4632    0.00  21865.2%    -3.1     -1    0.738       6
    total      109018   48523     57627       84.2%       6.8%      7.0%    93800    4.98      8.5%    99.9*    -4    0.654    7320


 NUMBER OF REFLECTIONS IN SELECTED SUBSET OF IMAGES    111238
 NUMBER OF REJECTED MISFITS                              2201
 NUMBER OF SYSTEMATIC ABSENT REFLECTIONS                    0
 NUMBER OF ACCEPTED OBSERVATIONS                       109037
 NUMBER OF UNIQUE ACCEPTED REFLECTIONS                  48532



 ******************************************************************************
  SUMMARY OF DATA SET STATISTICS FOR IMAGE   DATA_RANGE=       1     778
 ******************************************************************************


          COMPLETENESS AND QUALITY OF DATA SET
          ------------------------------------

 R-FACTOR
 observed = (SUM(ABS(I(h,i)-I(h))))/(SUM(I(h,i)))
 expected = expected R-FACTOR derived from Sigma(I)

 COMPARED = number of reflections used for calculating R-FACTOR
 I/SIGMA  = mean of intensity/Sigma(I) of unique reflections
            (after merging symmetry-related observations)
 Sigma(I) = standard deviation of reflection intensity I
            estimated from sample statistics

 R-meas   = redundancy independent R-factor (intensities)
            Diederichs & Karplus (1997), Nature Struct. Biol. 4, 269-275.

 CC(1/2)  = percentage of correlation between intensities from
            random half-datasets. Correlation significant at
            the 0.1% level is marked by an asterisk.
            Karplus & Diederichs (2012), Science 336, 1030-33
 Anomal   = percentage of correlation between random half-sets
  Corr      of anomalous intensity differences. Correlation
            significant at the 0.1% level is marked.
 SigAno   = mean anomalous difference in units of its estimated
            standard deviation (|F(+)-F(-)|/Sigma). F(+), F(-)
            are structure factor estimates obtained from the
            merged intensity observations in each parity class.
  Nano    = Number of unique reflections used to calculate
            Anomal_Corr & SigAno. At least two observations
            for each (+ and -) parity are required.


 SUBSET OF INTENSITY DATA WITH SIGNAL/NOISE >= -3.0 AS FUNCTION OF RESOLUTION
 RESOLUTION     NUMBER OF REFLECTIONS    COMPLETENESS R-FACTOR  R-FACTOR COMPARED I/SIGMA   R-meas  CC(1/2)  Anomal  SigAno   Nano
   LIMIT     OBSERVED  UNIQUE  POSSIBLE     OF DATA   observed  expected                                      Corr

     4.05        6544    2148      2243       95.8%       2.1%      2.3%     6481   43.37      2.5%    99.9*   -17    0.748     799
     2.87        9970    3615      3960       91.3%       3.9%      3.0%     9631   25.97      4.8%    99.8*   -14    0.912     803
     2.34       15333    4906      5068       96.8%      10.3%      9.6%    15180   10.51     12.5%    99.2*    -8    0.749    1455
     2.03       16937    5486      5989       91.6%      28.5%     29.9%    16601    3.86     34.3%    94.6*    -1    0.655    1451
     1.82       19624    6436      6813       94.5%      85.5%     94.8%    19199    1.24    103.3%    74.7*    -1    0.572    1461
     1.66       20509    7035      7448       94.5%     251.7%    286.4%    19867    0.40    305.9%    24.3*    -1    0.532    1294
     1.53       15968    6892      8120       84.9%     733.9%    844.6%    13885    0.13    909.8%     4.3      2    0.516     410
     1.44       12052    6703      8733       76.8%    1782.1%   2041.3%     8595    0.05   2263.1%     6.9     18    0.544      30
     1.35       10131    5973      9253       64.6%     -99.9%    -99.9%     6681    0.00    -99.9%    -1.4     15    0.738       6
    total      127068   49194     57627       85.4%       6.5%      6.8%   116120    5.52      8.0%    99.9*    -4    0.665    7709


 NUMBER OF REFLECTIONS IN SELECTED SUBSET OF IMAGES    129750
 NUMBER OF REJECTED MISFITS                              2656
 NUMBER OF SYSTEMATIC ABSENT REFLECTIONS                    0
 NUMBER OF ACCEPTED OBSERVATIONS                       127094
 NUMBER OF UNIQUE ACCEPTED REFLECTIONS                  49204



 ******************************************************************************
  SUMMARY OF DATA SET STATISTICS FOR IMAGE   DATA_RANGE=       1     889
 ******************************************************************************


          COMPLETENESS AND QUALITY OF DATA SET
          ------------------------------------

 R-FACTOR
 observed = (SUM(ABS(I(h,i)-I(h))))/(SUM(I(h,i)))
 expected = expected R-FACTOR derived from Sigma(I)

 COMPARED = number of reflections used for calculating R-FACTOR
 I/SIGMA  = mean of intensity/Sigma(I) of unique reflections
            (after merging symmetry-related observations)
 Sigma(I) = standard deviation of reflection intensity I
            estimated from sample statistics

 R-meas   = redundancy independent R-factor (intensities)
            Diederichs & Karplus (1997), Nature Struct. Biol. 4, 269-275.

 CC(1/2)  = percentage of correlation between intensities from
            random half-datasets. Correlation significant at
            the 0.1% level is marked by an asterisk.
            Karplus & Diederichs (2012), Science 336, 1030-33
 Anomal   = percentage of correlation between random half-sets
  Corr      of anomalous intensity differences. Correlation
            significant at the 0.1% level is marked.
 SigAno   = mean anomalous difference in units of its estimated
            standard deviation (|F(+)-F(-)|/Sigma). F(+), F(-)
            are structure factor estimates obtained from the
            merged intensity observations in each parity class.
  Nano    = Number of unique reflections used to calculate
            Anomal_Corr & SigAno. At least two observations
            for each (+ and -) parity are required.


 SUBSET OF INTENSITY DATA WITH SIGNAL/NOISE >= -3.0 AS FUNCTION OF RESOLUTION
 RESOLUTION     NUMBER OF REFLECTIONS    COMPLETENESS R-FACTOR  R-FACTOR COMPARED I/SIGMA   R-meas  CC(1/2)  Anomal  SigAno   Nano
   LIMIT     OBSERVED  UNIQUE  POSSIBLE     OF DATA   observed  expected                                      Corr

     4.05        7480    2149      2243       95.8%       2.1%      2.4%     7449   46.59      2.4%    99.9*   -11    0.765    1187
     2.87       11416    3635      3960       91.8%       3.8%      3.1%    11221   27.94      4.6%    99.8*   -16    0.884    1257
     2.34       17562    4910      5068       96.9%      10.2%      9.5%    17506   11.50     12.0%    99.4*    -9    0.763    2271
     2.03       19361    5508      5989       92.0%      27.6%     29.2%    19156    4.26     32.5%    95.6*    -3    0.700    2339
     1.82       22392    6445      6813       94.6%      81.8%     90.7%    22222    1.39     96.6%    78.0*    -1    0.622    2370
     1.66       23322    7052      7448       94.7%     231.5%    263.2%    23028    0.45    275.6%    27.1*    -1    0.579    1965
     1.53       17648    6932      8120       85.4%     583.6%    671.5%    16069    0.14    716.1%     6.7      2    0.560     555
     1.44       12854    6770      8733       77.5%    1499.3%   1717.3%     9717    0.05   1900.0%     6.2      8    0.501      44
     1.35       10611    6017      9253       65.0%   20357.5%  24056.7%     7466    0.00  25967.4%    -1.5     13    0.546       9
    total      142646   49418     57627       85.8%       6.5%      6.9%   133834    5.97      7.8%    99.9*    -5    0.695   11997


 NUMBER OF REFLECTIONS IN SELECTED SUBSET OF IMAGES    145645
 NUMBER OF REJECTED MISFITS                              2965
 NUMBER OF SYSTEMATIC ABSENT REFLECTIONS                    0
 NUMBER OF ACCEPTED OBSERVATIONS                       142680
 NUMBER OF UNIQUE ACCEPTED REFLECTIONS                  49430



 ******************************************************************************
  STATISTICS OF SAVED DATA SET "XDS_ASCII.HKL" (DATA_RANGE=       1    1000)
 FILE TYPE:         XDS_ASCII      MERGE=FALSE          FRIEDEL'S_LAW=TRUE 
 ******************************************************************************

   REFLECTIONS OF TYPE H,0,0  0,K,0  0,0,L OR EXPECTED TO BE ABSENT (*)
   --------------------------------------------------------------------

   H    K    L  RESOLUTION  INTENSITY     SIGMA    INTENSITY/SIGMA  #OBSERVED

    0    0    3    14.574   0.2132E+05  0.7384E+03        28.88           1 
    0    0    5     8.745   0.1261E+04  0.3112E+02        40.52           2 
    0    0    6     7.287   0.7905E+02  0.3356E+01        23.55           2 
    0    0    7     6.246   0.6316E+04  0.1550E+03        40.74           2 
    0    0    8     5.465   0.1520E+04  0.3769E+02        40.33           2 
    0    0    9     4.858   0.1084E+04  0.2707E+02        40.03           2 
    0    0   10     4.372   0.1857E+04  0.4613E+02        40.25           2 
    0    0   11     3.975   0.8284E+04  0.2048E+03        40.45           2 
    0    4    0    13.432   0.7752E+04  0.1902E+03        40.77           2 
    0    6    0     8.955   0.1036E+05  0.2542E+03        40.74           2 
    0    8    0     6.716   0.6767E+03  0.2087E+02        32.42           2 
    0   10    0     5.373   0.9999E+04  0.2465E+03        40.57           2 
    0   12    0     4.477   0.2860E+04  0.7380E+02        38.75           2 
    0   14    0     3.838   0.1996E+05  0.4907E+03        40.68           2 
    0   16    0     3.358   0.2590E+04  0.6965E+02        37.18           2 
    0   18    0     2.985   0.1079E+04  0.3542E+02        30.48           2 
    0   20    0     2.686   0.5245E+04  0.1335E+03        39.29           2 
    0   22    0     2.442   0.6483E+03  0.2733E+02        23.72           2 
    0   26    0     2.067   0.3889E+03  0.3420E+02        11.37           1 
    0   28    0     1.919   0.2299E+03  0.2994E+02         7.68           1 
    0   30    0     1.791   0.4899E+02  0.2534E+02         1.93           1 
    0   32    0     1.679   0.3439E+01  0.2566E+02         0.13           1 
    0   36    0     1.492   0.3670E+02  0.2698E+02         1.36           1 
    6    0    0    18.584   0.2042E+04  0.5035E+02        40.56           2 
    8    0    0    13.938   0.1999E+04  0.4951E+02        40.38           2 
   10    0    0    11.150   0.2212E+05  0.5409E+03        40.90           2 
   12    0    0     9.292   0.1489E+04  0.3766E+02        39.54           2 
   14    0    0     7.965   0.1037E+04  0.2717E+02        38.17           2 
   16    0    0     6.969   0.9816E+03  0.2636E+02        37.23           2 
   18    0    0     6.195   0.3505E+03  0.1315E+02        26.65           2 
   20    0    0     5.575   0.7412E+03  0.2209E+02        33.55           2 
   24    0    0     4.646   0.4471E+04  0.1120E+03        39.93           2 
   32    0    0     3.484   0.1665E+03  0.2195E+02         7.59           2 
   34    0    0     3.280   0.1061E+02  0.2042E+02         0.52           2 
   36    0    0     3.097   0.1753E+02  0.1924E+02         0.91           2 
   38    0    0     2.934   0.2464E+03  0.2087E+02        11.81           2 
   40    0    0     2.788   0.1257E+03  0.1841E+02         6.83           2 
   42    0    0     2.655   0.3411E+02  0.1899E+02         1.80           2 
   44    0    0     2.534  -0.1284E+01  0.1975E+02        -0.07           2 
   46    0    0     2.424   0.3358E+02  0.3245E+02         1.03           1 
   48    0    0     2.323  -0.1808E+02  0.3454E+02        -0.52           1 
   50    0    0     2.230   0.1530E+02  0.3710E+02         0.41           1 
   54    0    0     2.065   0.5091E+02  0.3618E+02         1.41           1 
   56    0    0     1.991   0.6612E+02  0.3466E+02         1.91           1 
   58    0    0     1.922   0.3042E+02  0.2049E+02         1.49           2 
   60    0    0     1.858   0.2084E+01  0.1834E+02         0.11           2 
   62    0    0     1.798   0.2295E+02  0.2195E+02         1.05           1 
   64    0    0     1.742   0.1028E+02  0.2238E+02         0.46           1 
   66    0    0     1.689  -0.1186E+02  0.2250E+02        -0.53           1 
   68    0    0     1.640   0.7944E+01  0.2357E+02         0.34           1 

          COMPLETENESS AND QUALITY OF DATA SET
          ------------------------------------

 R-FACTOR
 observed = (SUM(ABS(I(h,i)-I(h))))/(SUM(I(h,i)))
 expected = expected R-FACTOR derived from Sigma(I)

 COMPARED = number of reflections used for calculating R-FACTOR
 I/SIGMA  = mean of intensity/Sigma(I) of unique reflections
            (after merging symmetry-related observations)
 Sigma(I) = standard deviation of reflection intensity I
            estimated from sample statistics

 R-meas   = redundancy independent R-factor (intensities)
            Diederichs & Karplus (1997), Nature Struct. Biol. 4, 269-275.

 CC(1/2)  = percentage of correlation between intensities from
            random half-datasets. Correlation significant at
            the 0.1% level is marked by an asterisk.
            Karplus & Diederichs (2012), Science 336, 1030-33
 Anomal   = percentage of correlation between random half-sets
  Corr      of anomalous intensity differences. Correlation
            significant at the 0.1% level is marked.
 SigAno   = mean anomalous difference in units of its estimated
            standard deviation (|F(+)-F(-)|/Sigma). F(+), F(-)
            are structure factor estimates obtained from the
            merged intensity observations in each parity class.
  Nano    = Number of unique reflections used to calculate
            Anomal_Corr & SigAno. At least two observations
            for each (+ and -) parity are required.


 SUBSET OF INTENSITY DATA WITH SIGNAL/NOISE >= -3.0 AS FUNCTION OF RESOLUTION
 RESOLUTION     NUMBER OF REFLECTIONS    COMPLETENESS R-FACTOR  R-FACTOR COMPARED I/SIGMA   R-meas  CC(1/2)  Anomal  SigAno   Nano
   LIMIT     OBSERVED  UNIQUE  POSSIBLE     OF DATA   observed  expected                                      Corr

     4.05        8329    2151      2243       95.9%       2.2%      2.5%     8306   48.98      2.5%    99.9*    -5    0.860    1480
     2.87       12817    3639      3960       91.9%       3.9%      3.1%    12691   29.47      4.6%    99.8*   -14    0.920    1786
     2.34       19692    4912      5068       96.9%      10.4%      9.8%    19653   12.09     12.0%    99.5*    -7    0.776    3249
     2.03       21682    5517      5989       92.1%      28.6%     30.4%    21506    4.47     33.0%    96.0*    -2    0.719    3377
     1.82       25005    6449      6813       94.7%      85.9%     95.4%    24882    1.45     99.4%    79.7*    -2    0.630    3475
     1.66       25518    7057      7448       94.8%     243.1%    276.2%    25252    0.47    284.5%    26.5*     0    0.579    2646
     1.53       18927    6947      8120       85.6%     599.6%    692.3%    17471    0.14    725.6%     7.3      6    0.546     864
     1.44       13345    6813      8733       78.0%    1519.1%   1742.9%    10348    0.05   1917.4%     6.3      6    0.441     111
     1.35       10644    6026      9253       65.1%   18558.6%  21927.0%     7494    0.00  23657.3%    -0.8      9    0.551      23
    total      155959   49511     57627       85.9%       6.7%      7.0%   147603    6.27      7.8%    99.9*    -4    0.712   17011


 NUMBER OF REFLECTIONS IN SELECTED SUBSET OF IMAGES    159281
 NUMBER OF REJECTED MISFITS                              3279
 NUMBER OF SYSTEMATIC ABSENT REFLECTIONS                    0
 NUMBER OF ACCEPTED OBSERVATIONS                       156002
 NUMBER OF UNIQUE ACCEPTED REFLECTIONS                  49524



 ******************************************************************************
    WILSON STATISTICS OF DATA SET  "XDS_ASCII.HKL"
 ******************************************************************************

 Data is divided into resolution shells and a straight line 
 A - 2*B*SS is fitted to log<I>, where
   RES    = mean resolution (Angstrom) in shell
   SS     = mean of (sin(THETA)/LAMBDA)**2 in shell
   <I>    = mean reflection intensity in shell
   BO     = (A - log<I>)/(2*SS)
    #     = number of reflections in resolution shell

   WILSON LINE (using all data) : A=   7.809 B=  38.534 CORRELATION=  0.99
      #      RES      SS        <I>       log(<I>)       BO
     721     7.581   0.004  2.0999E+03       7.650      18.3
    1272     4.762   0.011  1.6290E+03       7.396      18.7
    1469     3.701   0.018  1.1057E+03       7.008      21.9
    1870     3.141   0.025  4.1425E+02       6.026      35.2
    2159     2.773   0.033  1.6050E+02       5.078      42.0
    2354     2.511   0.040  7.8701E+01       4.366      43.4
    2602     2.311   0.047  4.8011E+01       3.871      42.1
    2531     2.154   0.054  3.0261E+01       3.410      40.8
    2858     2.022   0.061  1.7242E+01       2.847      40.6
    3065     1.913   0.068  1.0775E+01       2.377      39.7
    3201     1.819   0.076  5.2220E+00       1.653      40.7
    3331     1.739   0.083  3.1311E+00       1.141      40.3
    3458     1.668   0.090  1.8274E+00       0.603      40.1
    3431     1.606   0.097  1.4753E+00       0.389      38.3
    3036     1.549   0.104  7.8377E-01      -0.244      38.6
    3295     1.498   0.111  6.8700E-01      -0.375      36.7
    3151     1.452   0.119  4.5029E-01      -0.798      36.3
    3051     1.410   0.126 -3.7175E-03    -999.999   -1000.0
    2657     1.373   0.133  1.0525E-01      -2.251      37.9


 HIGHER ORDER MOMENTS OF WILSON DISTRIBUTION OF  CENTRIC DATA
    AS COMPARED WITH THEORETICAL VALUES. (EXPECTED: 1.00)    
      #      RES        <I**2>/      <I**3>/      <I**4>/  
                         3<I>**2     15<I>**3    105<I>**4 

      84     7.581        2.227        3.109        3.678
      81     4.762        0.851        0.681        0.490
      71     3.701        0.435        0.258        0.127
      72     3.141        0.491        0.484        0.417
      90     2.773        0.430        0.414        0.351
      90     2.511        0.204        0.102        0.049
      93     2.311        0.355        0.227        0.133
      82     2.154        1.252        3.593        8.268
      83     2.022        0.181        0.051        0.019
      88     1.913        0.655        0.644        0.705
      84     1.819        0.687        0.048        0.111
      86     1.739        2.790        1.602        4.057
      77     1.668        5.271        2.395        8.490
      34     1.549       37.174       62.829      355.438
      26     1.452      158.920      305.288     6117.480
      16     1.373     3631.729    34887.117  2232497.128
    1264   overall       52.632      450.570    28406.078


 HIGHER ORDER MOMENTS OF WILSON DISTRIBUTION OF ACENTRIC DATA
    AS COMPARED WITH THEORETICAL VALUES. (EXPECTED: 1.00)    
      #      RES        <I**2>/      <I**3>/      <I**4>/  
                         2<I>**2      6<I>**3     24<I>**4 

     637     7.581        1.154        1.820        3.191
    1191     4.762        1.035        1.081        1.157
    1398     3.701        1.267        1.690        2.221
    1798     3.141        1.372        2.091        3.246
    2069     2.773        1.471        2.610        4.938
    2264     2.511        1.556        3.111        7.254
    2509     2.311        1.480        2.975        7.799
    2449     2.154        1.472        2.380        3.993
    2775     2.022        1.735        3.329        6.810
    2977     1.913        2.354       10.863       87.584
    3117     1.819        2.615        5.498       15.697
    3245     1.739        3.683        6.024       14.816
    3381     1.668        9.334       11.544       72.280
    3375     1.606       16.368       18.460      198.835
    3002     1.549       65.953       50.964     2802.360
    3265     1.498      108.362      164.044     8342.822
    3125     1.452      307.498     3102.709   281480.418
    3030     1.410       56.972      -14.012     2107.612
    2641     1.373     5452.609     1875.770 20088955.739
   48248   overall      332.695      322.342  1118628.147

   ======= CUMULATIVE INTENSITY DISTRIBUTION =======
 DEFINITIONS:
   <I>    = mean reflection intensity
 Na(Z)exp = expected number of acentric reflections with I <= Z*<I>
 Na(Z)obs = observed number of acentric reflections with I <= Z*<I>
 Nc(Z)exp = expected number of  centric reflections with I <= Z*<I>
 Nc(Z)obs = observed number of  centric reflections with I <= Z*<I>



 Nc(Z)obs/Nc(Z)exp versus resolution and Z (0.1-1.0)
      #      RES     0.1   0.2   0.3   0.4   0.5   0.6   0.7   0.8   0.9   1.0

      84     7.581  0.77  0.83  0.91  0.93  1.05  1.00  0.98  0.97  0.96  0.94
      81     4.762  1.14  1.07  1.01  0.94  0.92  0.90  0.89  0.88  0.86  0.92
      71     3.701  1.19  1.47  1.35  1.34  1.27  1.28  1.25  1.19  1.16  1.16
      72     3.141  1.23  1.37  1.47  1.47  1.49  1.41  1.40  1.37  1.31  1.26
      90     2.773  1.34  1.41  1.49  1.43  1.37  1.35  1.36  1.34  1.29  1.25
      90     2.511  1.43  1.32  1.31  1.31  1.30  1.37  1.32  1.36  1.34  1.34
      93     2.311  1.13  1.03  1.16  1.20  1.16  1.19  1.22  1.21  1.24  1.25
      82     2.154  1.23  1.06  1.17  1.29  1.26  1.28  1.33  1.30  1.28  1.23
      83     2.022  1.50  1.46  1.39  1.37  1.34  1.33  1.35  1.34  1.30  1.25
      88     1.913  1.78  1.45  1.25  1.22  1.20  1.15  1.12  1.12  1.11  1.08
      84     1.819  1.87  1.45  1.20  1.08  1.07  1.08  1.12  1.08  1.07  1.08
      86     1.739  1.92  1.38  1.17  1.06  0.96  0.93  0.90  0.87  0.87  0.87
      77     1.668  1.83  1.31  1.12  1.04  1.00  0.93  0.91  0.87  0.83  0.80
      34     1.549  1.66  1.28  1.13  1.06  0.96  0.89  0.84  0.80  0.76  0.73
      26     1.452  1.86  1.33  1.11  0.98  0.89  0.82  0.77  0.73  0.70  0.68
      16     1.373  1.76  1.27  1.05  0.92  0.84  0.78  0.73  0.70  0.67  0.64
    1264   overall  1.32  1.17  1.12  1.09  1.07  1.05  1.04  1.03  1.01  0.99


 Na(Z)obs/Na(Z)exp versus resolution and Z (0.1-1.0)
      #      RES     0.1   0.2   0.3   0.4   0.5   0.6   0.7   0.8   0.9   1.0

     637     7.581  1.25  1.17  1.17  1.11  1.13  1.13  1.13  1.10  1.08  1.09
    1191     4.762  1.01  1.12  1.02  1.07  1.05  1.07  1.04  1.01  0.99  0.99
    1398     3.701  1.14  1.16  1.22  1.21  1.16  1.13  1.09  1.08  1.08  1.06
    1798     3.141  1.34  1.35  1.28  1.24  1.20  1.17  1.14  1.12  1.09  1.08
    2069     2.773  1.48  1.40  1.31  1.24  1.21  1.15  1.12  1.11  1.08  1.07
    2264     2.511  1.44  1.34  1.26  1.23  1.21  1.18  1.17  1.14  1.11  1.10
    2509     2.311  1.43  1.22  1.20  1.17  1.17  1.17  1.15  1.12  1.09  1.07
    2449     2.154  1.89  1.42  1.30  1.25  1.21  1.16  1.14  1.12  1.09  1.06
    2775     2.022  2.31  1.56  1.32  1.22  1.15  1.12  1.11  1.09  1.08  1.05
    2977     1.913  2.77  1.72  1.40  1.22  1.14  1.10  1.07  1.05  1.04  1.03
    3117     1.819  3.51  2.00  1.51  1.28  1.14  1.06  1.01  0.97  0.95  0.93
    3245     1.739  3.92  2.16  1.58  1.30  1.13  1.03  0.96  0.92  0.88  0.86
    3381     1.668  4.39  2.37  1.70  1.37  1.18  1.05  0.96  0.89  0.84  0.81
    3375     1.606  4.51  2.41  1.71  1.37  1.16  1.04  0.95  0.89  0.83  0.79
    3002     1.549  4.82  2.56  1.80  1.43  1.21  1.06  0.96  0.88  0.82  0.78
    3265     1.498  5.04  2.66  1.87  1.48  1.25  1.09  0.98  0.91  0.85  0.80
    3125     1.452  5.11  2.70  1.89  1.49  1.25  1.10  0.99  0.90  0.84  0.79
    3030     1.410  5.31  2.79  1.95  1.53  1.29  1.12  1.00  0.92  0.85  0.80
    2641     1.373  5.23  2.75  1.93  1.51  1.27  1.11  1.00  0.91  0.85  0.80
   48248   overall  3.44  2.03  1.56  1.33  1.19  1.10  1.04  0.99  0.95  0.92


 List of    500 reflections *NOT* obeying Wilson distribution (Z> 10.0)

   h    k    l     RES      Z     Intensity    Sigma

   51   13   17    1.4314248.61 -0.1059E+03  0.3609E+02 "alien"
   59   23    4    1.4311676.40 -0.8681E+02  0.3709E+02 "alien"
   52   28    4    1.3911638.16 -0.8653E+02  0.3609E+02 "alien"
   67   21   -2    1.4110610.89 -0.7889E+02  0.3372E+02 "alien"
   54   26    6    1.3910468.21 -0.7783E+02  0.3636E+02 "alien"
   51   27    5    1.4210394.19 -0.7728E+02  0.3562E+02 "alien"
    8   32  -18    1.39 9830.21 -0.7309E+02  0.2634E+02 "alien"
   58   26    1    1.40 9213.33 -0.6850E+02  0.3501E+02 "alien"
   53   17   15    1.40 8973.25 -0.6672E+02  0.3660E+02 "alien"
   44   20   17    1.40 8800.36 -0.6543E+02  0.3573E+02 "alien"
   65    9   11    1.43 8754.83 -0.6509E+02  0.2923E+02 "alien"
   58   20   10    1.40 8661.24 -0.6440E+02  0.3751E+02 "alien"
   12   38    1    1.40 8659.21 -0.6438E+02  0.2493E+02 "alien"
   57   17   12    1.42 8540.30 -0.6350E+02  0.3574E+02 "alien"
   17   23   23    1.39 8451.46 -0.6284E+02  0.2770E+02 "alien"
   47    3   21    1.42 8425.09 -0.6264E+02  0.2724E+02 "alien"
   46   22   15    1.40 8387.86 -0.6236E+02  0.3744E+02 "alien"
   54   10   17    1.42 8280.93 -0.6157E+02  0.2889E+02 "alien"
   36    4   25    1.40 8192.70 -0.6091E+02  0.3338E+02 "alien"
   41    9   22    1.43 8182.39 -0.6084E+02  0.3383E+02 "alien"
   55   15  -24    1.40 8016.99 -0.5961E+02  0.2737E+02 "alien"
   43   23   15    1.41 8007.12 -0.5953E+02  0.3671E+02 "alien"
   44   14   20    1.40 8002.81 -0.5950E+02  0.3644E+02 "alien"
   25   31  -18    1.39 7998.61 -0.5947E+02  0.3481E+02 "alien"
   64   24   -5    1.39 7986.38 -0.5938E+02  0.3287E+02 "alien"
   32    6   25    1.43 7948.23 -0.5909E+02  0.3013E+02 "alien"
   61   25   -9    1.40 7931.82 -0.5897E+02  0.3074E+02 "alien"
   57   25    2    1.43 7907.41 -0.5879E+02  0.3657E+02 "alien"
   56   16  -22    1.43 7903.63 -0.5876E+02  0.2665E+02 "alien"
   55   27   -8    1.43 7833.10 -0.5824E+02  0.3123E+02 "alien"
   62   24   -1    1.41 7796.64 -0.5797E+02  0.3520E+02 "alien"
   57   23  -15    1.43 7770.64 -0.5777E+02  0.2794E+02 "alien"
   10   34   13    1.40 7744.43 -0.5758E+02  0.2496E+02 "alien"
   48   26  -17    1.41 7728.06 -0.5746E+02  0.3298E+02 "alien"
   41   19   19    1.39 7598.26 -0.5649E+02  0.2733E+02 "alien"
   50   16  -25    1.40 7436.10 -0.5529E+02  0.2525E+02 "alien"
   60   24  -11    1.42 7394.21 -0.5498E+02  0.3020E+02 "alien"
   60    6   15    1.42 7329.19 -0.5449E+02  0.2806E+02 "alien"
   46   16  -26    1.40 7269.56 -0.5405E+02  0.2828E+02 "alien"
   21    9   27    1.42 7172.38 -0.5333E+02  0.2923E+02 "alien"
   51   11   18    1.42 7019.68 -0.5219E+02  0.3831E+02 "alien"
   65   23   -4    1.40 6995.48 -0.5201E+02  0.3240E+02 "alien"
   63   13  -22    1.39 6919.86 -0.5145E+02  0.2757E+02 "alien"
   53   19  -22    1.41 6815.59 -0.5067E+02  0.2097E+02 "alien"
   28   20   22    1.40 6773.28 -0.5036E+02  0.2514E+02 "alien"
   49   15   18    1.39 6718.94 -0.4996E+02  0.3626E+02 "alien"
   38    4   24    1.41 6702.80 -0.4984E+02  0.2084E+02 "alien"
   41   13   21    1.42 6672.11 -0.4961E+02  0.3487E+02 "alien"
   56   20  -19    1.43 6596.89 -0.4905E+02  0.2765E+02 "alien"
   65    7  -22    1.43 6580.58 -0.4893E+02  0.1989E+02 "alien"
   62   22  -13    1.42 6546.75 -0.4867E+02  0.3035E+02 "alien"
   35   23   18    1.41 6490.68 -0.4826E+02  0.2682E+02 "alien"
   51   15   16    1.43 6388.54 -0.4750E+02  0.3550E+02 "alien"
   48   14   18    1.42 6374.28 -0.4739E+02  0.3561E+02 "alien"
   16   34   12    1.40 6343.75 -0.4717E+02  0.2753E+02 "alien"
   62    8   14    1.40 6301.45 -0.4685E+02  0.2896E+02 "alien"
   52   18   15    1.40 6290.75 -0.4677E+02  0.3803E+02 "alien"
   63   23    2    1.39 6231.82 -0.4633E+02  0.3719E+02 "alien"
   44   22   16    1.39 6225.64 -0.4629E+02  0.3689E+02 "alien"
   67   13  -19    1.41 6178.95 -0.4594E+02  0.2874E+02 "alien"
   24   16   25    1.40 6144.10 -0.4568E+02  0.2373E+02 "alien"
   50   20   14    1.41 6099.02 -0.4535E+02  0.3840E+02 "alien"
   57   11  -25    1.40 6081.96 -0.4522E+02  0.2689E+02 "alien"
   50   12   18    1.42 5936.24 -0.4414E+02  0.3796E+02 "alien"
   39   27   13    1.41 5934.99 -0.4413E+02  0.2508E+02 "alien"
   45    3   22    1.41 5920.83 -0.4402E+02  0.2689E+02 "alien"
   60   20    7    1.43 5904.43 -0.4390E+02  0.3630E+02 "alien"
   58   12  -24    1.41 5870.84 -0.4365E+02  0.2724E+02 "alien"
   63   11   12    1.41 5853.98 -0.4352E+02  0.2884E+02 "alien"
   57   23    7    1.41 5848.72 -0.4349E+02  0.3730E+02 "alien"
   51   29  -10    1.40 5838.75 -0.4341E+02  0.1952E+02 "alien"
   10   38    3    1.39 5833.25 -0.4337E+02  0.2461E+02 "alien"
   17   35  -12    1.41 5833.17 -0.4337E+02  0.2630E+02 "alien"
   54   24    7    1.43 5800.04 -0.4312E+02  0.3719E+02 "alien"
   35   25   17    1.39 5770.75 -0.4291E+02  0.3545E+02 "alien"
   46   12   20    1.40 5699.88 -0.4238E+02  0.3511E+02 "alien"
   35   29  -18    1.41 5695.13 -0.4234E+02  0.3249E+02 "alien"
   76   12   -7    1.42 5688.14 -0.4229E+02  0.2509E+02 "alien"
   48   24   12    1.40 5612.95 -0.4173E+02  0.3700E+02 "alien"
   63   17  -18    1.42 5604.73 -0.4167E+02  0.2839E+02 "alien"
   57   23  -17    1.39 5602.62 -0.4166E+02  0.2848E+02 "alien"
   73   15  -12    1.40 5554.05 -0.4129E+02  0.2409E+02 "alien"
   16   30   17    1.41 5530.68 -0.4112E+02  0.2122E+02 "alien"
   48    4   21    1.41 5496.48 -0.4087E+02  0.2771E+02 "alien"
   52   18   14    1.42 5478.19 -0.4073E+02  0.3736E+02 "alien"
   51   17   15    1.43 5447.37 -0.4050E+02  0.3708E+02 "alien"
   16   36  -10    1.40 5441.89 -0.4046E+02  0.2827E+02 "alien"
   51   27  -14    1.41 5437.12 -0.4042E+02  0.2164E+02 "alien"
   67   21  -10    1.40 5424.46 -0.4033E+02  0.3102E+02 "alien"
   56   14   14    1.43 5406.04 -0.4019E+02  0.3816E+02 "alien"
   39   17   20    1.42 5398.31 -0.4014E+02  0.3476E+02 "alien"
   48   26  -16    1.43 5377.61 -0.3998E+02  0.3298E+02 "alien"
    8   24  -25    1.39 5360.98 -0.3986E+02  0.1907E+02 "alien"
   56   28   -6    1.39 5359.34 -0.3985E+02  0.3244E+02 "alien"
   61    1   16    1.39 5351.31 -0.3979E+02  0.2736E+02 "alien"
   64   16  -19    1.41 5344.20 -0.3973E+02  0.2707E+02 "alien"
   56   24  -15    1.42 5331.38 -0.3964E+02  0.2830E+02 "alien"
   54   20  -21    1.40 5330.09 -0.3963E+02  0.2143E+02 "alien"
   28   22  -25    1.42 5302.50 -0.3942E+02  0.2846E+02 "alien"
   39   33   -2    1.42 5297.33 -0.3939E+02  0.1595E+02 "alien"
   73   15  -11    1.41 5296.57 -0.3938E+02  0.2411E+02 "alien"
   57   27    0    1.40 5208.93 -0.3873E+02  0.3518E+02 "alien"
   66   10   11    1.40 5201.91 -0.3868E+02  0.2966E+02 "alien"
   11   29   19    1.40 5195.57 -0.3863E+02  0.2073E+02 "alien"
    6   34  -15    1.40 5195.33 -0.3863E+02  0.2821E+02 "alien"
   42   18   19    1.40 5187.43 -0.3857E+02  0.3529E+02 "alien"
   28   30   13    1.42 5177.46 -0.3849E+02  0.2593E+02 "alien"
   51   23   11    1.41 5162.87 -0.3839E+02  0.3734E+02 "alien"
   19   35    8    1.41 5161.29 -0.3837E+02  0.2476E+02 "alien"
   20   30  -19    1.41 5150.82 -0.3830E+02  0.2676E+02 "alien"
   49   23  -20    1.41 5145.81 -0.3826E+02  0.2011E+02 "alien"
   63   23   -9    1.42 5102.88 -0.3794E+02  0.3081E+02 "alien"
   45   27    9    1.43 5095.40 -0.3788E+02  0.2630E+02 "alien"
   36    6   24    1.43 5087.45 -0.3783E+02  0.3197E+02 "alien"
   62   20  -17    1.40 5080.79 -0.3778E+02  0.2808E+02 "alien"
   60   24  -10    1.43 5074.07 -0.3773E+02  0.3102E+02 "alien"
   30   30  -18    1.40 5048.06 -0.3753E+02  0.3426E+02 "alien"
   52   20  -21    1.42 5039.32 -0.3747E+02  0.1993E+02 "alien"
   53    7   18    1.42 5025.12 -0.3736E+02  0.2817E+02 "alien"
   67   11  -21    1.39 5004.75 -0.3721E+02  0.3081E+02 "alien"
   66   20  -12    1.42 4989.07 -0.3709E+02  0.2986E+02 "alien"
    7   23  -25    1.42 4983.07 -0.3705E+02  0.2630E+02 "alien"
   28   34  -10    1.43 4966.97 -0.3693E+02  0.2780E+02 "alien"
   50   12  -26    1.42 4954.42 -0.3684E+02  0.2431E+02 "alien"
   26   32  -16    1.40 4930.03 -0.3665E+02  0.2582E+02 "alien"
    8   38    4    1.39 4917.71 -0.3656E+02  0.2565E+02 "alien"
   22   32  -17    1.40 4912.44 -0.3652E+02  0.2583E+02 "alien"
   64    8   12    1.43 4883.14 -0.3631E+02  0.2882E+02 "alien"
   11   25  -24    1.41 4880.95 -0.3629E+02  0.1862E+02 "alien"
   33   31  -15    1.42 4875.91 -0.3625E+02  0.2473E+02 "alien"
   22   36   -5    1.43 4875.78 -0.3625E+02  0.2553E+02 "alien"
   20   30   16    1.41 4866.42 -0.3618E+02  0.2692E+02 "alien"
   54   20  -20    1.43 4841.65 -0.3600E+02  0.2219E+02 "alien"
   16   36    8    1.40 4838.97 -0.3598E+02  0.2357E+02 "alien"
   60   24    1    1.42 4821.15 -0.3585E+02  0.3471E+02 "alien"
   55   21  -20    1.40 4820.24 -0.3584E+02  0.2204E+02 "alien"
    8   36    9    1.41 4808.24 -0.3575E+02  0.2357E+02 "alien"
   35   27   15    1.40 4793.50 -0.3564E+02  0.2379E+02 "alien"
   67   21   -7    1.41 4786.40 -0.3559E+02  0.3003E+02 "alien"
   22   20   23    1.42 4784.96 -0.3558E+02  0.2467E+02 "alien"
   43   19  -25    1.40 4782.55 -0.3556E+02  0.2336E+02 "alien"
   29   35   -5    1.42 4769.91 -0.3546E+02  0.2333E+02 "alien"
   10   14   28    1.40 4731.07 -0.3518E+02  0.2730E+02 "alien"
   72   12  -15    1.42 4716.78 -0.3507E+02  0.2384E+02 "alien"
   56   20   11    1.41 4696.14 -0.3492E+02  0.3745E+02 "alien"
   61   21  -15    1.42 4689.03 -0.3486E+02  0.2843E+02 "alien"
   25   29  -20    1.40 4679.71 -0.3479E+02  0.2116E+02 "alien"
   54   24  -16    1.42 4679.29 -0.3479E+02  0.2213E+02 "alien"
   66    6   12    1.41 4676.18 -0.3477E+02  0.2815E+02 "alien"
   49   17   17    1.39 4674.64 -0.3476E+02  0.3732E+02 "alien"
   29   35   -6    1.42 4673.21 -0.3475E+02  0.2366E+02 "alien"
   65    5   12    1.43 4630.74 -0.3443E+02  0.2776E+02 "alien"
   56    4   18    1.40 4620.51 -0.3435E+02  0.2754E+02 "alien"
   48   10   20    1.40 4614.20 -0.3431E+02  0.3728E+02 "alien"
   60   14  -22    1.41 4581.99 -0.3407E+02  0.2623E+02 "alien"
   54    4   19    1.39 4580.84 -0.3406E+02  0.2788E+02 "alien"
   36   24  -23    1.40 4577.67 -0.3403E+02  0.2028E+02 "alien"
    7   27  -22    1.42 4565.49 -0.3394E+02  0.1878E+02 "alien"
   28   36   -4    1.40 4562.61 -0.3392E+02  0.2786E+02 "alien"
    6   30   19    1.40 4559.04 -0.3390E+02  0.2692E+02 "alien"
   60    6  -25    1.41 4553.35 -0.3385E+02  0.3066E+02 "alien"
   62   10   13    1.41 4553.26 -0.3385E+02  0.2957E+02 "alien"
   49   13   18    1.42 4545.84 -0.3380E+02  0.3556E+02 "alien"
   27   25   19    1.40 4540.76 -0.3376E+02  0.1984E+02 "alien"
   43   27   11    1.41 4531.54 -0.3369E+02  0.2756E+02 "alien"
   67   17  -17    1.39 4531.26 -0.3369E+02  0.2779E+02 "alien"
   53   23   10    1.41 4486.09 -0.3335E+02  0.3740E+02 "alien"
    1   37   -8    1.40 4485.39 -0.3335E+02  0.2513E+02 "alien"
   32   32  -15    1.39 4483.56 -0.3334E+02  0.2151E+02 "alien"
   53   29   -2    1.40 4468.04 -0.3322E+02  0.2492E+02 "alien"
   29   31  -16    1.42 4456.25 -0.3313E+02  0.2598E+02 "alien"
   15   37    2    1.42 4450.57 -0.3309E+02  0.2399E+02 "alien"
   44   10   21    1.42 4442.37 -0.3303E+02  0.3473E+02 "alien"
   63   15  -21    1.39 4442.01 -0.3303E+02  0.2782E+02 "alien"
   62   22    5    1.39 4439.47 -0.3301E+02  0.3606E+02 "alien"
   26   14   25    1.41 4433.38 -0.3296E+02  0.2317E+02 "alien"
   59    9  -25    1.40 4383.39 -0.3259E+02  0.2702E+02 "alien"
   52   12   17    1.42 4379.77 -0.3256E+02  0.3723E+02 "alien"
   45   27  -17    1.41 4377.71 -0.3255E+02  0.2271E+02 "alien"
   55   25    5    1.42 4367.27 -0.3247E+02  0.3594E+02 "alien"
   52    6  -27    1.42 4354.68 -0.3238E+02  0.2472E+02 "alien"
   66   20  -14    1.40 4350.23 -0.3234E+02  0.2135E+02 "alien"
   18   34   10    1.42 4330.53 -0.3220E+02  0.2728E+02 "alien"
   29   31   11    1.43 4328.06 -0.3218E+02  0.1706E+02 "alien"
   18   36   -9    1.41 4312.78 -0.3207E+02  0.2775E+02 "alien"
   59   17  -20    1.43 4292.18 -0.3191E+02  0.2104E+02 "alien"
   32   10   25    1.40 4274.26 -0.3178E+02  0.2418E+02 "alien"
   19   37   -3    1.41 4240.48 -0.3153E+02  0.2612E+02 "alien"
   12   12   28    1.41 4236.92 -0.3150E+02  0.1633E+02 "alien"
   26   34  -11    1.42 4214.06 -0.3133E+02  0.2694E+02 "alien"
    5   13   29    1.40 4200.19 -0.3123E+02  0.2151E+02 "alien"
   33   27   15    1.41 4199.42 -0.3122E+02  0.3544E+02 "alien"
   22    8   27    1.43 4198.08 -0.3121E+02  0.2848E+02 "alien"
   23   29  -20    1.41 4189.39 -0.3115E+02  0.2095E+02 "alien"
   39   21  -24    1.42 4185.83 -0.3112E+02  0.2308E+02 "alien"
    5   29   19    1.43 4185.39 -0.3112E+02  0.2652E+02 "alien"
   61   25   -5    1.41 4185.26 -0.3112E+02  0.3179E+02 "alien"
    6   34   14    1.40 4168.81 -0.3099E+02  0.2444E+02 "alien"
   74   16   -6    1.40 4162.70 -0.3095E+02  0.2479E+02 "alien"
   17   37    4    1.40 4158.44 -0.3092E+02  0.2361E+02 "alien"
   52   16  -24    1.41 4150.14 -0.3086E+02  0.2087E+02 "alien"
   23   37   -3    1.39 4150.07 -0.3086E+02  0.2598E+02 "alien"
    9   37    5    1.42 4149.46 -0.3085E+02  0.2442E+02 "alien"
   59   25  -12    1.40 4145.23 -0.3082E+02  0.3113E+02 "alien"
   58   24    5    1.40 4133.64 -0.3073E+02  0.3710E+02 "alien"
   63   23  -12    1.39 4119.80 -0.3063E+02  0.3121E+02 "alien"
   66    6  -23    1.40 4119.69 -0.3063E+02  0.2332E+02 "alien"
   37   27   14    1.40 4110.43 -0.3056E+02  0.2508E+02 "alien"
   75   15   -8    1.40 4103.64 -0.3051E+02  0.2449E+02 "alien"
   37   33    5    1.39 4092.75 -0.3043E+02  0.1636E+02 "alien"
   63   13   12    1.39 4076.09 -0.3031E+02  0.2922E+02 "alien"
   24   36    3    1.41 4056.59 -0.3016E+02  0.2521E+02 "alien"
   66   18  -15    1.42 4032.53 -0.2998E+02  0.2155E+02 "alien"
   35   31  -15    1.41 4029.96 -0.2996E+02  0.1794E+02 "alien"
   58    2  -26    1.42 4012.82 -0.2984E+02  0.1847E+02 "alien"
   70   14  -17    1.40 4012.44 -0.2983E+02  0.3144E+02 "alien"
   60   26   -6    1.40 3996.75 -0.2972E+02  0.3362E+02 "alien"
   34   10   24    1.42 3996.70 -0.2972E+02  0.3204E+02 "alien"
   19   31  -18    1.41 3994.28 -0.2970E+02  0.2673E+02 "alien"
   53   27  -13    1.40 3988.68 -0.2966E+02  0.2217E+02 "alien"
   42   22   16    1.41 3971.87 -0.2953E+02  0.2763E+02 "alien"
   31   17  -28    1.39 3966.93 -0.2949E+02  0.2094E+02 "alien"
   60   24  -12    1.41 3950.49 -0.2937E+02  0.3102E+02 "alien"
    1   27   22    1.40 3941.68 -0.2931E+02  0.1950E+02 "alien"
    3   35   13    1.39 3936.77 -0.2927E+02  0.2479E+02 "alien"
   13   29  -20    1.43 3934.76 -0.2925E+02  0.1872E+02 "alien"
   56   26  -13    1.40 3929.88 -0.2922E+02  0.2350E+02 "alien"
   35   33    3    1.43 3929.12 -0.2921E+02  0.1636E+02 "alien"
   57   19  -21    1.39 3926.69 -0.2919E+02  0.2855E+02 "alien"
   47   21  -23    1.39 3893.33 -0.2895E+02  0.1936E+02 "alien"
   11   37    7    1.39 3891.48 -0.2893E+02  0.2228E+02 "alien"
   10   32  -17    1.41 3883.04 -0.2887E+02  0.2591E+02 "alien"
   56   22  -17    1.43 3880.88 -0.2885E+02  0.2990E+02 "alien"
   49   29    3    1.41 3878.64 -0.2884E+02  0.1971E+02 "alien"
   26   20  -26    1.43 3862.73 -0.2872E+02  0.1653E+02 "alien"
   44   16   19    1.41 3861.73 -0.2871E+02  0.3625E+02 "alien"
   55   27   -7    1.43 3852.91 -0.2865E+02  0.3101E+02 "alien"
   64   16  -18    1.43 3847.53 -0.2861E+02  0.2112E+02 "alien"
   31   29  -18    1.43 3846.39 -0.2860E+02  0.2099E+02 "alien"
   18   30   16    1.42 3840.92 -0.2856E+02  0.2736E+02 "alien"
   60   10  -24    1.41 3836.80 -0.2853E+02  0.2761E+02 "alien"
   72   16   -4    1.43 3834.46 -0.2851E+02  0.2536E+02 "alien"
   69    7  -21    1.40 3834.45 -0.2851E+02  0.2337E+02 "alien"
   11   35   10    1.42 3824.85 -0.2844E+02  0.2441E+02 "alien"
   34   28   14    1.40 3821.55 -0.2841E+02  0.2522E+02 "alien"
   56   26    1    1.43 3819.65 -0.2840E+02  0.3566E+02 "alien"
   23   31   14    1.41 3816.57 -0.2838E+02  0.3105E+02 "alien"
   48    8  -28    1.41 3814.56 -0.2836E+02  0.1699E+02 "alien"
   13   11   28    1.42 3812.10 -0.2834E+02  0.1615E+02 "alien"
   44   32   -5    1.41 3809.60 -0.2832E+02  0.1556E+02 "alien"
   65   21    0    1.42 3795.22 -0.2822E+02  0.3410E+02 "alien"
   23    9   27    1.41 3792.85 -0.2820E+02  0.1821E+02 "alien"
   50   14   18    1.39 3789.97 -0.2818E+02  0.3604E+02 "alien"
   11   23   23    1.43 3786.60 -0.2815E+02  0.2808E+02 "alien"
   64    8   13    1.40 3784.43 -0.2814E+02  0.2932E+02 "alien"
   11   23   24    1.39 3780.72 -0.2811E+02  0.2756E+02 "alien"
   35    3   25    1.41 3757.62 -0.2794E+02  0.2090E+02 "alien"
   40   24   15    1.42 3737.75 -0.2779E+02  0.3573E+02 "alien"
   37   25  -21    1.43 3733.28 -0.2776E+02  0.1362E+02 "alien"
   60   26   -7    1.39 3729.68 -0.2773E+02  0.3161E+02 "alien"
   22   36    1    1.43 3727.17 -0.2771E+02  0.2398E+02 "alien"
   18    8   28    1.41 3718.35 -0.2765E+02  0.1766E+02 "alien"
   53   27    5    1.40 3714.37 -0.2762E+02  0.3538E+02 "alien"
   50   26  -16    1.41 3703.92 -0.2754E+02  0.2756E+02 "alien"
   12   20  -27    1.41 3700.34 -0.2751E+02  0.1446E+02 "alien"
   59   21    7    1.42 3698.08 -0.2750E+02  0.3741E+02 "alien"
   16   26   21    1.40 3696.55 -0.2748E+02  0.1990E+02 "alien"
    9   27  -22    1.42 3696.03 -0.2748E+02  0.1863E+02 "alien"
   20   36    4    1.42 3692.71 -0.2746E+02  0.2352E+02 "alien"
   53   27    2    1.43 3690.94 -0.2744E+02  0.3579E+02 "alien"
    1   23  -25    1.40 3683.40 -0.2739E+02  0.2739E+02 "alien"
   18    6   28    1.43 3671.35 -0.2730E+02  0.1491E+02 "alien"
    5   21   25    1.43 3669.47 -0.2728E+02  0.1829E+02 "alien"
   34   24   18    1.40 3636.51 -0.2704E+02  0.2705E+02 "alien"
   70   16    4    1.39 3634.20 -0.2702E+02  0.2833E+02 "alien"
   36   10   24    1.40 3623.30 -0.2694E+02  0.3372E+02 "alien"
    9   37    4    1.43 3617.20 -0.2689E+02  0.2478E+02 "alien"
   64   20  -16    1.39 3616.51 -0.2689E+02  0.2165E+02 "alien"
   48   30    2    1.40 3612.43 -0.2686E+02  0.1996E+02 "alien"
   15   31  -18    1.42 3609.10 -0.2683E+02  0.2701E+02 "alien"
   61   21    6    1.41 3591.74 -0.2670E+02  0.3730E+02 "alien"
   62   12  -23    1.39 3569.40 -0.2654E+02  0.2712E+02 "alien"
   40   28   11    1.41 3568.13 -0.2653E+02  0.2053E+02 "alien"
   20   14  -29    1.43 3562.37 -0.2649E+02  0.1723E+02 "alien"
   18   34  -14    1.40 3554.87 -0.2643E+02  0.2666E+02 "alien"
   61   23    4    1.40 3550.54 -0.2640E+02  0.3643E+02 "alien"
   23   35    8    1.39 3546.44 -0.2637E+02  0.2117E+02 "alien"
   42   32   -6    1.42 3533.12 -0.2627E+02  0.1566E+02 "alien"
   57   17  -22    1.40 3532.00 -0.2626E+02  0.2707E+02 "alien"
   28   24  -24    1.40 3513.99 -0.2613E+02  0.2401E+02 "alien"
   46    4   22    1.39 3509.80 -0.2610E+02  0.2766E+02 "alien"
   45    1   22    1.41 3504.12 -0.2605E+02  0.2703E+02 "alien"
   54   12  -25    1.42 3498.75 -0.2601E+02  0.2681E+02 "alien"
   31   29   14    1.40 3497.01 -0.2600E+02  0.2571E+02 "alien"
   55    7  -26    1.42 3493.35 -0.2597E+02  0.2513E+02 "alien"
   50   18  -24    1.40 3488.15 -0.2593E+02  0.1953E+02 "alien"
    5   19   26    1.43 3488.07 -0.2593E+02  0.1782E+02 "alien"
   48   28    7    1.40 3486.63 -0.2592E+02  0.2579E+02 "alien"
   16   36   -9    1.41 3477.52 -0.2586E+02  0.2857E+02 "alien"
   68   20   -8    1.42 3476.53 -0.2585E+02  0.3079E+02 "alien"
   66   20  -11    1.43 3472.75 -0.2582E+02  0.2979E+02 "alien"
    7   37   -9    1.39 3466.43 -0.2577E+02  0.1595E+02 "alien"
   33   35    1    1.39 3460.10 -0.2573E+02  0.1683E+02 "alien"
   65   13  -20    1.42 3454.02 -0.2568E+02  0.2799E+02 "alien"
   20   18  -28    1.40 3442.98 -0.2560E+02  0.2454E+02 "alien"
   23   31   13    1.43 3435.26 -0.2554E+02  0.3171E+02 "alien"
   63   19  -17    1.41 3429.06 -0.2549E+02  0.2285E+02 "alien"
   54   18  -22    1.41 3425.95 -0.2547E+02  0.2637E+02 "alien"
   60    4   16    1.40 3419.05 -0.2542E+02  0.2723E+02 "alien"
   68   12  -19    1.41 3404.19 -0.2531E+02  0.3262E+02 "alien"
   61   11  -23    1.41 3396.08 -0.2525E+02  0.2779E+02 "alien"
   17   37   -3    1.42 3387.40 -0.2519E+02  0.2590E+02 "alien"
   27   21   22    1.39 3384.18 -0.2516E+02  0.2588E+02 "alien"
   34   34   -1    1.43 3382.48 -0.2515E+02  0.1982E+02 "alien"
   68   20  -12    1.39 3381.22 -0.2514E+02  0.3071E+02 "alien"
   59   13   13    1.43 3380.70 -0.2514E+02  0.2911E+02 "alien"
   19   31   15    1.41 3380.28 -0.2513E+02  0.3218E+02 "alien"
    2   34  -15    1.39 3379.90 -0.2513E+02  0.2821E+02 "alien"
   66   22   -2    1.40 3379.12 -0.2512E+02  0.3523E+02 "alien"
   40   26   13    1.42 3375.35 -0.2510E+02  0.4103E+02 "alien"
   60   12  -23    1.41 3374.73 -0.2509E+02  0.2712E+02 "alien"
   46   26  -18    1.41 3373.09 -0.2508E+02  0.2108E+02 "alien"
   45   27   10    1.41 3358.50 -0.2497E+02  0.2672E+02 "alien"
   64    6  -24    1.39 3353.65 -0.2493E+02  0.1989E+02 "alien"
   27   23   20    1.42 3332.27 -0.2478E+02  0.2593E+02 "alien"
   47   15  -26    1.41 3330.55 -0.2476E+02  0.2828E+02 "alien"
   36   34   -8    1.39 3307.58 -0.2459E+02  0.1592E+02 "alien"
   45   27  -18    1.39 3306.76 -0.2459E+02  0.2305E+02 "alien"
   55   11  -25    1.42 3304.86 -0.2457E+02  0.2728E+02 "alien"
   54   26    5    1.41 3300.45 -0.2454E+02  0.3529E+02 "alien"
   37   25   15    1.43 3299.54 -0.2453E+02  0.3586E+02 "alien"
   70   16  -15    1.40 3299.01 -0.2453E+02  0.2841E+02 "alien"
   14   16   26    1.43 3280.90 -0.2439E+02  0.2098E+02 "alien"
   22   34  -13    1.41 3277.42 -0.2437E+02  0.1795E+02 "alien"
    5   37    7    1.41 3273.02 -0.2433E+02  0.2453E+02 "alien"
   53   23    9    1.43 3269.80 -0.2431E+02  0.3688E+02 "alien"
   58   26   -1    1.41 3265.01 -0.2428E+02  0.3495E+02 "alien"
   33    9   25    1.40 3258.96 -0.2423E+02  0.3114E+02 "alien"
   24   34  -12    1.42 3256.78 -0.2421E+02  0.2561E+02 "alien"
   62   24  -10    1.40 3250.64 -0.2417E+02  0.3182E+02 "alien"
   64   24   -4    1.39 3250.19 -0.2417E+02  0.3258E+02 "alien"
   54   22  -19    1.41 3248.45 -0.2415E+02  0.2181E+02 "alien"
   41   17   20    1.40 3240.59 -0.2409E+02  0.3527E+02 "alien"
   46   24   13    1.40 3227.81 -0.2400E+02  0.2883E+02 "alien"
   13   29  -21    1.40 3225.72 -0.2398E+02  0.1933E+02 "alien"
   50   22  -20    1.42 3212.24 -0.2388E+02  0.2056E+02 "alien"
   61   23  -11    1.43 3206.43 -0.2384E+02  0.3019E+02 "alien"
    6   34  -14    1.42 3204.68 -0.2383E+02  0.2838E+02 "alien"
   61   13   13    1.40 3202.30 -0.2381E+02  0.2996E+02 "alien"
   35    5   25    1.40 3201.31 -0.2380E+02  0.2016E+02 "alien"
   47   11   20    1.40 3188.32 -0.2371E+02  0.3599E+02 "alien"
   43   21   17    1.39 3186.67 -0.2369E+02  0.2775E+02 "alien"
   64   20    3    1.43 3184.21 -0.2367E+02  0.3528E+02 "alien"
   51   29  -11    1.39 3183.51 -0.2367E+02  0.1936E+02 "alien"
   37   29  -17    1.41 3169.73 -0.2357E+02  0.2263E+02 "alien"
   49   21  -21    1.43 3156.67 -0.2347E+02  0.1993E+02 "alien"
   67   15    6    1.43 3137.83 -0.2333E+02  0.2921E+02 "alien"
   24   22  -25    1.43 3135.53 -0.2331E+02  0.1876E+02 "alien"
   55   23    9    1.40 3129.05 -0.2326E+02  0.3633E+02 "alien"
    2   38   -1    1.41 3128.92 -0.2326E+02  0.2675E+02 "alien"
   65   11  -21    1.42 3122.40 -0.2321E+02  0.2942E+02 "alien"
   24   36   -4    1.42 3119.64 -0.2319E+02  0.2544E+02 "alien"
   25   25   19    1.42 3116.62 -0.2317E+02  0.1977E+02 "alien"
   53   29    0    1.39 3098.29 -0.2304E+02  0.2614E+02 "alien"
   30   14  -29    1.40 3088.08 -0.2296E+02  0.1670E+02 "alien"
   15   37   -4    1.42 3083.73 -0.2293E+02  0.2685E+02 "alien"
   36   24  -22    1.43 3078.68 -0.2289E+02  0.1937E+02 "alien"
   39   29  -17    1.40 3078.15 -0.2289E+02  0.1431E+02 "alien"
   47   29  -13    1.41 3070.53 -0.2283E+02  0.1491E+02 "alien"
   25   35  -10    1.41 3064.08 -0.2278E+02  0.2665E+02 "alien"
   40   30    8    1.41 3063.61 -0.2278E+02  0.1657E+02 "alien"
   62    4  -25    1.40 3060.06 -0.2275E+02  0.1979E+02 "alien"
   30   18  -27    1.42 3058.89 -0.2274E+02  0.1642E+02 "alien"
   36   26   15    1.41 3058.87 -0.2274E+02  0.3591E+02 "alien"
   20   32   13    1.42 3058.44 -0.2274E+02  0.3282E+02 "alien"
   26   30  -18    1.42 3056.43 -0.2272E+02  0.2178E+02 "alien"
   14   30  -20    1.40 3055.15 -0.2271E+02  0.1897E+02 "alien"
   50    2  -28    1.42 3051.27 -0.2269E+02  0.1152E+02 "alien"
   44   28    8    1.43 3047.09 -0.2266E+02  0.2045E+02 "alien"
   26   36   -5    1.41 3044.44 -0.2264E+02  0.2604E+02 "alien"
   56   18  -21    1.42 3040.11 -0.2260E+02  0.1776E+02 "alien"
   34   14   23    1.41 3036.41 -0.2258E+02  0.3921E+02 "alien"
   57   19  -20    1.41 3032.53 -0.2255E+02  0.2809E+02 "alien"
   12   34  -14    1.41 3029.70 -0.2253E+02  0.2765E+02 "alien"
   68   20  -10    1.41 3029.64 -0.2253E+02  0.2395E+02 "alien"
   68   10  -20    1.41 3027.35 -0.2251E+02  0.2321E+02 "alien"
   55   13  -25    1.40 3023.33 -0.2248E+02  0.2687E+02 "alien"
   36   32    7    1.41 3019.91 -0.2245E+02  0.1628E+02 "alien"
   21   35    6    1.43 3015.27 -0.2242E+02  0.2490E+02 "alien"
   38   22   17    1.43 3014.71 -0.2241E+02  0.2649E+02 "alien"
   38   10  -29    1.42 3013.83 -0.2241E+02  0.1859E+02 "alien"
   53    3  -27    1.43 3012.95 -0.2240E+02  0.2508E+02 "alien"
   64    2   14    1.40 3011.27 -0.2239E+02  0.2794E+02 "alien"
   15   37    0    1.43 3008.30 -0.2237E+02  0.2434E+02 "alien"
   23   25  -24    1.39 3005.43 -0.2235E+02  0.2572E+02 "alien"
   13   37   -8    1.40 3004.45 -0.2234E+02  0.2795E+02 "alien"
    0   38    3    1.41 3002.96 -0.2233E+02  0.2619E+02 "alien"
   56    8  -26    1.41 3002.57 -0.2232E+02  0.2577E+02 "alien"
   28   36   -1    1.40 3000.59 -0.2231E+02  0.3739E+02 "alien"
    4   22   25    1.41 2997.84 -0.2229E+02  0.2577E+02 "alien"
   30    8   26    1.40 2996.18 -0.2228E+02  0.2240E+02 "alien"
    3   31  -18    1.42 2995.62 -0.2227E+02  0.1561E+02 "alien"
   29   35    1    1.42 2995.31 -0.2227E+02  0.1782E+02 "alien"
   54   16   14    1.43 2994.31 -0.2226E+02  0.3588E+02 "alien"
   40   28  -17    1.42 2989.52 -0.2223E+02  0.2206E+02 "alien"
   26   24   20    1.40 2985.06 -0.2219E+02  0.2011E+02 "alien"
   77    7  -13    1.42 2977.14 -0.2213E+02  0.2326E+02 "alien"
   52   20   13    1.41 2976.85 -0.2213E+02  0.2559E+02 "alien"
    0   18   27    1.42 2973.75 -0.2211E+02  0.1584E+02 "alien"
    8   38   -5    1.39 2972.49 -0.2210E+02  0.2808E+02 "alien"
   19   33  -16    1.39 2972.11 -0.2210E+02  0.2697E+02 "alien"
   35   33  -11    1.40 2968.73 -0.2207E+02  0.1636E+02 "alien"
   15   37   -6    1.41 2966.49 -0.2206E+02  0.2742E+02 "alien"
   52    6   19    1.41 2963.98 -0.2204E+02  0.2856E+02 "alien"
   31   33    8    1.40 2963.45 -0.2203E+02  0.1658E+02 "alien"
   20   36   -7    1.42 2960.45 -0.2201E+02  0.2663E+02 "alien"
   64   22  -13    1.39 2959.87 -0.2201E+02  0.3095E+02 "alien"
    9   19  -27    1.43 2958.61 -0.2200E+02  0.1222E+02 "alien"
   25   35   -8    1.43 2948.09 -0.2192E+02  0.2603E+02 "alien"
   52   26  -15    1.41 2942.47 -0.2188E+02  0.2189E+02 "alien"
   38   26   15    1.39 2937.95 -0.2184E+02  0.4114E+02 "alien"
   30   26   17    1.41 2928.73 -0.2178E+02  0.2020E+02 "alien"
   71    9  -19    1.40 2920.91 -0.2172E+02  0.2387E+02 "alien"
   12   28  -22    1.39 2913.94 -0.2167E+02  0.1924E+02 "alien"
   20   36   -9    1.40 2910.07 -0.2164E+02  0.2726E+02 "alien"
   40   32    4    1.41 2906.64 -0.2161E+02  0.1612E+02 "alien"
   62   18  -18    1.42 2901.86 -0.2158E+02  0.2115E+02 "alien"
   53   13   17    1.40 2901.01 -0.2157E+02  0.3769E+02 "alien"
   23   35  -10    1.41 2899.88 -0.2156E+02  0.2761E+02 "alien"
   13   33  -15    1.43 2896.27 -0.2153E+02  0.2652E+02 "alien"
   20   34  -14    1.40 2896.21 -0.2153E+02  0.1836E+02 "alien"
   44   32   -6    1.40 2889.55 -0.2148E+02  0.1541E+02 "alien"
   58   10  -25    1.40 2888.75 -0.2148E+02  0.2709E+02 "alien"
   34   32    7    1.42 2880.91 -0.2142E+02  0.1628E+02 "alien"
   33   11   24    1.42 2870.32 -0.2134E+02  0.3205E+02 "alien"
   57   25  -14    1.40 2846.31 -0.2116E+02  0.2373E+02 "alien"
   41   11  -28    1.43 2846.27 -0.2116E+02  0.1450E+02 "alien"
    1   37    7    1.41 2839.92 -0.2111E+02  0.2532E+02 "alien"
   46   22   14    1.42 2838.78 -0.2111E+02  0.3692E+02 "alien"
   45   23  -22    1.39 2832.82 -0.2106E+02  0.1945E+02 "alien"
    6   30  -19    1.42 2831.99 -0.2106E+02  0.1941E+02 "alien"
   25   35  -11    1.39 2828.41 -0.2103E+02  0.2656E+02 "alien"
   38   26   14    1.42 2825.36 -0.2101E+02  0.2737E+02 "alien"
   33   23  -24    1.41 2822.19 -0.2098E+02  0.2324E+02 "alien"
   24   30  -19    1.40 2819.40 -0.2096E+02  0.2215E+02 "alien"
   10   18  -28    1.40 2817.38 -0.2095E+02  0.1341E+02 "alien"
   32   34   -8    1.42 2812.69 -0.2091E+02  0.1768E+02 "alien"
   33   33  -12    1.40 2805.70 -0.2086E+02  0.2144E+02 "alien"
   22   28   17    1.42 2805.02 -0.2086E+02  0.2040E+02 "alien"
   27   19  -27    1.41 2802.52 -0.2084E+02  0.2296E+02 "alien"
   24   36   -2    1.42 2799.92 -0.2082E+02  0.2488E+02 "alien"
   57    5  -26    1.42 2791.19 -0.2075E+02  0.2690E+02 "alien"
   56   16  -23    1.40 2786.98 -0.2072E+02  0.2718E+02 "alien"
   29   33    8    1.42 2785.93 -0.2071E+02  0.1674E+02 "alien"
   14   38    0    1.39 2785.22 -0.2071E+02  0.2529E+02 "alien"
   62   18  -19    1.40 2780.42 -0.2067E+02  0.2127E+02 "alien"
   38   26  -21    1.40 2776.70 -0.2064E+02  0.1379E+02 "alien"
   53   17  -23    1.41 2772.68 -0.2061E+02  0.2139E+02 "alien"
   66   20  -13    1.41 2769.43 -0.2059E+02  0.1650E+02 "alien"
   31   19   22    1.40 2767.99 -0.2058E+02  0.2552E+02 "alien"
   58   10   15    1.42 2767.37 -0.2058E+02  0.2860E+02 "alien"
   36   30  -17    1.39 2766.61 -0.2057E+02  0.2068E+02 "alien"
   43   31    5    1.40 2761.00 -0.2053E+02  0.1614E+02 "alien"
   26   34  -12    1.41 2755.95 -0.2049E+02  0.2525E+02 "alien"
   62   20  -16    1.42 2754.56 -0.2048E+02  0.2828E+02 "alien"
   69   15  -15    1.43 2747.68 -0.2043E+02  0.3039E+02 "alien"
   23   35    6    1.42 2741.01 -0.2038E+02  0.2534E+02 "alien"
   29   33  -13    1.41 2737.27 -0.2035E+02  0.2208E+02 "alien"
   66    8   12    1.39 2735.52 -0.2034E+02  0.2917E+02 "alien"
   59   19  -19    1.41 2734.14 -0.2033E+02  0.2114E+02 "alien"
   50   30   -1    1.40 2728.75 -0.2029E+02  0.2552E+02 "alien"
   18   30   17    1.39 2724.32 -0.2026E+02  0.2736E+02 "alien"
   48   26  -18    1.39 2724.13 -0.2025E+02  0.3231E+02 "alien"
   55   27  -10    1.41 2723.10 -0.2025E+02  0.2446E+02 "alien"
    6   26  -23    1.41 2715.04 -0.2019E+02  0.1855E+02 "alien"
   21   23   22    1.40 2713.00 -0.2017E+02  0.2943E+02 "alien"
   35   31  -14    1.42 2712.70 -0.2017E+02  0.1818E+02 "alien"
   15   37   -2    1.43 2709.32 -0.2014E+02  0.2604E+02 "alien"
   39    1   24    1.41 2708.94 -0.2014E+02  0.1890E+02 "alien"
   27   23  -25    1.40 2707.03 -0.2013E+02  0.1857E+02 "alien"
   35   11   24    1.40 2702.33 -0.2009E+02  0.3286E+02 "alien"
   32   16   23    1.40 2700.98 -0.2008E+02  0.3845E+02 "alien"
    7   37    8    1.39 2699.41 -0.2007E+02  0.2416E+02 "alien"
   23   23  -25    1.41 2692.69 -0.2002E+02  0.1521E+02 "alien"
   27   21   21    1.43 2691.95 -0.2001E+02  0.2499E+02 "alien"
   17   37   -6    1.40 2682.29 -0.1994E+02  0.2699E+02 "alien"
   10   16   27    1.41 2678.10 -0.1991E+02  0.1804E+02 "alien"
   37   21   19    1.40 2677.84 -0.1991E+02  0.2630E+02 "alien"
   62   24   -4    1.42 2664.07 -0.1981E+02  0.3256E+02 "alien"
   44    6   22    1.41 2646.62 -0.1968E+02  0.2282E+02 "alien"
   53   29   -8    1.39 2642.42 -0.1965E+02  0.2343E+02 "alien"
   46   28    8    1.41 2637.73 -0.1961E+02  0.2706E+02 "alien"
   53   29   -1    1.40 2637.04 -0.1961E+02  0.3394E+02 "alien"
   60   26   -4    1.40 2632.61 -0.1957E+02  0.3348E+02 "alien"
   22   34    9    1.42 2625.90 -0.1952E+02  0.2118E+02 "alien"
   16   34  -14    1.41 2619.62 -0.1948E+02  0.2691E+02 "alien"
   54   28   -4    1.42 2617.07 -0.1946E+02  0.3201E+02 "alien"
   35   23  -24    1.40 2608.55 -0.1939E+02  0.2483E+02 "alien"
   51    9  -27    1.41 2607.06 -0.1938E+02  0.2488E+02 "alien"
   60   18   10    1.41 2602.77 -0.1935E+02  0.3585E+02 "alien"


 List of    500 reflections *NOT* obeying Wilson distribution (sorted by resolution)
 Ice rings could occur at (Angstrom):
 3.897,3.669,3.441, 2.671,2.249,2.072, 1.948,1.918,1.883,1.721

   h    k    l     RES      Z     Intensity    Sigma

   53   29    0    1.39 3098.29 -0.2304E+02  0.2614E+02
    8   32  -18    1.39 9830.21 -0.7309E+02  0.2634E+02
   68   20  -12    1.39 3381.22 -0.2514E+02  0.3071E+02
    3   35   13    1.39 3936.77 -0.2927E+02  0.2479E+02
   45   27  -18    1.39 3306.76 -0.2459E+02  0.2305E+02
   63   13   12    1.39 4076.09 -0.3031E+02  0.2922E+02
   51   29  -11    1.39 3183.51 -0.2367E+02  0.1936E+02
   44   22   16    1.39 6225.64 -0.4629E+02  0.3689E+02
   63   15  -21    1.39 4442.01 -0.3303E+02  0.2782E+02
   11   23   24    1.39 3780.72 -0.2811E+02  0.2756E+02
   67   17  -17    1.39 4531.26 -0.3369E+02  0.2779E+02
    8   38    4    1.39 4917.71 -0.3656E+02  0.2565E+02
    7   37    8    1.39 2699.41 -0.2007E+02  0.2416E+02
   57   19  -21    1.39 3926.69 -0.2919E+02  0.2855E+02
   35   25   17    1.39 5770.75 -0.4291E+02  0.3545E+02
   48   26  -18    1.39 2724.13 -0.2025E+02  0.3231E+02
   47   21  -23    1.39 3893.33 -0.2895E+02  0.1936E+02
   17   23   23    1.39 8451.46 -0.6284E+02  0.2770E+02
   11   37    7    1.39 3891.48 -0.2893E+02  0.2228E+02
   62   22    5    1.39 4439.47 -0.3301E+02  0.3606E+02
   37   33    5    1.39 4092.75 -0.3043E+02  0.1636E+02
   45   23  -22    1.39 2832.82 -0.2106E+02  0.1945E+02
   38   26   15    1.39 2937.95 -0.2184E+02  0.4114E+02
   14   38    0    1.39 2785.22 -0.2071E+02  0.2529E+02
    2   34  -15    1.39 3379.90 -0.2513E+02  0.2821E+02
   23   35    8    1.39 3546.44 -0.2637E+02  0.2117E+02
   64   24   -4    1.39 3250.19 -0.2417E+02  0.3258E+02
   62   12  -23    1.39 3569.40 -0.2654E+02  0.2712E+02
   64   20  -16    1.39 3616.51 -0.2689E+02  0.2165E+02
   61    1   16    1.39 5351.31 -0.3979E+02  0.2736E+02
   36   30  -17    1.39 2766.61 -0.2057E+02  0.2068E+02
   23   37   -3    1.39 4150.07 -0.3086E+02  0.2598E+02
   64   22  -13    1.39 2959.87 -0.2201E+02  0.3095E+02
   10   38    3    1.39 5833.25 -0.4337E+02  0.2461E+02
   54    4   19    1.39 4580.84 -0.3406E+02  0.2788E+02
   67   11  -21    1.39 5004.75 -0.3721E+02  0.3081E+02
    7   37   -9    1.39 3466.43 -0.2577E+02  0.1595E+02
   33   35    1    1.39 3460.10 -0.2573E+02  0.1683E+02
   63   23    2    1.39 6231.82 -0.4633E+02  0.3719E+02
   64   24   -5    1.39 7986.38 -0.5938E+02  0.3287E+02
    8   38   -5    1.39 2972.49 -0.2210E+02  0.2808E+02
   53   29   -8    1.39 2642.42 -0.1965E+02  0.2343E+02
   52   28    4    1.3911638.16 -0.8653E+02  0.3609E+02
   54   26    6    1.3910468.21 -0.7783E+02  0.3636E+02
    8   24  -25    1.39 5360.98 -0.3986E+02  0.1907E+02
   25   31  -18    1.39 7998.61 -0.5947E+02  0.3481E+02
   25   35  -11    1.39 2828.41 -0.2103E+02  0.2656E+02
   49   15   18    1.39 6718.94 -0.4996E+02  0.3626E+02
   18   30   17    1.39 2724.32 -0.2026E+02  0.2736E+02
   70   16    4    1.39 3634.20 -0.2702E+02  0.2833E+02
   41   19   19    1.39 7598.26 -0.5649E+02  0.2733E+02
   19   33  -16    1.39 2972.11 -0.2210E+02  0.2697E+02
   27   21   22    1.39 3384.18 -0.2516E+02  0.2588E+02
   66    8   12    1.39 2735.52 -0.2034E+02  0.2917E+02
   49   17   17    1.39 4674.64 -0.3476E+02  0.3732E+02
   31   17  -28    1.39 3966.93 -0.2949E+02  0.2094E+02
   32   32  -15    1.39 4483.56 -0.3334E+02  0.2151E+02
   56   28   -6    1.39 5359.34 -0.3985E+02  0.3244E+02
   63   13  -22    1.39 6919.86 -0.5145E+02  0.2757E+02
   57   23  -17    1.39 5602.62 -0.4166E+02  0.2848E+02
   50   14   18    1.39 3789.97 -0.2818E+02  0.3604E+02
   63   23  -12    1.39 4119.80 -0.3063E+02  0.3121E+02
   43   21   17    1.39 3186.67 -0.2369E+02  0.2775E+02
   12   28  -22    1.39 2913.94 -0.2167E+02  0.1924E+02
   46    4   22    1.39 3509.80 -0.2610E+02  0.2766E+02
   60   26   -7    1.39 3729.68 -0.2773E+02  0.3161E+02
   64    6  -24    1.39 3353.65 -0.2493E+02  0.1989E+02
   36   34   -8    1.39 3307.58 -0.2459E+02  0.1592E+02
   23   25  -24    1.39 3005.43 -0.2235E+02  0.2572E+02
   57   27    0    1.40 5208.93 -0.3873E+02  0.3518E+02
   38   26  -21    1.40 2776.70 -0.2064E+02  0.1379E+02
   53   27    5    1.40 3714.37 -0.2762E+02  0.3538E+02
   30    8   26    1.40 2996.18 -0.2228E+02  0.2240E+02
   16   36    8    1.40 4838.97 -0.3598E+02  0.2357E+02
   16   26   21    1.40 3696.55 -0.2748E+02  0.1990E+02
   66   20  -14    1.40 4350.23 -0.3234E+02  0.2135E+02
   50   18  -24    1.40 3488.15 -0.2593E+02  0.1953E+02
   46   22   15    1.40 8387.86 -0.6236E+02  0.3744E+02
   41   17   20    1.40 3240.59 -0.2409E+02  0.3527E+02
   56   26  -13    1.40 3929.88 -0.2922E+02  0.2350E+02
   34   24   18    1.40 3636.51 -0.2704E+02  0.2705E+02
    6   34  -15    1.40 5195.33 -0.3863E+02  0.2821E+02
   75   15   -8    1.40 4103.64 -0.3051E+02  0.2449E+02
   22   32  -17    1.40 4912.44 -0.3652E+02  0.2583E+02
   53   29   -1    1.40 2637.04 -0.1961E+02  0.3394E+02
   12   38    1    1.40 8659.21 -0.6438E+02  0.2493E+02
   14   30  -20    1.40 3055.15 -0.2271E+02  0.1897E+02
   57   25  -14    1.40 2846.31 -0.2116E+02  0.2373E+02
    6   30   19    1.40 4559.04 -0.3390E+02  0.2692E+02
   55   13  -25    1.40 3023.33 -0.2248E+02  0.2687E+02
   52   18   15    1.40 6290.75 -0.4677E+02  0.3803E+02
   70   14  -17    1.40 4012.44 -0.2983E+02  0.3144E+02
   61   13   13    1.40 3202.30 -0.2381E+02  0.2996E+02
   35   27   15    1.40 4793.50 -0.3564E+02  0.2379E+02
    6   34   14    1.40 4168.81 -0.3099E+02  0.2444E+02
   31   19   22    1.40 2767.99 -0.2058E+02  0.2552E+02
   36   10   24    1.40 3623.30 -0.2694E+02  0.3372E+02
   43   31    5    1.40 2761.00 -0.2053E+02  0.1614E+02
   55   21  -20    1.40 4820.24 -0.3584E+02  0.2204E+02
    5   13   29    1.40 4200.19 -0.3123E+02  0.2151E+02
   13   29  -21    1.40 3225.72 -0.2398E+02  0.1933E+02
   13   37   -8    1.40 3004.45 -0.2234E+02  0.2795E+02
   16   34   12    1.40 6343.75 -0.4717E+02  0.2753E+02
   36    4   25    1.40 8192.70 -0.6091E+02  0.3338E+02
   35   11   24    1.40 2702.33 -0.2009E+02  0.3286E+02
   24   16   25    1.40 6144.10 -0.4568E+02  0.2373E+02
   35   23  -24    1.40 2608.55 -0.1939E+02  0.2483E+02
   60   26   -6    1.40 3996.75 -0.2972E+02  0.3362E+02
   59   25  -12    1.40 4145.23 -0.3082E+02  0.3113E+02
   70   16  -15    1.40 3299.01 -0.2453E+02  0.2841E+02
   56    4   18    1.40 4620.51 -0.3435E+02  0.2754E+02
   27   23  -25    1.40 2707.03 -0.2013E+02  0.1857E+02
   33    9   25    1.40 3258.96 -0.2423E+02  0.3114E+02
   61   25   -9    1.40 7931.82 -0.5897E+02  0.3074E+02
   60   26   -4    1.40 2632.61 -0.1957E+02  0.3348E+02
   46   16  -26    1.40 7269.56 -0.5405E+02  0.2828E+02
   66    6  -23    1.40 4119.69 -0.3063E+02  0.2332E+02
   20   34  -14    1.40 2896.21 -0.2153E+02  0.1836E+02
   17   37    4    1.40 4158.44 -0.3092E+02  0.2361E+02
   50   16  -25    1.40 7436.10 -0.5529E+02  0.2525E+02
   53   13   17    1.40 2901.01 -0.2157E+02  0.3769E+02
   55   15  -24    1.40 8016.99 -0.5961E+02  0.2737E+02
   61   23    4    1.40 3550.54 -0.2640E+02  0.3643E+02
   32   10   25    1.40 4274.26 -0.3178E+02  0.2418E+02
   62    4  -25    1.40 3060.06 -0.2275E+02  0.1979E+02
   62   18  -19    1.40 2780.42 -0.2067E+02  0.2127E+02
   28   36   -4    1.40 4562.61 -0.3392E+02  0.2786E+02
   37   21   19    1.40 2677.84 -0.1991E+02  0.2630E+02
   59    9  -25    1.40 4383.39 -0.3259E+02  0.2702E+02
   36   24  -23    1.40 4577.67 -0.3403E+02  0.2028E+02
   48   28    7    1.40 3486.63 -0.2592E+02  0.2579E+02
   21   23   22    1.40 2713.00 -0.2017E+02  0.2943E+02
   53   17   15    1.40 8973.25 -0.6672E+02  0.3660E+02
   74   16   -6    1.40 4162.70 -0.3095E+02  0.2479E+02
   55   23    9    1.40 3129.05 -0.2326E+02  0.3633E+02
   53   29   -2    1.40 4468.04 -0.3322E+02  0.2492E+02
   57   11  -25    1.40 6081.96 -0.4522E+02  0.2689E+02
   58   20   10    1.40 8661.24 -0.6440E+02  0.3751E+02
   43   19  -25    1.40 4782.55 -0.3556E+02  0.2336E+02
   62   20  -17    1.40 5080.79 -0.3778E+02  0.2808E+02
   28   36   -1    1.40 3000.59 -0.2231E+02  0.3739E+02
   58   26    1    1.40 9213.33 -0.6850E+02  0.3501E+02
   58   10  -25    1.40 2888.75 -0.2148E+02  0.2709E+02
   42   18   19    1.40 5187.43 -0.3857E+02  0.3529E+02
   64    8   13    1.40 3784.43 -0.2814E+02  0.2932E+02
   48   10   20    1.40 4614.20 -0.3431E+02  0.3728E+02
   51   29  -10    1.40 5838.75 -0.4341E+02  0.1952E+02
   60    4   16    1.40 3419.05 -0.2542E+02  0.2723E+02
   67   21  -10    1.40 5424.46 -0.4033E+02  0.3102E+02
   32   16   23    1.40 2700.98 -0.2008E+02  0.3845E+02
   10   14   28    1.40 4731.07 -0.3518E+02  0.2730E+02
   65   23   -4    1.40 6995.48 -0.5201E+02  0.3240E+02
   66   10   11    1.40 5201.91 -0.3868E+02  0.2966E+02
   56   16  -23    1.40 2786.98 -0.2072E+02  0.2718E+02
   50   30   -1    1.40 2728.75 -0.2029E+02  0.2552E+02
   57   17  -22    1.40 3532.00 -0.2626E+02  0.2707E+02
   39   29  -17    1.40 3078.15 -0.2289E+02  0.1431E+02
   66   22   -2    1.40 3379.12 -0.2512E+02  0.3523E+02
   53   27  -13    1.40 3988.68 -0.2966E+02  0.2217E+02
   31   33    8    1.40 2963.45 -0.2203E+02  0.1658E+02
   62   24  -10    1.40 3250.64 -0.2417E+02  0.3182E+02
   71    9  -19    1.40 2920.91 -0.2172E+02  0.2387E+02
   64    2   14    1.40 3011.27 -0.2239E+02  0.2794E+02
   25   29  -20    1.40 4679.71 -0.3479E+02  0.2116E+02
   37   27   14    1.40 4110.43 -0.3056E+02  0.2508E+02
   69    7  -21    1.40 3834.45 -0.2851E+02  0.2337E+02
   48   24   12    1.40 5612.95 -0.4173E+02  0.3700E+02
   24   30  -19    1.40 2819.40 -0.2096E+02  0.2215E+02
   46   24   13    1.40 3227.81 -0.2400E+02  0.2883E+02
   44   20   17    1.40 8800.36 -0.6543E+02  0.3573E+02
   16   36  -10    1.40 5441.89 -0.4046E+02  0.2827E+02
   54   20  -21    1.40 5330.09 -0.3963E+02  0.2143E+02
   20   36   -9    1.40 2910.07 -0.2164E+02  0.2726E+02
   30   30  -18    1.40 5048.06 -0.3753E+02  0.3426E+02
   73   15  -12    1.40 5554.05 -0.4129E+02  0.2409E+02
    1   23  -25    1.40 3683.40 -0.2739E+02  0.2739E+02
   26   32  -16    1.40 4930.03 -0.3665E+02  0.2582E+02
   47   11   20    1.40 3188.32 -0.2371E+02  0.3599E+02
   10   34   13    1.40 7744.43 -0.5758E+02  0.2496E+02
   18   34  -14    1.40 3554.87 -0.2643E+02  0.2666E+02
   35    5   25    1.40 3201.31 -0.2380E+02  0.2016E+02
   17   37   -6    1.40 2682.29 -0.1994E+02  0.2699E+02
   31   29   14    1.40 3497.01 -0.2600E+02  0.2571E+02
    1   27   22    1.40 3941.68 -0.2931E+02  0.1950E+02
   58   24    5    1.40 4133.64 -0.3073E+02  0.3710E+02
   11   29   19    1.40 5195.57 -0.3863E+02  0.2073E+02
   20   18  -28    1.40 3442.98 -0.2560E+02  0.2454E+02
   28   24  -24    1.40 3513.99 -0.2613E+02  0.2401E+02
   48   30    2    1.40 3612.43 -0.2686E+02  0.1996E+02
   27   25   19    1.40 4540.76 -0.3376E+02  0.1984E+02
   34   28   14    1.40 3821.55 -0.2841E+02  0.2522E+02
    1   37   -8    1.40 4485.39 -0.3335E+02  0.2513E+02
   44   32   -6    1.40 2889.55 -0.2148E+02  0.1541E+02
   30   14  -29    1.40 3088.08 -0.2296E+02  0.1670E+02
   62    8   14    1.40 6301.45 -0.4685E+02  0.2896E+02
   44   14   20    1.40 8002.81 -0.5950E+02  0.3644E+02
   26   24   20    1.40 2985.06 -0.2219E+02  0.2011E+02
   33   33  -12    1.40 2805.70 -0.2086E+02  0.2144E+02
   10   18  -28    1.40 2817.38 -0.2095E+02  0.1341E+02
   46   12   20    1.40 5699.88 -0.4238E+02  0.3511E+02
   35   33  -11    1.40 2968.73 -0.2207E+02  0.1636E+02
   28   20   22    1.40 6773.28 -0.5036E+02  0.2514E+02
   16   30   17    1.41 5530.68 -0.4112E+02  0.2122E+02
   47   15  -26    1.41 3330.55 -0.2476E+02  0.2828E+02
   48    4   21    1.41 5496.48 -0.4087E+02  0.2771E+02
   35   29  -18    1.41 5695.13 -0.4234E+02  0.3249E+02
   46   28    8    1.41 2637.73 -0.1961E+02  0.2706E+02
   53   23   10    1.41 4486.09 -0.3335E+02  0.3740E+02
   53   19  -22    1.41 6815.59 -0.5067E+02  0.2097E+02
   24   36    3    1.41 4056.59 -0.3016E+02  0.2521E+02
   47   29  -13    1.41 3070.53 -0.2283E+02  0.1491E+02
   39   27   13    1.41 5934.99 -0.4413E+02  0.2508E+02
   56   20   11    1.41 4696.14 -0.3492E+02  0.3745E+02
   25   35  -10    1.41 3064.08 -0.2278E+02  0.2665E+02
   36   32    7    1.41 3019.91 -0.2245E+02  0.1628E+02
   17   35  -12    1.41 5833.17 -0.4337E+02  0.2630E+02
   45   27   10    1.41 3358.50 -0.2497E+02  0.2672E+02
   68   20  -10    1.41 3029.64 -0.2253E+02  0.2395E+02
   33   23  -24    1.41 2822.19 -0.2098E+02  0.2324E+02
   56    8  -26    1.41 3002.57 -0.2232E+02  0.2577E+02
   26   36   -5    1.41 3044.44 -0.2264E+02  0.2604E+02
   63   19  -17    1.41 3429.06 -0.2549E+02  0.2285E+02
   11   25  -24    1.41 4880.95 -0.3629E+02  0.1862E+02
   40   32    4    1.41 2906.64 -0.2161E+02  0.1612E+02
   66   20  -13    1.41 2769.43 -0.2059E+02  0.1650E+02
   23   31   14    1.41 3816.57 -0.2838E+02  0.3105E+02
   44   32   -5    1.41 3809.60 -0.2832E+02  0.1556E+02
   35   31  -15    1.41 4029.96 -0.2996E+02  0.1794E+02
    0   38    3    1.41 3002.96 -0.2233E+02  0.2619E+02
    5   37    7    1.41 3273.02 -0.2433E+02  0.2453E+02
   66    6   12    1.41 4676.18 -0.3477E+02  0.2815E+02
   20   30   16    1.41 4866.42 -0.3618E+02  0.2692E+02
   16   34  -14    1.41 2619.62 -0.1948E+02  0.2691E+02
   54   22  -19    1.41 3248.45 -0.2415E+02  0.2181E+02
   67   21   -2    1.4110610.89 -0.7889E+02  0.3372E+02
   64   16  -19    1.41 5344.20 -0.3973E+02  0.2707E+02
   58   12  -24    1.41 5870.84 -0.4365E+02  0.2724E+02
   60   18   10    1.41 2602.77 -0.1935E+02  0.3585E+02
   54   26    5    1.41 3300.45 -0.2454E+02  0.3529E+02
   48    8  -28    1.41 3814.56 -0.2836E+02  0.1699E+02
   23   29  -20    1.41 4189.39 -0.3115E+02  0.2095E+02
   43   27   11    1.41 4531.54 -0.3369E+02  0.2756E+02
   23   23  -25    1.41 2692.69 -0.2002E+02  0.1521E+02
   60   10  -24    1.41 3836.80 -0.2853E+02  0.2761E+02
    4   22   25    1.41 2997.84 -0.2229E+02  0.2577E+02
   35   23   18    1.41 6490.68 -0.4826E+02  0.2682E+02
   12   20  -27    1.41 3700.34 -0.2751E+02  0.1446E+02
   23    9   27    1.41 3792.85 -0.2820E+02  0.1821E+02
   51   27  -14    1.41 5437.12 -0.4042E+02  0.2164E+02
   46   26  -18    1.41 3373.09 -0.2508E+02  0.2108E+02
   62   24   -1    1.41 7796.64 -0.5797E+02  0.3520E+02
   43   23   15    1.41 8007.12 -0.5953E+02  0.3671E+02
   26   14   25    1.41 4433.38 -0.3296E+02  0.2317E+02
   26   34  -12    1.41 2755.95 -0.2049E+02  0.2525E+02
   60   24  -12    1.41 3950.49 -0.2937E+02  0.3102E+02
   57   23    7    1.41 5848.72 -0.4349E+02  0.3730E+02
   18   36   -9    1.41 4312.78 -0.3207E+02  0.2775E+02
   52   26  -15    1.41 2942.47 -0.2188E+02  0.2189E+02
   22   34  -13    1.41 3277.42 -0.2437E+02  0.1795E+02
   27   19  -27    1.41 2802.52 -0.2084E+02  0.2296E+02
   19   31   15    1.41 3380.28 -0.2513E+02  0.3218E+02
   44   16   19    1.41 3861.73 -0.2871E+02  0.3625E+02
   15   37   -6    1.41 2966.49 -0.2206E+02  0.2742E+02
   34   14   23    1.41 3036.41 -0.2258E+02  0.3921E+02
   61   25   -5    1.41 4185.26 -0.3112E+02  0.3179E+02
   19   37   -3    1.41 4240.48 -0.3153E+02  0.2612E+02
   49   23  -20    1.41 5145.81 -0.3826E+02  0.2011E+02
   51   23   11    1.41 5162.87 -0.3839E+02  0.3734E+02
   44    6   22    1.41 2646.62 -0.1968E+02  0.2282E+02
   39    1   24    1.41 2708.94 -0.2014E+02  0.1890E+02
   60    6  -25    1.41 4553.35 -0.3385E+02  0.3066E+02
   51    9  -27    1.41 2607.06 -0.1938E+02  0.2488E+02
   19   31  -18    1.41 3994.28 -0.2970E+02  0.2673E+02
    8   36    9    1.41 4808.24 -0.3575E+02  0.2357E+02
   45    3   22    1.41 5920.83 -0.4402E+02  0.2689E+02
   52   16  -24    1.41 4150.14 -0.3086E+02  0.2087E+02
   73   15  -11    1.41 5296.57 -0.3938E+02  0.2411E+02
   35    3   25    1.41 3757.62 -0.2794E+02  0.2090E+02
   45   27  -17    1.41 4377.71 -0.3255E+02  0.2271E+02
    6   26  -23    1.41 2715.04 -0.2019E+02  0.1855E+02
   48   26  -17    1.41 7728.06 -0.5746E+02  0.3298E+02
   52    6   19    1.41 2963.98 -0.2204E+02  0.2856E+02
   68   12  -19    1.41 3404.19 -0.2531E+02  0.3262E+02
   55   27  -10    1.41 2723.10 -0.2025E+02  0.2446E+02
   50   26  -16    1.41 3703.92 -0.2754E+02  0.2756E+02
   61   21    6    1.41 3591.74 -0.2670E+02  0.3730E+02
   29   33  -13    1.41 2737.27 -0.2035E+02  0.2208E+02
   36   26   15    1.41 3058.87 -0.2274E+02  0.3591E+02
   49   29    3    1.41 3878.64 -0.2884E+02  0.1971E+02
   68   10  -20    1.41 3027.35 -0.2251E+02  0.2321E+02
   52   20   13    1.41 2976.85 -0.2213E+02  0.2559E+02
   67   13  -19    1.41 6178.95 -0.4594E+02  0.2874E+02
   50   20   14    1.41 6099.02 -0.4535E+02  0.3840E+02
    2   38   -1    1.41 3128.92 -0.2326E+02  0.2675E+02
   40   30    8    1.41 3063.61 -0.2278E+02  0.1657E+02
   20   30  -19    1.41 5150.82 -0.3830E+02  0.2676E+02
   10   16   27    1.41 2678.10 -0.1991E+02  0.1804E+02
   18    8   28    1.41 3718.35 -0.2765E+02  0.1766E+02
   30   26   17    1.41 2928.73 -0.2178E+02  0.2020E+02
   40   28   11    1.41 3568.13 -0.2653E+02  0.2053E+02
    1   37    7    1.41 2839.92 -0.2111E+02  0.2532E+02
   12   34  -14    1.41 3029.70 -0.2253E+02  0.2765E+02
   58   26   -1    1.41 3265.01 -0.2428E+02  0.3495E+02
   53   17  -23    1.41 2772.68 -0.2061E+02  0.2139E+02
   63   11   12    1.41 5853.98 -0.4352E+02  0.2884E+02
   62   10   13    1.41 4553.26 -0.3385E+02  0.2957E+02
   60   12  -23    1.41 3374.73 -0.2509E+02  0.2712E+02
   42   22   16    1.41 3971.87 -0.2953E+02  0.2763E+02
   67   21   -7    1.41 4786.40 -0.3559E+02  0.3003E+02
   54   18  -22    1.41 3425.95 -0.2547E+02  0.2637E+02
   61   11  -23    1.41 3396.08 -0.2525E+02  0.2779E+02
   23   35  -10    1.41 2899.88 -0.2156E+02  0.2761E+02
   12   12   28    1.41 4236.92 -0.3150E+02  0.1633E+02
   16   36   -9    1.41 3477.52 -0.2586E+02  0.2857E+02
   38    4   24    1.41 6702.80 -0.4984E+02  0.2084E+02
   45    1   22    1.41 3504.12 -0.2605E+02  0.2703E+02
   57   19  -20    1.41 3032.53 -0.2255E+02  0.2809E+02
   37   29  -17    1.41 3169.73 -0.2357E+02  0.2263E+02
   19   35    8    1.41 5161.29 -0.3837E+02  0.2476E+02
   10   32  -17    1.41 3883.04 -0.2887E+02  0.2591E+02
   60   14  -22    1.41 4581.99 -0.3407E+02  0.2623E+02
   59   19  -19    1.41 2734.14 -0.2033E+02  0.2114E+02
   33   27   15    1.41 4199.42 -0.3122E+02  0.3544E+02
    7   23  -25    1.42 4983.07 -0.3705E+02  0.2630E+02
   57    5  -26    1.42 2791.19 -0.2075E+02  0.2690E+02
   44   10   21    1.42 4442.37 -0.3303E+02  0.3473E+02
    6   34  -14    1.42 3204.68 -0.2383E+02  0.2838E+02
   56   24  -15    1.42 5331.38 -0.3964E+02  0.2830E+02
   28   22  -25    1.42 5302.50 -0.3942E+02  0.2846E+02
   58    2  -26    1.42 4012.82 -0.2984E+02  0.1847E+02
    3   31  -18    1.42 2995.62 -0.2227E+02  0.1561E+02
   22   34    9    1.42 2625.90 -0.1952E+02  0.2118E+02
   65   13  -20    1.42 3454.02 -0.2568E+02  0.2799E+02
   68   20   -8    1.42 3476.53 -0.2585E+02  0.3079E+02
   62   20  -16    1.42 2754.56 -0.2048E+02  0.2828E+02
   77    7  -13    1.42 2977.14 -0.2213E+02  0.2326E+02
   38   10  -29    1.42 3013.83 -0.2241E+02  0.1859E+02
   17   37   -3    1.42 3387.40 -0.2519E+02  0.2590E+02
   29   33    8    1.42 2785.93 -0.2071E+02  0.1674E+02
   24   34  -12    1.42 3256.78 -0.2421E+02  0.2561E+02
   51   11   18    1.42 7019.68 -0.5219E+02  0.3831E+02
   30   18  -27    1.42 3058.89 -0.2274E+02  0.1642E+02
   15   31  -18    1.42 3609.10 -0.2683E+02  0.2701E+02
   54   12  -25    1.42 3498.75 -0.2601E+02  0.2681E+02
   39   21  -24    1.42 4185.83 -0.3112E+02  0.2308E+02
   38   26   14    1.42 2825.36 -0.2101E+02  0.2737E+02
   54   10   17    1.42 8280.93 -0.6157E+02  0.2889E+02
    9   37    5    1.42 4149.46 -0.3085E+02  0.2442E+02
   66   20  -12    1.42 4989.07 -0.3709E+02  0.2986E+02
   34   10   24    1.42 3996.70 -0.2972E+02  0.3204E+02
   65   11  -21    1.42 3122.40 -0.2321E+02  0.2942E+02
   33   11   24    1.42 2870.32 -0.2134E+02  0.3205E+02
   62   22  -13    1.42 6546.75 -0.4867E+02  0.3035E+02
   23   35    6    1.42 2741.01 -0.2038E+02  0.2534E+02
   39   17   20    1.42 5398.31 -0.4014E+02  0.3476E+02
   20   32   13    1.42 3058.44 -0.2274E+02  0.3282E+02
   63   23   -9    1.42 5102.88 -0.3794E+02  0.3081E+02
   62   18  -18    1.42 2901.86 -0.2158E+02  0.2115E+02
   66   18  -15    1.42 4032.53 -0.2998E+02  0.2155E+02
   15   37    2    1.42 4450.57 -0.3309E+02  0.2399E+02
   41   13   21    1.42 6672.11 -0.4961E+02  0.3487E+02
   60   24  -11    1.42 7394.21 -0.5498E+02  0.3020E+02
   22   20   23    1.42 4784.96 -0.3558E+02  0.2467E+02
    7   27  -22    1.42 4565.49 -0.3394E+02  0.1878E+02
   27   23   20    1.42 3332.27 -0.2478E+02  0.2593E+02
   33   31  -15    1.42 4875.91 -0.3625E+02  0.2473E+02
   55   11  -25    1.42 3304.86 -0.2457E+02  0.2728E+02
   40   24   15    1.42 3737.75 -0.2779E+02  0.3573E+02
   54   28   -4    1.42 2617.07 -0.1946E+02  0.3201E+02
   51   27    5    1.4210394.19 -0.7728E+02  0.3562E+02
   13   11   28    1.42 3812.10 -0.2834E+02  0.1615E+02
   26   30  -18    1.42 3056.43 -0.2272E+02  0.2178E+02
   20   36    4    1.42 3692.71 -0.2746E+02  0.2352E+02
   62   24   -4    1.42 2664.07 -0.1981E+02  0.3256E+02
   56   18  -21    1.42 3040.11 -0.2260E+02  0.1776E+02
   50   12   18    1.42 5936.24 -0.4414E+02  0.3796E+02
   29   35   -6    1.42 4673.21 -0.3475E+02  0.2366E+02
   58   10   15    1.42 2767.37 -0.2058E+02  0.2860E+02
   55   25    5    1.42 4367.27 -0.3247E+02  0.3594E+02
   18   30   16    1.42 3840.92 -0.2856E+02  0.2736E+02
   15   37   -4    1.42 3083.73 -0.2293E+02  0.2685E+02
   11   35   10    1.42 3824.85 -0.2844E+02  0.2441E+02
   25   25   19    1.42 3116.62 -0.2317E+02  0.1977E+02
    9   27  -22    1.42 3696.03 -0.2748E+02  0.1863E+02
   24   36   -4    1.42 3119.64 -0.2319E+02  0.2544E+02
   60    6   15    1.42 7329.19 -0.5449E+02  0.2806E+02
   29   31  -16    1.42 4456.25 -0.3313E+02  0.2598E+02
   48   14   18    1.42 6374.28 -0.4739E+02  0.3561E+02
   49   13   18    1.42 4545.84 -0.3380E+02  0.3556E+02
   76   12   -7    1.42 5688.14 -0.4229E+02  0.2509E+02
   54   24  -16    1.42 4679.29 -0.3479E+02  0.2213E+02
   50   12  -26    1.42 4954.42 -0.3684E+02  0.2431E+02
   39   33   -2    1.42 5297.33 -0.3939E+02  0.1595E+02
   40   26   13    1.42 3375.35 -0.2510E+02  0.4103E+02
   20   36   -7    1.42 2960.45 -0.2201E+02  0.2663E+02
   29   35    1    1.42 2995.31 -0.2227E+02  0.1782E+02
   52   20  -21    1.42 5039.32 -0.3747E+02  0.1993E+02
   50    2  -28    1.42 3051.27 -0.2269E+02  0.1152E+02
   32   34   -8    1.42 2812.69 -0.2091E+02  0.1768E+02
   60   24    1    1.42 4821.15 -0.3585E+02  0.3471E+02
   46   22   14    1.42 2838.78 -0.2111E+02  0.3692E+02
   47    3   21    1.42 8425.09 -0.6264E+02  0.2724E+02
   57   17   12    1.42 8540.30 -0.6350E+02  0.3574E+02
   42   32   -6    1.42 3533.12 -0.2627E+02  0.1566E+02
   40   28  -17    1.42 2989.52 -0.2223E+02  0.2206E+02
   26   34  -11    1.42 4214.06 -0.3133E+02  0.2694E+02
   18   34   10    1.42 4330.53 -0.3220E+02  0.2728E+02
    6   30  -19    1.42 2831.99 -0.2106E+02  0.1941E+02
   28   30   13    1.42 5177.46 -0.3849E+02  0.2593E+02
   50   22  -20    1.42 3212.24 -0.2388E+02  0.2056E+02
   72   12  -15    1.42 4716.78 -0.3507E+02  0.2384E+02
    0   18   27    1.42 2973.75 -0.2211E+02  0.1584E+02
   55    7  -26    1.42 3493.35 -0.2597E+02  0.2513E+02
   52   18   14    1.42 5478.19 -0.4073E+02  0.3736E+02
   63   17  -18    1.42 5604.73 -0.4167E+02  0.2839E+02
   34   32    7    1.42 2880.91 -0.2142E+02  0.1628E+02
   24   36   -2    1.42 2799.92 -0.2082E+02  0.2488E+02
   22   28   17    1.42 2805.02 -0.2086E+02  0.2040E+02
   52    6  -27    1.42 4354.68 -0.3238E+02  0.2472E+02
   59   21    7    1.42 3698.08 -0.2750E+02  0.3741E+02
   52   12   17    1.42 4379.77 -0.3256E+02  0.3723E+02
   29   35   -5    1.42 4769.91 -0.3546E+02  0.2333E+02
   61   21  -15    1.42 4689.03 -0.3486E+02  0.2843E+02
   35   31  -14    1.42 2712.70 -0.2017E+02  0.1818E+02
   53    7   18    1.42 5025.12 -0.3736E+02  0.2817E+02
   65   21    0    1.42 3795.22 -0.2822E+02  0.3410E+02
   21    9   27    1.42 7172.38 -0.5333E+02  0.2923E+02
    9   19  -27    1.43 2958.61 -0.2200E+02  0.1222E+02
   22    8   27    1.43 4198.08 -0.3121E+02  0.2848E+02
   41    9   22    1.43 8182.39 -0.6084E+02  0.3383E+02
   15   37    0    1.43 3008.30 -0.2237E+02  0.2434E+02
   55   27   -8    1.43 7833.10 -0.5824E+02  0.3123E+02
   13   29  -20    1.43 3934.76 -0.2925E+02  0.1872E+02
   28   34  -10    1.43 4966.97 -0.3693E+02  0.2780E+02
    5   21   25    1.43 3669.47 -0.2728E+02  0.1829E+02
    9   37    4    1.43 3617.20 -0.2689E+02  0.2478E+02
   13   33  -15    1.43 2896.27 -0.2153E+02  0.2652E+02
   37   25  -21    1.43 3733.28 -0.2776E+02  0.1362E+02
   15   37   -2    1.43 2709.32 -0.2014E+02  0.2604E+02
   51   13   17    1.4314248.61 -0.1059E+03  0.3609E+02
   45   27    9    1.43 5095.40 -0.3788E+02  0.2630E+02
   53   23    9    1.43 3269.80 -0.2431E+02  0.3688E+02
   22   36   -5    1.43 4875.78 -0.3625E+02  0.2553E+02
   67   15    6    1.43 3137.83 -0.2333E+02  0.2921E+02
   56   26    1    1.43 3819.65 -0.2840E+02  0.3566E+02
    5   19   26    1.43 3488.07 -0.2593E+02  0.1782E+02
   64    8   12    1.43 4883.14 -0.3631E+02  0.2882E+02
   54   24    7    1.43 5800.04 -0.4312E+02  0.3719E+02
   69   15  -15    1.43 2747.68 -0.2043E+02  0.3039E+02
   59   13   13    1.43 3380.70 -0.2514E+02  0.2911E+02
   59   23    4    1.4311676.40 -0.8681E+02  0.3709E+02
   54   20  -20    1.43 4841.65 -0.3600E+02  0.2219E+02
   51   17   15    1.43 5447.37 -0.4050E+02  0.3708E+02
   65    9   11    1.43 8754.83 -0.6509E+02  0.2923E+02
   25   35   -8    1.43 2948.09 -0.2192E+02  0.2603E+02
   20   14  -29    1.43 3562.37 -0.2649E+02  0.1723E+02
   31   29  -18    1.43 3846.39 -0.2860E+02  0.2099E+02
   66   20  -11    1.43 3472.75 -0.2582E+02  0.2979E+02
   34   34   -1    1.43 3382.48 -0.2515E+02  0.1982E+02
   18    6   28    1.43 3671.35 -0.2730E+02  0.1491E+02
   37   25   15    1.43 3299.54 -0.2453E+02  0.3586E+02
   11   23   23    1.43 3786.60 -0.2815E+02  0.2808E+02
   56   22  -17    1.43 3880.88 -0.2885E+02  0.2990E+02
   60   24  -10    1.43 5074.07 -0.3773E+02  0.3102E+02
   36    6   24    1.43 5087.45 -0.3783E+02  0.3197E+02
   29   31   11    1.43 4328.06 -0.3218E+02  0.1706E+02
   64   16  -18    1.43 3847.53 -0.2861E+02  0.2112E+02
   41   11  -28    1.43 2846.27 -0.2116E+02  0.1450E+02
   24   22  -25    1.43 3135.53 -0.2331E+02  0.1876E+02
   56   16  -22    1.43 7903.63 -0.5876E+02  0.2665E+02
   44   28    8    1.43 3047.09 -0.2266E+02  0.2045E+02
   57   23  -15    1.43 7770.64 -0.5777E+02  0.2794E+02
   36   24  -22    1.43 3078.68 -0.2289E+02  0.1937E+02
   22   36    1    1.43 3727.17 -0.2771E+02  0.2398E+02
    5   29   19    1.43 4185.39 -0.3112E+02  0.2652E+02
   56   20  -19    1.43 6596.89 -0.4905E+02  0.2765E+02
   59   17  -20    1.43 4292.18 -0.3191E+02  0.2104E+02
   49   21  -21    1.43 3156.67 -0.2347E+02  0.1993E+02
   53    3  -27    1.43 3012.95 -0.2240E+02  0.2508E+02
   27   21   21    1.43 2691.95 -0.2001E+02  0.2499E+02
   72   16   -4    1.43 3834.46 -0.2851E+02  0.2536E+02
   51   15   16    1.43 6388.54 -0.4750E+02  0.3550E+02
   26   20  -26    1.43 3862.73 -0.2872E+02  0.1653E+02
   65    7  -22    1.43 6580.58 -0.4893E+02  0.1989E+02
   57   25    2    1.43 7907.41 -0.5879E+02  0.3657E+02
   53   27    2    1.43 3690.94 -0.2744E+02  0.3579E+02
   65    5   12    1.43 4630.74 -0.3443E+02  0.2776E+02
   54   16   14    1.43 2994.31 -0.2226E+02  0.3588E+02
   61   23  -11    1.43 3206.43 -0.2384E+02  0.3019E+02
   38   22   17    1.43 3014.71 -0.2241E+02  0.2649E+02
   35   33    3    1.43 3929.12 -0.2921E+02  0.1636E+02
   21   35    6    1.43 3015.27 -0.2242E+02  0.2490E+02
   60   20    7    1.43 5904.43 -0.4390E+02  0.3630E+02
   32    6   25    1.43 7948.23 -0.5909E+02  0.3013E+02
   55   27   -7    1.43 3852.91 -0.2865E+02  0.3101E+02
   23   31   13    1.43 3435.26 -0.2554E+02  0.3171E+02
   64   20    3    1.43 3184.21 -0.2367E+02  0.3528E+02
   48   26  -16    1.43 5377.61 -0.3998E+02  0.3298E+02
   14   16   26    1.43 3280.90 -0.2439E+02  0.2098E+02
   56   14   14    1.43 5406.04 -0.4019E+02  0.3816E+02

 NUMBER OF UNIQUE ALIEN REFLECTIONS WITH A Z-SCORE ABOVE LIMIT      3766
 (ALIENS ABOVE LIMIT (REJECT_ALIEN=      20.0) ARE MARKED INVALID)

 NUMBER OF REFLECTION RECORDS ON OUTPUT FILE "XDS_ASCII.HKL"      159281
 NUMBER OF ACCEPTED OBSERVATIONS (INCLUDING SYSTEMATIC ABSENCES)  149634
 NUMBER OF REJECTED MISFITS & ALIENS (marked by -1*SIGMA(IOBS))     9647


 cpu time used                205.9 sec
 elapsed wall-clock time       12.7 sec
