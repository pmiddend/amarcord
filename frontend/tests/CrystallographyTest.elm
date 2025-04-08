module CrystallographyTest exposing (..)

import Amarcord.Crystallography exposing (Axis(..), BravaisLattice(..), CellDescription, Centering(..), LatticeSystem(..), allAxes, allLatticeSystems, cellDescriptionToString, centeringsForSystem, maximumSymmetry, parseCellDescription)
import Expect
import Fuzz exposing (Fuzzer, niceFloat)
import Maybe.Extra
import Result.Extra
import Test exposing (..)


axisFuzzer : Fuzzer Axis
axisFuzzer =
    Fuzz.oneOfValues allAxes


bravaisLatticeFuzzer : Fuzzer BravaisLattice
bravaisLatticeFuzzer =
    Fuzz.oneOfValues allLatticeSystems
        |> Fuzz.andThen
            (\noAxCrystalSystem ->
                axisFuzzer
                    |> Fuzz.andThen
                        (\axis ->
                            Fuzz.oneOfValues (centeringsForSystem noAxCrystalSystem)
                                |> Fuzz.map
                                    (\centering ->
                                        case noAxCrystalSystem of
                                            LatticeTriclinic ->
                                                Triclinic centering

                                            LatticeMonoclinic ->
                                                Monoclinic centering axis

                                            LatticeOrthorhombic ->
                                                Orthorhombic centering

                                            LatticeTetragonal ->
                                                Tetragonal centering axis

                                            LatticeRhombohedral ->
                                                Rhombohedral centering

                                            LatticeHexagonal ->
                                                Hexagonal centering axis

                                            LatticeCubic ->
                                                Cubic centering
                                    )
                        )
            )


cellDescriptionFuzzer : Fuzzer CellDescription
cellDescriptionFuzzer =
    Fuzz.floatRange 0 1000
        |> Fuzz.andThen
            (\cellA ->
                Fuzz.floatRange 0 1000
                    |> Fuzz.andThen
                        (\cellB ->
                            Fuzz.floatRange 0 1000
                                |> Fuzz.andThen
                                    (\cellC ->
                                        Fuzz.floatRange 0 360
                                            |> Fuzz.andThen
                                                (\cellAlpha ->
                                                    Fuzz.floatRange 0 360
                                                        |> Fuzz.andThen
                                                            (\cellBeta ->
                                                                Fuzz.floatRange 0 360
                                                                    |> Fuzz.andThen
                                                                        (\cellGamma ->
                                                                            bravaisLatticeFuzzer
                                                                                |> Fuzz.map
                                                                                    (\bravaisLattice ->
                                                                                        { bravaisLattice = bravaisLattice
                                                                                        , cellA = cellA
                                                                                        , cellB = cellB
                                                                                        , cellC = cellC
                                                                                        , cellAlpha = cellAlpha
                                                                                        , cellBeta = cellBeta
                                                                                        , cellGamma = cellGamma
                                                                                        }
                                                                                    )
                                                                        )
                                                            )
                                                )
                                    )
                        )
            )


niceFloat3 : Fuzzer ( Float, Float, Float )
niceFloat3 =
    Fuzz.triple niceFloat niceFloat niceFloat


suite : Test
suite =
    describe "Crystallography"
        [ describe "real parsing"
            [ test "simple cell description" <|
                \_ ->
                    Expect.equal
                        (Ok
                            { bravaisLattice = Monoclinic CentFace AxisC
                            , cellA = 30.0
                            , cellB = 30.0
                            , cellC = 30.0
                            , cellAlpha = 40.0
                            , cellBeta = 40.0
                            , cellGamma = 40.0
                            }
                        )
                        (parseCellDescription "monoclinic F c (30 30 30) (40 40 40)")
            , test "invalid cell description: only two sides" <|
                \_ ->
                    Expect.equal
                        True
                        (Result.Extra.isErr <| parseCellDescription "monoclinic F c (30 30) (40 40 40)")
            , test "invalid cell description: only two angles" <|
                \_ ->
                    Expect.equal
                        True
                        (Result.Extra.isErr <| parseCellDescription "monoclinic F c (30 30 30) (40 40)")
            , test "invalid cell description: wrong centering" <|
                \_ ->
                    Expect.equal
                        True
                        (Result.Extra.isErr <| parseCellDescription "hexagonal F c (30 30 30) (40 40 40)")
            , test "monoclinic P a should work" <|
                \_ ->
                    Expect.equal
                        (Ok
                            { bravaisLattice = Monoclinic CentPrimitive AxisA
                            , cellA = 0
                            , cellB = 0
                            , cellC = 0
                            , cellAlpha = 0
                            , cellBeta = 0
                            , cellGamma = 0
                            }
                        )
                        (parseCellDescription "monoclinic P a (0 0 0) (0 0 0)")
            , test "triclinic P should work" <|
                \_ ->
                    Expect.equal
                        (Ok
                            { bravaisLattice = Triclinic CentPrimitive
                            , cellA = 1
                            , cellB = 1
                            , cellC = 1
                            , cellAlpha = 90
                            , cellBeta = 90
                            , cellGamma = 90
                            }
                        )
                        (parseCellDescription "triclinic P * (1 1 1) (90 90 90)")
            , test "specific example doesnt't work" <|
                \_ ->
                    Expect.equal
                        (Ok
                            { bravaisLattice = Rhombohedral CentRhombohedral
                            , cellA = 0
                            , cellB = 0
                            , cellC = 0
                            , cellAlpha = 0
                            , cellBeta = 0
                            , cellGamma = 0
                            }
                        )
                        (parseCellDescription "rhombohedral R ? (0 0 0) (0 0 0)")
            , fuzz cellDescriptionFuzzer "involute parse and coparse" <|
                \cellDescription ->
                    Expect.equal
                        (Ok cellDescription)
                        (parseCellDescription (cellDescriptionToString cellDescription))
            ]
        , describe "maximum symmetry"
            [ fuzz2 niceFloat bravaisLatticeFuzzer "maximum symmetry is cubic" <|
                \lengthValue bravaisLattice ->
                    Expect.equal ( LatticeCubic, Nothing )
                        (maximumSymmetry
                            { cellAlpha = 90
                            , cellBeta = 90
                            , cellGamma = 90
                            , cellA = lengthValue
                            , cellB = lengthValue
                            , cellC = lengthValue
                            , bravaisLattice = bravaisLattice
                            }
                        )
            , fuzz2 niceFloat3 bravaisLatticeFuzzer "all ninety angles are at least orthorhombic" <|
                \( cellA, cellB, cellC ) bravaisLattice ->
                    maximumSymmetry
                        { cellAlpha = 90
                        , cellBeta = 90
                        , cellGamma = 90
                        , cellA = cellA
                        , cellB = cellB
                        , cellC = cellC
                        , bravaisLattice = bravaisLattice
                        }
                        |> Expect.all
                            [ \( maxSymm, _ ) -> Expect.equal (maxSymm == LatticeOrthorhombic || maxSymm == LatticeTetragonal || maxSymm == LatticeCubic) True
                            , \( maxSymm, axis ) -> Expect.equal (maxSymm /= LatticeTetragonal || Maybe.Extra.isJust axis) True
                            ]
            ]
        ]
