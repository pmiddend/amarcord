module Amarcord.Crystallography exposing (..)

import Amarcord.Html exposing (em_, span_)
import Amarcord.Parser exposing (signedNumber, spaces1)
import Amarcord.Util exposing (deadEndsToString)
import Html exposing (Html, a, text)
import Html.Attributes exposing (href)
import List exposing (any)
import Maybe.Extra
import Parser exposing ((|.), (|=), spaces)
import Set


validPointGroups : Set.Set String
validPointGroups =
    -- Taken from https://www.desy.de/~twhite/crystfel/tutorial-0.9.1.html#merge
    Set.fromList
        [ -- Triclinic
          "1"
        , "-1"

        -- Monoclinic
        , "2/m"
        , "2"
        , "m"

        -- Orthorhombic
        , "mmm"
        , "222"
        , "mm2"

        -- Tetragonal
        , "4/m"
        , "4"
        , "-4"
        , "4/mmm"
        , "422"
        , "-42m"
        , "-4m2"
        , "4mm"

        -- Trigonal (rhombohedral axes)
        , "3_R"
        , "-3_R"
        , "32_R"
        , "3m_R"
        , "-3m_R"

        -- Trigonal (hexagonal axes)
        , "3_H"
        , "-3_H"
        , "321_H"
        , "312_H"
        , "3m1_H"
        , "31m_H"
        , "-3m1_H"
        , "-31m_H"

        -- Hexagonal
        , "6/m"
        , "6"
        , "-6"
        , "6/mmm"
        , "622"
        , "-62m"
        , "-6m2"
        , "6mm"

        -- Cubic
        , "23"
        , "m-3"
        , "432"
        , "-43m"
        , "m-3m"
        ]


validatePointGroup : String -> Result (Html msg) ()
validatePointGroup pointGroup =
    if String.isEmpty pointGroup then
        Ok ()

    else
        let
            realPointGroup =
                if any (\x -> String.endsWith x pointGroup) [ "_uaa", "_uab", "uac" ] then
                    String.dropRight 4 pointGroup

                else
                    pointGroup
        in
        if Set.member realPointGroup validPointGroups then
            Ok ()

        else
            Err <|
                span_
                    [ text "Invalid point group “"
                    , em_ [ text pointGroup ]
                    , text "”! Please refer to section 13 of the "
                    , a [ href "https://www.desy.de/~twhite/crystfel/tutorial-0.9.1.html#merge" ] [ text "CrystFEL tutorial" ]
                    , text " for info on the available point groups."
                    ]


type Axis
    = AxisA
    | AxisB
    | AxisC


allAxes : List Axis
allAxes =
    [ AxisA, AxisB, AxisC ]


axisToString : Axis -> String
axisToString x =
    case x of
        AxisA ->
            "a"

        AxisB ->
            "b"

        AxisC ->
            "c"


type Centering
    = CentPrimitive
    | CentBody
    | CentFace
    | CentHexagonal
    | CentRhombohedral
    | CentBase Axis


allCenterings : List Centering
allCenterings =
    [ CentPrimitive
    , CentBody
    , CentFace
    , CentHexagonal
    , CentRhombohedral
    , CentBase AxisA
    , CentBase AxisB
    , CentBase AxisC
    ]


centeringFromString : String -> Maybe Centering
centeringFromString x =
    List.head (List.filter (\ls -> centeringToString ls == x) allCenterings)


type LatticeSystem
    = LatticeTriclinic
    | LatticeMonoclinic
    | LatticeOrthorhombic
    | LatticeTetragonal
    | LatticeRhombohedral
    | LatticeHexagonal
    | LatticeCubic


allLatticeSystems : List LatticeSystem
allLatticeSystems =
    [ LatticeTriclinic
    , LatticeMonoclinic
    , LatticeOrthorhombic
    , LatticeTetragonal
    , LatticeRhombohedral
    , LatticeHexagonal
    , LatticeCubic
    ]


latticeSystemToString : LatticeSystem -> String
latticeSystemToString x =
    case x of
        LatticeTriclinic ->
            "triclinic"

        LatticeMonoclinic ->
            "monoclinic"

        LatticeOrthorhombic ->
            "orthorhombic"

        LatticeTetragonal ->
            "tetragonal"

        LatticeRhombohedral ->
            "rhombohedral"

        LatticeHexagonal ->
            "hexagonal"

        _ ->
            "cubic"


latticeSystemFromString : String -> Maybe LatticeSystem
latticeSystemFromString x =
    List.head (List.filter (\ls -> latticeSystemToString ls == x) allLatticeSystems)


latticeSystemHasUniqueAxis : LatticeSystem -> Bool
latticeSystemHasUniqueAxis x =
    case x of
        LatticeMonoclinic ->
            True

        LatticeTetragonal ->
            True

        LatticeHexagonal ->
            True

        _ ->
            False


type BravaisLattice
    = Triclinic Centering
    | Monoclinic Centering Axis
    | Orthorhombic Centering
    | Tetragonal Centering Axis
    | Rhombohedral Centering
    | Hexagonal Centering Axis
    | Cubic Centering


bravaisLatticeToSystem : BravaisLattice -> LatticeSystem
bravaisLatticeToSystem x =
    case x of
        Triclinic _ ->
            LatticeTriclinic

        Monoclinic _ _ ->
            LatticeMonoclinic

        Orthorhombic _ ->
            LatticeOrthorhombic

        Tetragonal _ _ ->
            LatticeTetragonal

        Rhombohedral _ ->
            LatticeRhombohedral

        Hexagonal _ _ ->
            LatticeHexagonal

        _ ->
            LatticeCubic


bravaisLatticeUniqueAxis : BravaisLattice -> Maybe Axis
bravaisLatticeUniqueAxis x =
    case x of
        Monoclinic _ ua ->
            Just ua

        Tetragonal _ ua ->
            Just ua

        Hexagonal _ ua ->
            Just ua

        _ ->
            Nothing


bravaisLatticeCentering : BravaisLattice -> Centering
bravaisLatticeCentering x =
    case x of
        Triclinic c ->
            c

        Monoclinic c _ ->
            c

        Orthorhombic c ->
            c

        Tetragonal c _ ->
            c

        Rhombohedral c ->
            c

        Hexagonal c _ ->
            c

        Cubic c ->
            c


bravaisLatticeToString : BravaisLattice -> String
bravaisLatticeToString x =
    case x of
        Triclinic centering ->
            "triclinic " ++ centeringToString centering ++ " ?"

        Monoclinic centering ua ->
            "monoclinic " ++ centeringToString centering ++ " " ++ axisToString ua

        Orthorhombic centering ->
            "orthorhombic " ++ centeringToString centering ++ " ?"

        Tetragonal centering ua ->
            "tetragonal " ++ centeringToString centering ++ " " ++ axisToString ua

        Rhombohedral centering ->
            "rhombohedral " ++ centeringToString centering ++ " ?"

        Hexagonal centering ua ->
            "hexagonal " ++ centeringToString centering ++ " " ++ axisToString ua

        Cubic centering ->
            "cubic " ++ centeringToString centering ++ " ?"



-- For pretty-printing, not for the input line


bravaisLatticeToStringNoUnknownAxis : BravaisLattice -> String
bravaisLatticeToStringNoUnknownAxis x =
    case x of
        Triclinic centering ->
            "triclinic " ++ centeringToString centering

        Monoclinic centering ua ->
            "monoclinic " ++ axisToString ua ++ " " ++ centeringToString centering

        Orthorhombic centering ->
            "orthorhombic " ++ centeringToString centering

        Tetragonal centering ua ->
            "tetragonal " ++ axisToString ua ++ " " ++ centeringToString centering

        Rhombohedral centering ->
            "rhombohedral " ++ centeringToString centering

        Hexagonal centering ua ->
            "hexagonal " ++ axisToString ua ++ " " ++ centeringToString centering

        Cubic centering ->
            "cubic " ++ centeringToString centering


bravaisLatticeToStringNoCentering : BravaisLattice -> String
bravaisLatticeToStringNoCentering x =
    case x of
        Triclinic _ ->
            "triclinic"

        Monoclinic _ ua ->
            "monoclinic " ++ axisToString ua

        Orthorhombic _ ->
            "orthorhombic"

        Tetragonal _ ua ->
            "tetragonal " ++ axisToString ua

        Rhombohedral _ ->
            "rhombohedral"

        Hexagonal _ ua ->
            "hexagonal " ++ axisToString ua

        _ ->
            "cubic"


type alias CellDescription =
    { bravaisLattice : BravaisLattice
    , cellA : Float
    , cellB : Float
    , cellC : Float
    , cellAlpha : Float
    , cellBeta : Float
    , cellGamma : Float
    }


cellDescriptionToString : CellDescription -> String
cellDescriptionToString { bravaisLattice, cellA, cellB, cellC, cellAlpha, cellBeta, cellGamma } =
    bravaisLatticeToString bravaisLattice
        ++ " ("
        ++ String.fromFloat cellA
        ++ " "
        ++ String.fromFloat cellB
        ++ " "
        ++ String.fromFloat cellC
        ++ ") ("
        ++ String.fromFloat cellAlpha
        ++ " "
        ++ String.fromFloat cellBeta
        ++ " "
        ++ String.fromFloat cellGamma
        ++ ")"


constantParser : (a -> String) -> a -> Parser.Parser a
constantParser toString x =
    Parser.map (always x) <| Parser.keyword (toString x)


parseLatticeSystem : String -> Result (List Parser.DeadEnd) LatticeSystem
parseLatticeSystem =
    Parser.run (Parser.oneOf (List.map (constantParser latticeSystemToString) allLatticeSystems))


bravaisLatticeParser : Parser.Parser BravaisLattice
bravaisLatticeParser =
    Parser.oneOf
        [ Parser.succeed
            Triclinic
            |. Parser.keyword "triclinic"
            |. spaces1
            |= centeringParser [ CentPrimitive ]
            |. spaces1
            |. Parser.oneOf [ Parser.keyword "*", Parser.keyword "?" ]
        , Parser.succeed
            Monoclinic
            |. Parser.keyword "monoclinic"
            |. spaces1
            |= centeringParser [ CentPrimitive, CentBody, CentFace, CentBase AxisA, CentBase AxisB, CentBase AxisC ]
            |. spaces1
            |= axisParser
        , Parser.succeed
            Orthorhombic
            |. Parser.keyword "orthorhombic"
            |. spaces1
            |= centeringParser [ CentPrimitive, CentBase AxisA, CentBase AxisB, CentBase AxisC ]
            |. spaces1
            |. Parser.oneOf [ Parser.keyword "*", Parser.keyword "?" ]
        , Parser.succeed
            Tetragonal
            |. Parser.keyword "tetragonal"
            |. spaces1
            |= centeringParser [ CentPrimitive, CentBody ]
            |. spaces1
            |= axisParser
        , Parser.succeed
            Rhombohedral
            |. Parser.keyword "rhombohedral"
            |. spaces1
            |= centeringParser [ CentRhombohedral ]
            |. spaces1
            |. Parser.oneOf [ Parser.keyword "*", Parser.keyword "?" ]
        , Parser.succeed
            Hexagonal
            |. Parser.keyword "hexagonal"
            |. spaces1
            |= centeringParser [ CentHexagonal ]
            |. spaces1
            |= axisParser
        , Parser.succeed
            Cubic
            |. Parser.keyword "cubic"
            |. spaces1
            |= centeringParser [ CentBody, CentFace, CentPrimitive ]
            |. spaces1
            |. Parser.oneOf [ Parser.keyword "*", Parser.keyword "?" ]
        ]


centeringParser : List Centering -> Parser.Parser Centering
centeringParser centerings =
    Parser.oneOf (List.map (constantParser centeringToString) centerings)


cellDescriptionParserPossiblyInvalidCrystalSystem : Parser.Parser ( LatticeSystem, Centering, Maybe Axis )
cellDescriptionParserPossiblyInvalidCrystalSystem =
    Parser.succeed
        (\ls c a -> ( ls, c, a ))
        |= Parser.oneOf (List.map (constantParser latticeSystemToString) allLatticeSystems)
        |. spaces1
        |= Parser.oneOf (List.map (constantParser centeringToString) allCenterings)
        |. spaces1
        |= Parser.oneOf [ Parser.map Just axisParser, Parser.map (always Nothing) (Parser.keyword "?"), Parser.map (always Nothing) (Parser.keyword "*") ]
        |. spaces1
        |. Parser.symbol "("
        |. spaces
        |. signedNumber
        |. spaces1
        |. signedNumber
        |. spaces1
        |. signedNumber
        |. spaces
        |. Parser.symbol ")"
        |. spaces1
        |. Parser.symbol "("
        |. spaces
        |. signedNumber
        |. spaces1
        |. signedNumber
        |. spaces1
        |. signedNumber
        |. spaces
        |. Parser.symbol ")"


cellDescriptionParser : Parser.Parser CellDescription
cellDescriptionParser =
    Parser.succeed
        CellDescription
        |= bravaisLatticeParser
        |. spaces1
        |. Parser.symbol "("
        |. spaces
        |= signedNumber
        |. spaces1
        |= signedNumber
        |. spaces1
        |= signedNumber
        |. spaces
        |. Parser.symbol ")"
        |. spaces1
        |. Parser.symbol "("
        |. spaces
        |= signedNumber
        |. spaces1
        |= signedNumber
        |. spaces1
        |= signedNumber
        |. spaces
        |. Parser.symbol ")"


parseCellDescription : String -> Result String CellDescription
parseCellDescription s =
    case Parser.run cellDescriptionParser s of
        Err e ->
            case Parser.run cellDescriptionParserPossiblyInvalidCrystalSystem s of
                Ok ( ls, c, a ) ->
                    if not (List.member c (centeringsForSystem ls)) then
                        Err <| latticeSystemToString ls ++ " cannot have " ++ centeringToString c ++ " centering."

                    else if latticeSystemHasUniqueAxis ls && Maybe.Extra.isNothing a then
                        Err <| latticeSystemToString ls ++ " needs a unique axis."

                    else if not (latticeSystemHasUniqueAxis ls) && Maybe.Extra.isJust a then
                        Err <| latticeSystemToString ls ++ " does not have a unique axis."

                    else
                        Err "Invalid combination of Bravais lattice type, unique axis and centering."

                Err _ ->
                    Err (deadEndsToString e)

        Ok cd ->
            Ok cd


axisParser : Parser.Parser Axis
axisParser =
    Parser.oneOf (List.map (constantParser axisToString) allAxes)


parseAxis : String -> Maybe Axis
parseAxis x =
    case x of
        "a" ->
            Just AxisA

        "b" ->
            Just AxisB

        "c" ->
            Just AxisC

        -- Here we are deliberately putting *, ? and erroneous axes into one pot
        _ ->
            Nothing


centeringToString : Centering -> String
centeringToString x =
    case x of
        CentPrimitive ->
            "P"

        CentBody ->
            "I"

        CentFace ->
            "F"

        CentRhombohedral ->
            "R"

        CentHexagonal ->
            "H"

        CentBase axis ->
            String.toUpper (axisToString axis)


centeringsForSystem : LatticeSystem -> List Centering
centeringsForSystem s =
    case s of
        LatticeCubic ->
            [ CentBody, CentFace, CentPrimitive ]

        LatticeHexagonal ->
            [ CentHexagonal ]

        LatticeRhombohedral ->
            [ CentRhombohedral ]

        LatticeTetragonal ->
            [ CentPrimitive, CentBody ]

        LatticeOrthorhombic ->
            [ CentPrimitive, CentBase AxisA, CentBase AxisB, CentBase AxisC ]

        LatticeMonoclinic ->
            [ CentPrimitive, CentBody, CentFace, CentBase AxisA, CentBase AxisB, CentBase AxisC ]

        LatticeTriclinic ->
            [ CentPrimitive ]


parseCentering : String -> Maybe Centering
parseCentering x =
    case x of
        "P" ->
            Just CentPrimitive

        "I" ->
            Just CentBody

        "F" ->
            Just CentFace

        "R" ->
            Just CentRhombohedral

        "H" ->
            Just CentHexagonal

        _ ->
            case parseAxis (String.toLower x) of
                Just axis ->
                    Just (CentBase axis)

                _ ->
                    Nothing


almostEqual : Float -> Float -> Bool
almostEqual x y =
    abs (x - y) < 0.01


possibleIssue : CellDescription -> Maybe String
possibleIssue m =
    let
        ( maxSymmetry, uniqueAxis ) =
            maximumSymmetry m

        thisSymmetry =
            bravaisLatticeToSystem m.bravaisLattice
    in
    if thisSymmetry /= maxSymmetry then
        Just <| "You specified " ++ bravaisLatticeToString m.bravaisLattice ++ " but the maximum symmetry is " ++ latticeSystemToString maxSymmetry ++ " (check the lengths and angles)."

    else
        let
            thisUniqueAxis =
                bravaisLatticeUniqueAxis m.bravaisLattice
        in
        if uniqueAxis /= thisUniqueAxis then
            Just <|
                "You specified "
                    ++ (case thisUniqueAxis of
                            Nothing ->
                                "no"

                            Just ua ->
                                axisToString ua ++ " as "
                       )
                    ++ " unique axis, but the symmetry dictates "
                    ++ (case uniqueAxis of
                            Nothing ->
                                "no"

                            Just ua ->
                                axisToString ua ++ " as "
                       )
                    ++ " unique axis."

        else
            Nothing


almostNinety : Float -> Bool
almostNinety x =
    almostEqual x 90


maximumSymmetry : CellDescription -> ( LatticeSystem, Maybe Axis )
maximumSymmetry { cellA, cellB, cellC, cellAlpha, cellBeta, cellGamma } =
    if almostNinety cellAlpha && almostNinety cellBeta && almostNinety cellGamma then
        -- α = β = γ = 90°, could be cubic, tetragonal or orthorhombic
        if almostEqual cellA cellB && almostEqual cellB cellC then
            -- α = β = γ = 90°, a = b = c, so we're cubic (and )
            ( LatticeCubic, Nothing )

        else if almostEqual cellA cellB then
            ( LatticeTetragonal, Just AxisC )

        else if almostEqual cellA cellC then
            ( LatticeTetragonal, Just AxisB )

        else if almostEqual cellB cellC then
            ( LatticeTetragonal, Just AxisA )

        else
            ( LatticeOrthorhombic, Nothing )

    else if almostNinety cellAlpha && almostNinety cellBeta && almostEqual cellGamma 120 then
        ( LatticeHexagonal, Just AxisC )

    else if almostNinety cellAlpha && almostNinety cellGamma && almostEqual cellBeta 120 then
        ( LatticeHexagonal, Just AxisB )

    else if almostNinety cellBeta && almostNinety cellGamma && almostEqual cellAlpha 120 then
        ( LatticeHexagonal, Just AxisA )

    else if almostNinety cellAlpha && almostNinety cellBeta then
        ( LatticeMonoclinic, Just AxisC )

    else if almostNinety cellAlpha && almostNinety cellGamma then
        ( LatticeMonoclinic, Just AxisB )

    else if almostNinety cellBeta && almostNinety cellGamma then
        ( LatticeMonoclinic, Just AxisA )

    else if almostEqual cellAlpha cellBeta && almostEqual cellBeta cellGamma && almostEqual cellA cellB && almostEqual cellB cellC then
        ( LatticeRhombohedral, Nothing )

    else
        ( LatticeTriclinic, Nothing )


cellDescriptionsAlmostEqual : CellDescription -> CellDescription -> Bool
cellDescriptionsAlmostEqual left right =
    left.bravaisLattice
        == right.bravaisLattice
        && almostEqual left.cellA right.cellA
        && almostEqual left.cellB right.cellB
        && almostEqual left.cellC right.cellC
        && almostEqual left.cellAlpha right.cellAlpha
        && almostEqual left.cellBeta right.cellBeta
        && almostEqual left.cellGamma right.cellGamma


cellDescriptionsAlmostEqualStrings : String -> String -> Bool
cellDescriptionsAlmostEqualStrings left right =
    Result.withDefault False
        (Result.map2 cellDescriptionsAlmostEqual (parseCellDescription left) (parseCellDescription right))
