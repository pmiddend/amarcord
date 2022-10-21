module Amarcord.Crystallography exposing (validateCellDescription, validatePointGroup)

import Amarcord.Html exposing (div_, em_, p_, span_)
import Amarcord.Parser exposing (deadEndsToHtml, signedNumber, spaces1)
import Html exposing (Html, a, text)
import Html.Attributes exposing (href)
import List exposing (any)
import Parser exposing ((|.), spaces)
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


validLatticeTypes : List String
validLatticeTypes =
    [ "triclinic", "monoclinic", "orthorhombic", "tetragonal", "rhombohedral", "hexagonal", "cubic" ]


validCenterings : List String
validCenterings =
    [ "P", "A", "B", "C", "I", "F", "R", "H" ]


cellDescriptionParser : Parser.Parser ()
cellDescriptionParser =
    let
        latticeTypeParser : Parser.Parser ()
        latticeTypeParser =
            Parser.oneOf (List.map Parser.keyword validLatticeTypes)

        centeringParser : Parser.Parser ()
        centeringParser =
            Parser.oneOf (List.map Parser.symbol validCenterings)

        uniqueAxisParser : Parser.Parser ()
        uniqueAxisParser =
            Parser.oneOf [ Parser.symbol "a", Parser.symbol "b", Parser.symbol "c", Parser.symbol "?", Parser.symbol "*" ]
    in
    Parser.succeed ()
        |. latticeTypeParser
        |. spaces1
        |. centeringParser
        |. spaces1
        |. uniqueAxisParser
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


validateCellDescription : String -> Result (Html msg) ()
validateCellDescription input =
    if String.isEmpty input then
        Ok ()

    else
        case Parser.run cellDescriptionParser input of
            Err deadEnds ->
                Err <|
                    div_
                        [ p_ [ text "Invalid cell description “", em_ [ text input ], text "”." ]
                        , deadEndsToHtml True deadEnds
                        ]

            Ok _ ->
                Ok ()
