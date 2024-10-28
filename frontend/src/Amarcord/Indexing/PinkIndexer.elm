module Amarcord.Indexing.PinkIndexer exposing (Model, fromCommandLine, init, toCommandLine, view)

import Amarcord.CommandLineParser exposing (CommandLineOption(..))
import Amarcord.Indexing.Util exposing (CommandLineOptionResult(..), integerToCommandLine, numberToCommandLine, viewCitation, viewNumericInput)
import Dict
import Html exposing (Html, div, p, text)
import String


type alias Model =
    { consideredPeaksCount : String
    , angleResolution : String
    , refinementType : String
    , tolerance : String
    , reflectionRadius : String
    , maxResolutionForIndexing : String
    , maxRefinementDisbalance : String
    }


init : Model
init =
    { consideredPeaksCount = ""
    , angleResolution = ""
    , refinementType = ""
    , tolerance = ""
    , reflectionRadius = ""
    , maxResolutionForIndexing = ""
    , maxRefinementDisbalance = ""
    }


toCommandLine : Model -> List (Result String (List CommandLineOption))
toCommandLine model =
    [ integerToCommandLine "pinkIndexer-considered-peaks-count" model.consideredPeaksCount (\n -> [ LongOption "pinkIndexer-considered-peaks-count" n ])
    , numberToCommandLine "pinkIndexer-angle-resolution" model.angleResolution (\n -> [ LongOption "pinkIndexer-angle-resolution" n ])
    , numberToCommandLine "pinkIndexer-refinement-type" model.refinementType (\n -> [ LongOption "pinkIndexer-refinement-type" n ])
    , numberToCommandLine "pinkIndexer-tolerance" model.tolerance (\n -> [ LongOption "pinkIndexer-tolerance" n ])
    , numberToCommandLine "pinkIndexer-reflection-radius" model.reflectionRadius (\n -> [ LongOption "pinkIndexer-reflection-radius" n ])
    , numberToCommandLine "pinkIndexer-max-resolution-for-indexing" model.maxResolutionForIndexing (\n -> [ LongOption "pinkIndexer-max-resolution-for-indexing" n ])
    , numberToCommandLine "pinkIndexer-max-refinement-disbalance" model.maxRefinementDisbalance (\n -> [ LongOption "pinkIndexer-max-refinement-disbalance" n ])
    ]


fromCommandLine : CommandLineOption -> Model -> CommandLineOptionResult Model
fromCommandLine option priorOptions =
    case option of
        LongOption optionName optionValue ->
            let
                knownOptions =
                    Dict.fromList
                        [ ( "considered-peaks-count", \_ -> { priorOptions | consideredPeaksCount = optionValue } )
                        , ( "angle-resolution", \_ -> { priorOptions | angleResolution = optionValue } )
                        , ( "refinement-type", \_ -> { priorOptions | refinementType = optionValue } )
                        , ( "tolerance", \_ -> { priorOptions | tolerance = optionValue } )
                        , ( "reflection-radius", \_ -> { priorOptions | reflectionRadius = optionValue } )
                        , ( "max-resolution-for-indexing", \_ -> { priorOptions | maxResolutionForIndexing = optionValue } )
                        , ( "max-refinement-disbalance", \_ -> { priorOptions | maxRefinementDisbalance = optionValue } )
                        ]
            in
            case Dict.get (String.dropLeft 12 optionName) knownOptions of
                Nothing ->
                    CommandLineUninteresting

                Just modifier ->
                    case String.toFloat optionValue of
                        Nothing ->
                            InvalidCommandLine <| "invalid " ++ optionName ++ " value " ++ optionValue

                        Just _ ->
                            ValidCommandLine (modifier ())

        _ ->
            CommandLineUninteresting


view : Model -> Html (Model -> Model)
view options =
    div []
        [ p [] [ text "Use the pinkIndexer algorithm. See ", viewCitation "Gevorkov, Y., Barty, A., Brehm, W., White, T. A., Tolstikova, A., Wiedorn, M. O., Meents, A., Grigat, R.-R., Chapman, H. N. & Yefanov, O. (2020). Acta Cryst. A76, 121-131." "https://doi.org/10.1107/S2053273319015559" ]
        , viewNumericInput
            "pinkIndexer-considered-peaks-count"
            options.consideredPeaksCount
            "Considered Peaks Count"
            (Just "Selects how many peaks are considered for indexing. [0-4] (veryFew to manyMany). Default: 4 (manyMany).")
            (\newValue priorModel -> { priorModel | consideredPeaksCount = newValue })
        , viewNumericInput
            "pinkIndexer-angle-resolution"
            options.angleResolution
            "Angle Resolution"
            (Just "Selects how dense the orientation angles of the sample lattice are sampled. [0-4] (extremelyLoose to extremelyDense). Default: 2 (normal).")
            (\newValue priorModel -> { priorModel | angleResolution = newValue })
        , viewNumericInput
            "pinkIndexer-refinement-type"
            options.refinementType
            "Refinement Type"
            (Just "Selects the refinement type. 0 = none, 1 = fixedLatticeParameters, 2 = variableLatticeParameters, 3 = firstFixedThenVariableLatticeParameters, 4 = firstFixedThenVariableLatticeParametersMultiSeed, 5 = firstFixedThenVariableLatticeParametersCenterAdjustmentMultiSeed. Default: 1 (fixedLatticeParameters).")
            (\newValue priorModel -> { priorModel | refinementType = newValue })
        , viewNumericInput
            "pinkIndexer-tolerance"
            options.tolerance
            "Tolerance"
            (Just "Selects the tolerance of the pinkIndexer (relative tolerance of the lattice vectors). For bad geometries or cell parameters use a high tolerance. For a well known geometry and cell use a small tolerance. Only important for refinement and indexed/not indexed identificaton. Too small tolerance will lead to refining to only a fraction of the peaks and possibly discarding of correctly indexed images. Too high tolerance will lead to bad fitting in presence of multiples or noise and can mark wrongly-indexed patterns as indexed. Default: 0.06.")
            (\newValue priorModel -> { priorModel | angleResolution = newValue })
        , viewNumericInput
            "pinkIndexer-reflection-radius"
            options.reflectionRadius
            "Reflection Radius"
            (Just "Sets radius of the reflections in reciprocal space in 1/A. Default is 2% of a* (which works quiet well for X-rays). Should be chosen much bigger for electrons (~0.002).")
            (\newValue priorModel -> { priorModel | reflectionRadius = newValue })
        , viewNumericInput
            "pinkIndexer-max-resolution-for-indexing"
            options.maxResolutionForIndexing
            "Maximum Resolution for Indexing"
            (Just "Sets the maximum resolution in 1/A used for indexing. Peaks at high resolution don't add much information, but they add a lot of computation time. Does not influence the refinement. Default: infinity.")
            (\newValue priorModel -> { priorModel | maxResolutionForIndexing = newValue })
        , viewNumericInput
            "pinkIndexer-max-refinement-disbalance"
            options.maxRefinementDisbalance
            "Maximum Refinement Disbalance"
            (Just "Indexing solutions are dismissed if the refinement refined very well to one side of the detector and very badly to the other side. Allowed values range from 0 (no disbalance) to 2 (extreme disbalance). Disbalance after refinement usually appears for bad geometries or bad prior unit cell parameters. Default: 0.4.")
            (\newValue priorModel -> { priorModel | maxRefinementDisbalance = newValue })
        ]
