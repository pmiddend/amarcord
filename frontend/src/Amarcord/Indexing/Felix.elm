module Amarcord.Indexing.Felix exposing (Model, fromCommandLine, init, toCommandLine, view)

import Amarcord.CommandLineParser exposing (CommandLineOption(..))
import Amarcord.Indexing.Util exposing (CommandLineOptionResult(..), integerToCommandLine, numberToCommandLine, viewCitation, viewNumericInput)
import Dict
import Html exposing (Html, div, p, text)
import String


type alias Model =
    { domega : String
    , fractionMaxVisits : String
    , maxInternalAngle : String
    , maxUniqueness : String
    , minCompleteness : String
    , minVisits : String
    , numVoxels : String
    , sigma : String
    , tthrangeMax : String
    , tthrangeMin : String
    }


init : Model
init =
    { domega = ""
    , fractionMaxVisits = ""
    , maxInternalAngle = ""
    , maxUniqueness = ""
    , minCompleteness = ""
    , minVisits = ""
    , numVoxels = ""
    , sigma = ""
    , tthrangeMax = ""
    , tthrangeMin = ""
    }


toCommandLine : Model -> List (Result String (List CommandLineOption))
toCommandLine model =
    [ numberToCommandLine "domega" model.domega (\n -> [ LongOption "felix-domega" n ])
    , numberToCommandLine "fraction-max-visits" model.fractionMaxVisits (\n -> [ LongOption "felix-fraction-max-visits" n ])
    , numberToCommandLine "max-internal-angle" model.maxInternalAngle (\n -> [ LongOption "felix-max-internal-angle" n ])
    , numberToCommandLine "max-uniqueness" model.maxUniqueness (\n -> [ LongOption "felix-max-uniqueness" n ])
    , numberToCommandLine "min-completeness" model.minCompleteness (\n -> [ LongOption "felix-min-completeness" n ])
    , integerToCommandLine "min-visits" model.minVisits (\n -> [ LongOption "felix-min-visits" n ])
    , integerToCommandLine "num-voxels" model.numVoxels (\n -> [ LongOption "felix-num-voxels" n ])
    , numberToCommandLine "sigma" model.sigma (\n -> [ LongOption "felix-sigma" n ])
    , numberToCommandLine "tthrangeMax" model.tthrangeMax (\n -> [ LongOption "felix-tthrange-max" n ])
    , numberToCommandLine "tthrangeMin" model.tthrangeMin (\n -> [ LongOption "felix-tthrange-min" n ])
    ]


fromCommandLine : CommandLineOption -> Model -> CommandLineOptionResult Model
fromCommandLine option priorFelixOptions =
    case option of
        LongOption optionName optionValue ->
            let
                knownOptions =
                    Dict.fromList
                        [ ( "domega", \_ -> { priorFelixOptions | domega = optionValue } )
                        , ( "fraction-max-visits", \_ -> { priorFelixOptions | fractionMaxVisits = optionValue } )
                        , ( "max-internal-angle", \_ -> { priorFelixOptions | maxInternalAngle = optionValue } )
                        , ( "max-uniqueness", \_ -> { priorFelixOptions | maxUniqueness = optionValue } )
                        , ( "min-completeness", \_ -> { priorFelixOptions | minCompleteness = optionValue } )
                        , ( "min-visits", \_ -> { priorFelixOptions | minVisits = optionValue } )
                        , ( "num-voxels", \_ -> { priorFelixOptions | numVoxels = optionValue } )
                        , ( "sigma", \_ -> { priorFelixOptions | sigma = optionValue } )
                        , ( "tthrange-max", \_ -> { priorFelixOptions | tthrangeMax = optionValue } )
                        , ( "tthrange-min", \_ -> { priorFelixOptions | tthrangeMin = optionValue } )
                        ]
            in
            case Dict.get (String.dropLeft 6 optionName) knownOptions of
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
view felixOptions =
    div []
        [ p [] [ text "Invoke Felix, which will use your cell parameters to find multiple crystals in each pattern. See ", viewCitation "Beyerlein, K. R., White, T. A., Yefanov, O., Gati, C., Kazantsev, I. G., Nielsen, N. F.-G., Larsen, P. M., Chapman, H. N. & Schmidt, S. (2017). J. Appl. Cryst. 50, 1075-1083." "https://doi.org/10.1107/s1600576717007506" ]
        , viewNumericInput
            "felix-domega"
            felixOptions.domega
            "Δω"
            (Just "Degree range of omega (moscaicity) to consider. Default 2.")
            (\newValue priorModel -> { priorModel | domega = newValue })
        , viewNumericInput
            "felix-fraction-max-visits"
            felixOptions.fractionMaxVisits
            "Fraction of Vₘₐₓ (max visits)"
            (Just "Cutoff for minimum fraction of the max visits. Default: 0.75.")
            (\newValue priorModel -> { priorModel | fractionMaxVisits = newValue })
        , viewNumericInput
            "felix-max-internal-angle"
            felixOptions.maxInternalAngle
            "Maximum internal angle"
            (Just "Cutoff for maximum internal angle between observed spots and predicted spots. Default: 0.25")
            (\newValue priorModel -> { priorModel | maxInternalAngle = newValue })
        , viewNumericInput
            "felix-max-uniqueness"
            felixOptions.maxUniqueness
            "Maximum uniqueness"
            (Just "Cutoff for maximum fraction of found spots which can belong to other crystallites. Default: 0.5")
            (\newValue priorModel -> { priorModel | maxUniqueness = newValue })
        , viewNumericInput
            "felix-min-completeness"
            felixOptions.minCompleteness
            "Minimum completeness"
            (Just "Cutoff for minimum fraction of projected spots found in the pattern. Default: 0.001")
            (\newValue priorModel -> { priorModel | minCompleteness = newValue })
        , viewNumericInput
            "felix-min-visits"
            felixOptions.minVisits
            "Minimum visits"
            (Just "Cutoff for minimum number of voxel visits. Default: 15")
            (\newValue priorModel -> { priorModel | minVisits = newValue })
        , viewNumericInput
            "felix-num-voxels"
            felixOptions.numVoxels
            "Number of voxels"
            (Just "Number of voxels for Rodrigues space search Default: 100.")
            (\newValue priorModel -> { priorModel | numVoxels = newValue })
        , viewNumericInput
            "felix-sigma"
            felixOptions.sigma
            "σ"
            (Just "The sigma of the 2θ, η and ω angles. Default: 0.2.")
            (\newValue priorModel -> { priorModel | sigma = newValue })
        , viewNumericInput
            "felix-tthrange-max"
            felixOptions.tthrangeMax
            "2θ range max"
            (Just "Maximum 2θ to consider for indexing (degrees) Default: 30°.")
            (\newValue priorModel -> { priorModel | tthrangeMax = newValue })
        , viewNumericInput
            "felix-tthrange-min"
            felixOptions.tthrangeMin
            "2θ range min"
            (Just "Minimum 2θ to consider for indexing (degrees) Default: 30°.")
            (\newValue priorModel -> { priorModel | tthrangeMin = newValue })
        ]
