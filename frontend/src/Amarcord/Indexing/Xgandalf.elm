module Amarcord.Indexing.Xgandalf exposing (Model, fromCommandLine, init, toCommandLine, view)

import Amarcord.CommandLineParser exposing (CommandLineOption(..))
import Amarcord.Indexing.Util exposing (CommandLineOptionResult(..), boolToSwitchCommandLine, integerToCommandLine, numberToCommandLine, viewCitation, viewFormCheck, viewNumericInput)
import Dict
import Html exposing (Html, div, p, text)
import String


type alias Model =
    { samplingPitch : String
    , gradDescIterations : String
    , tolerance : String
    , noDeviationFromProvidedCell : Bool
    , minLatticeVectorLength : String
    , maxLatticeVectorLength : String
    , maxPeaks : String
    }


init : Model
init =
    { samplingPitch = ""
    , gradDescIterations = ""
    , tolerance = ""
    , noDeviationFromProvidedCell = False
    , minLatticeVectorLength = ""
    , maxLatticeVectorLength = ""
    , maxPeaks = ""
    }


toCommandLine : Model -> List (Result String (List CommandLineOption))
toCommandLine model =
    [ numberToCommandLine "xgandalf-sampling-pitch" model.samplingPitch (\n -> [ LongOption "xgandalf-sampling-pitch" n ])
    , numberToCommandLine "xgandalf-grad-desc-iterations" model.gradDescIterations (\n -> [ LongOption "xgandalf-grad-desc-iterations" n ])
    , numberToCommandLine "xgandalf-tolerance" model.tolerance (\n -> [ LongOption "xgandalf-tolerance" n ])
    , boolToSwitchCommandLine "xgandalf-no-deviation-from-provided-cell" model.noDeviationFromProvidedCell
    , numberToCommandLine "xgandalf-min-lattice-vector-length" model.minLatticeVectorLength (\n -> [ LongOption "xgandalf-min-lattice-vector-length" n ])
    , numberToCommandLine "xgandalf-max-lattice-vector-length" model.maxLatticeVectorLength (\n -> [ LongOption "xgandalf-max-lattice-vector-length" n ])
    , integerToCommandLine "xgandalf-max-peaks" model.maxPeaks (\n -> [ LongOption "xgandalf-max-peaks" n ])
    ]


fromCommandLine : CommandLineOption -> Model -> CommandLineOptionResult Model
fromCommandLine option priorOptions =
    case option of
        LongOption optionName optionValue ->
            let
                knownOptions =
                    Dict.fromList
                        [ ( "sampling-pitch", \_ -> { priorOptions | samplingPitch = optionValue } )
                        , ( "grad-desc-iterations", \_ -> { priorOptions | gradDescIterations = optionValue } )
                        , ( "tolerance", \_ -> { priorOptions | tolerance = optionValue } )
                        , ( "min-lattice-vector-length", \_ -> { priorOptions | minLatticeVectorLength = optionValue } )
                        , ( "max-lattice-vector-length", \_ -> { priorOptions | maxLatticeVectorLength = optionValue } )
                        , ( "max-peaks", \_ -> { priorOptions | maxPeaks = optionValue } )
                        ]
            in
            case Dict.get (String.dropLeft 9 optionName) knownOptions of
                Nothing ->
                    CommandLineUninteresting

                Just modifier ->
                    case String.toFloat optionValue of
                        Nothing ->
                            InvalidCommandLine <| "invalid " ++ optionName ++ " value " ++ optionValue

                        Just _ ->
                            ValidCommandLine (modifier ())

        LongSwitch "xgandalf-no-deviation-from-provided-cell" ->
            ValidCommandLine { priorOptions | noDeviationFromProvidedCell = True }

        _ ->
            CommandLineUninteresting


view : Model -> Html (Model -> Model)
view options =
    div []
        [ p [] [ text "Use the eXtended GrAdieNt Descent Algorithm for Lattice Finding. See ", viewCitation "Gevorkov, Y., Yefanov, O., Barty, A., White, T. A., Mariani, V., Brehm, W., Tolstikova, A., Grigat, R.-R. & Chapman, H. N. (2019). Acta Cryst. A75, 694-704." "https://doi.org/10.1107/S2053273319010593" ]
        , viewNumericInput
            "xgandalf-sampling-pitch"
            options.samplingPitch
            "Sampling Pitch"
            (Just "Selects how dense the reciprocal space is sampled. 0-4: extremelyLoose to extremelyDense. 5-7: standardWithSecondaryMillerIndices to extremelyDenseWithSecondaryMillerIndices. Default 6 (denseWithSecondaryMillerIndices).")
            (\newValue priorModel -> { priorModel | samplingPitch = newValue })
        , viewNumericInput
            "xgandalf-grad-desc-iterations"
            options.gradDescIterations
            "Gradient Descent Iterations"
            (Just "Selects how many gradient descent iterations are performed. 0-5: veryFew to extremelyMany. Default is 4 (manyMany).")
            (\newValue priorModel -> { priorModel | gradDescIterations = newValue })
        , viewNumericInput
            "xgandalf-tolerance"
            options.tolerance
            "Tolerance"
            (Just "Relative tolerance of the lattice vectors. Default 0.02.")
            (\newValue priorModel -> { priorModel | tolerance = newValue })
        , viewNumericInput
            "xgandalf-min-lattice-vector-length"
            options.minLatticeVectorLength
            "Min. Lattice Vector Length"
            (Just "Minimum possible lattice vector length in Angstrom. Used for fitting without prior lattice as starting point for gradient descent, so the final minimum lattice vector length can be smaller/bigger than min/max. Note: This is valid for the uncentered cell, i.e. the P-cell! Default: 30A and 250A respectively.")
            (\newValue priorModel -> { priorModel | minLatticeVectorLength = newValue })
        , viewNumericInput
            "xgandalf-max-lattice-vector-length"
            options.maxLatticeVectorLength
            "Max. Lattice Vector Length"
            (Just "Maximum possible lattice vector length in Angstrom. Used for fitting without prior lattice as starting point for gradient descent, so the final maximum lattice vector length can be smaller/bigger than max/max. Note: This is valid for the uncentered cell, i.e. the P-cell! Default: 30A and 250A respectively.")
            (\newValue priorModel -> { priorModel | maxLatticeVectorLength = newValue })
        , viewNumericInput
            "xgandalf-max-peaks"
            options.maxPeaks
            "Maximum number of Peaks"
            (Just "Maximum number of peaks used for indexing. For refinement all peaks are used. Peaks are selected by increasing radius. Limits the maximum execution time for patterns with a huge amount of peaks - either real ones or false positives. Default: 250.")
            (\newValue priorModel -> { priorModel | maxPeaks = newValue })
        , viewFormCheck
            "xgandalf-no-deviation-from-provided-cell"
            "No deviation from provided cell"
            (Just (text "If a prior unit cell was provided, and this flag is set, the found unit cell will have exactly the same size as the provided one."))
            options.noDeviationFromProvidedCell
            (\_ m -> { m | noDeviationFromProvidedCell = not m.noDeviationFromProvidedCell })
        ]
