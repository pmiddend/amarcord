module Amarcord.Indexing.TakeTwo exposing (Model, fromCommandLine, init, toCommandLine, view)

import Amarcord.CommandLineParser exposing (CommandLineOption(..))
import Amarcord.Indexing.Util exposing (CommandLineOptionResult(..), numberToCommandLine, viewCitation, viewNumericInput)
import Dict
import Html exposing (Html, div, p, text)
import String


type alias Model =
    { memberThreshold : String
    , lengthTolerance : String
    , angleTolerance : String
    , traceTolerance : String
    }


init : Model
init =
    { memberThreshold = ""
    , lengthTolerance = ""
    , angleTolerance = ""
    , traceTolerance = ""
    }


toCommandLine : Model -> List (Result String (List CommandLineOption))
toCommandLine model =
    [ numberToCommandLine "taketwo-member-threshold" model.memberThreshold (\n -> [ LongOption "taketwo-member-threshold" n ])
    , numberToCommandLine "taketwo-len-tolerance" model.lengthTolerance (\n -> [ LongOption "taketwo-len-tolerance" n ])
    , numberToCommandLine "taketwo-angle-tolerance" model.angleTolerance (\n -> [ LongOption "taketwo-angle-tolerance" n ])
    , numberToCommandLine "taketwo-trace-tolerance" model.traceTolerance (\n -> [ LongOption "taketwo-trace-tolerance" n ])
    ]


fromCommandLine : CommandLineOption -> Model -> CommandLineOptionResult Model
fromCommandLine option priorOptions =
    case option of
        LongOption optionName optionValue ->
            let
                knownOptions =
                    Dict.fromList
                        [ ( "member-threshold", \_ -> { priorOptions | memberThreshold = optionValue } )
                        , ( "len-tolerance", \_ -> { priorOptions | lengthTolerance = optionValue } )
                        , ( "angle-tolerance", \_ -> { priorOptions | angleTolerance = optionValue } )
                        , ( "trace-tolerance", \_ -> { priorOptions | traceTolerance = optionValue } )
                        ]
            in
            case Dict.get (String.dropLeft 8 optionName) knownOptions of
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
        [ p [] [ text "Use the TakeTwo algorithm. See ", viewCitation "Ginn, H. M., Roedig, P., Kuo, A., Evans, G., Sauter, N. K., Ernst, O. P., Meents, A., Mueller-Werkmeister, H., Miller, R. J. D. & Stuart, D. I. (2016). Acta Cryst. D72, 956-965." "https://doi.org/10.1107/S2059798316010706" ]
        , viewNumericInput
            "taketwo-member-threshold"
            felixOptions.memberThreshold
            "Member Threshold"
            (Just "Minimum number of vectors in the network before the pattern is considered. Default 20.")
            (\newValue priorModel -> { priorModel | memberThreshold = newValue })
        , viewNumericInput
            "taketwo-len-tolerance"
            felixOptions.lengthTolerance
            "Length Tolerance"
            (Just "The length tolerance in reciprocal Angstroms.  Default 0.001.")
            (\newValue priorModel -> { priorModel | lengthTolerance = newValue })
        , viewNumericInput
            "taketwo-angle-tolerance"
            felixOptions.angleTolerance
            "Angle Tolerance"
            (Just "The angle tolerance in degrees.  Default 0.6.")
            (\newValue priorModel -> { priorModel | angleTolerance = newValue })
        , viewNumericInput
            "taketwo-"
            felixOptions.traceTolerance
            "Trace Tolerance"
            (Just "The rotation matrix trace tolerance in degrees.  Default 3.")
            (\newValue priorModel -> { priorModel | traceTolerance = newValue })
        ]
