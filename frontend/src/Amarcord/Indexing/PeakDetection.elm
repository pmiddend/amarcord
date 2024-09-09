module Amarcord.Indexing.PeakDetection exposing (Model, fromCommandLine, init, toCommandLine, view)

import Amarcord.CommandLineParser exposing (CommandLineOption(..))
import Amarcord.Html exposing (br_, div_, li_, span_)
import Amarcord.Indexing.Util exposing (CommandLineOptionResult(..), commaSeparatedNumbersToCommandLine, integerToCommandLine, numberToCommandLine, viewCitation, viewFormCheck, viewNumericInput)
import Dict
import Html exposing (Html, div, label, ol, option, p, select, text)
import Html.Attributes exposing (class, for, id, selected, value)
import Html.Events exposing (onInput)
import List.Extra
import Maybe.Extra


type alias Model =
    { threshold : String
    , minSquaredGradient : String
    , peakRadius : String
    , minSnr : String
    , minPixCount : String
    , maxPixCount : String
    , localBgRadius : String
    , minRes : String
    , maxRes : String
    , peakfinder8Fast : Bool
    , minSnrBiggestPix : String
    , minSnrPeakPix : String
    , minSig : String
    , minPeaks : String
    , peakDetector : Maybe String
    }


init : Model
init =
    { threshold = ""
    , minSquaredGradient = ""
    , minSnr = ""
    , peakRadius = ""
    , minPixCount = ""
    , maxPixCount = ""
    , localBgRadius = ""
    , minRes = ""
    , maxRes = ""
    , minSnrBiggestPix = ""
    , minSnrPeakPix = ""
    , minSig = ""
    , minPeaks = "0"
    , peakfinder8Fast = False
    , peakDetector = Nothing
    }


viewNumericPdInput : String -> String -> String -> Maybe String -> (String -> Model -> Model) -> Html (Model -> Model)
viewNumericPdInput inputId currentValue description optionalDetails changer =
    viewNumericInput
        inputId
        currentValue
        description
        optionalDetails
        changer


viewFormCheckPd : String -> String -> Maybe (Html (a -> b)) -> Bool -> (a -> b) -> Html (a -> b)
viewFormCheckPd elementId description details checkedValue f =
    viewFormCheck
        elementId
        description
        details
        checkedValue
        (\_ -> f)


viewMinPeaks : Model -> Html (Model -> Model)
viewMinPeaks model =
    viewNumericPdInput "min-peaks" model.minPeaks "Skip frames with fewer than peaks" (Just "Do not try to index frames with fewer than this peaks. These frames will still be described in the output stream. Leave at 0 to have all frames be considered hits, even if they have no peaks at all.") (\newValue m -> { m | minPeaks = newValue })


viewThresholdInput : Model -> Html (Model -> Model)
viewThresholdInput model =
    viewNumericPdInput "threshold"
        model.threshold
        "Threshold"
        (Just "The threshold has arbitrary units matching the pixel values in the data.")
        (\newValue ip -> { ip | threshold = newValue })


viewMinSnrInput : Model -> Html (Model -> Model)
viewMinSnrInput model =
    viewNumericPdInput
        "min-snr"
        model.minSnr
        "Minimum signal/noise ratio"
        Nothing
        (\newValue ip -> { ip | minSnr = newValue })


viewZaefDetails : Model -> Html (Model -> Model)
viewZaefDetails model =
    div_
        [ p [ class "mb-3 mt-3" ]
            [ text "zaef is a simple gradient search after "
            , viewCitation "Zaefferer, S. (2000). J. Appl. Cryst. 33, 10-25." "https://doi.org/10.1107/S0021889899010894"
            , text "."
            , br_
            , text <|
                "You can control the overall threshold and minimum squared gradient for finding a peak. "
                    ++ "Peaks will be rejected if the “foot point” is further away from the “summit” "
                    ++ "of the peak by more than the inner integration radius. They will also be rejected "
                    ++ "if the peak is closer than twice the inner integration radius from another peak."
            ]
        , div [ class "row" ]
            [ div [ class "col-6" ]
                [ viewThresholdInput model
                ]
            , div [ class "col-6" ]
                [ viewNumericPdInput
                    "min-squared-gradient"
                    model.minSquaredGradient
                    "Minimum squared gradient"
                    (Just "The minimum gradient also has arbitrary (squared) units matching the pixel values.")
                    (\newValue ip -> { ip | minSquaredGradient = newValue })
                ]
            ]
        , div [ class "row" ]
            [ div [ class "col-6" ]
                [ viewMinSnrInput model
                ]
            , div [ class "col-6" ]
                [ div [ class "form-floating mb-3" ]
                    [ viewNumericPdInput
                        "peak-radius"
                        model.peakRadius
                        "Peak radius (inner, middle, outer)"
                        (Just "Specify three numbers, comma-separated. These are the inner, middle and outer radii for three-ring integration during the peak search.")
                        (\newValue ip -> { ip | peakRadius = newValue })
                    ]
                ]
            ]
        ]


viewPeakfinder8Details : Model -> Html (Model -> Model)
viewPeakfinder8Details model =
    div_
        [ p [ class "mb-3 mt-3" ]
            [ text "The “peakfinder8” algorithm is described in "
            , viewCitation "Barty, A., Kirian, R. A., Maia, F. R. N. C., Hantke, M., Yoon, C. H., White, T. A. & Chapman, H. (2014). J. Appl. Cryst. 47, 1118-1131." "https://doi.org/10.1107/S1600576714007626"
            , text "."
            , br_
            , text "Pixels above a radius-dependent intensity threshold are considered as candidate peaks (although the user sets an absolute minimum threshold for candidate peaks). Peaks are then only accepted if their signal to noise level over the local background is sufficiently high. Peaks can include multiple pixels and the user can reject a peak if it includes too many or too few. The distance of a peak from the center of the detector can also be used as a filtering criterion. Note that the peakfinder8 will not report more than 2048 peaks for each panel: any additional peak is ignored."
            ]
        , div [ class "row" ]
            [ div [ class "col-6" ]
                [ viewThresholdInput model
                ]
            , div [ class "col-6" ]
                [ viewMinSnrInput model
                ]
            ]
        , div [ class "row" ]
            [ div [ class "col-6" ]
                [ viewNumericPdInput "min-pix-count"
                    model.minPixCount
                    "Minimum number of pixels"
                    Nothing
                    (\newValue ip -> { ip | minPixCount = newValue })
                ]
            , div [ class "col-6" ]
                [ viewNumericPdInput "max-pix-count"
                    model.maxPixCount
                    "Maximum number of pixels"
                    Nothing
                    (\newValue ip -> { ip | maxPixCount = newValue })
                ]
            , div [ class "row" ]
                [ div [ class "col-6" ]
                    [ viewNumericPdInput "local-bg-radius"
                        model.localBgRadius
                        "Local background radius"
                        Nothing
                        (\newValue ip -> { ip | localBgRadius = newValue })
                    ]
                , div [ class "col-6" ]
                    [ viewFormCheckPd
                        "peakfinder8-fast"
                        "Fast"
                        (Just (span_ [ text "Increase speed by restricting the number of sampling points used for the background statistics calculation." ]))
                        model.peakfinder8Fast
                        (\m -> { m | peakfinder8Fast = not m.peakfinder8Fast })
                    ]
                ]
            ]
        , div [ class "row" ]
            [ div [ class "col-6" ]
                [ viewNumericPdInput "min-res"
                    model.minRes
                    "Minimum resolution"
                    (Just "This is in pixels, so it must be an integer.")
                    (\newValue ip -> { ip | minRes = newValue })
                ]
            , div [ class "col-6" ]
                [ viewNumericPdInput "max-res"
                    model.maxRes
                    "Maximum resolution"
                    (Just "This is in pixels, so it must be an integer.")
                    (\newValue ip -> { ip | maxRes = newValue })
                ]
            ]
        ]


viewPeakfinder9Details : Model -> Html (Model -> Model)
viewPeakfinder9Details model =
    div_
        [ p [ class "mt-3" ]
            [ text "The “peakfinder9” algorithm is described in "
            , viewCitation "Gevorkov, Yaroslav [P:(DE-H253)PIP1027420]" "https://doi.org/10.3204/PUBDB-2022-03562"
            , text "."
            ]
        , p []
            [ text <|
                "Unlike peakfinder8, peakfinder9 uses local background estimation based on border pixels in a "
                    ++ "specified radius (see parameter “Local background radius”). "
                    ++ "For being fast and precise, a hierarchy of conditions is used:"
            ]
        , ol []
            [ li_
                [ text "A pixel that is the biggest pixel in a peak must be larger than every border pixel by a constant value. This condition is only useful for speed considerations and is omitted here."
                ]
            , li_ [ text "Ensure that peaks rise monotonically towards the biggest pixel. " ]
            , li_ [ text "Ensure that the biggest pixel in the peak is significantly over the noise level (see parameter “Minimum signal/noise ratio of brightest pixel in peak”) by computing the local statistics from the border pixels in a specified radius." ]
            , li_ [ text "Sum up all pixels belonging to the peak (see parameter “Minimum signal/noise ratio of peak pixel”) and demand that the whole peak must be significantly over the noise level (see parameter “Minimum signal/noise radio”)." ]
            ]
        , p [] [ text "Only if all conditions are passed, the peak is accepted." ]
        , div [ class "row" ]
            [ div [ class "col-6" ]
                [ viewNumericPdInput "min-snr-biggest-pix"
                    model.minSnrBiggestPix
                    "Minimum signal/noise ratio of brightest pixel in peak"
                    Nothing
                    (\newValue ip -> { ip | minSnrBiggestPix = newValue })
                ]
            , div [ class "col-6" ]
                [ viewNumericPdInput "min-snr-peak-pix"
                    model.minSnrPeakPix
                    "Minimum signal/noise ratio of peak pixel"
                    Nothing
                    (\newValue ip -> { ip | minSnrPeakPix = newValue })
                ]
            ]
        , div [ class "row" ]
            [ div [ class "col-6" ]
                [ viewMinSnrInput model
                ]
            , div [ class "col-6" ]
                [ viewNumericPdInput "min-sig"
                    model.minSig
                    "Minimum background standard deviation"
                    Nothing
                    (\newValue ip -> { ip | minSig = newValue })
                ]
            , div [ class "row" ]
                [ div [ class "col-6" ]
                    [ viewNumericPdInput "local-bg-radius"
                        model.localBgRadius
                        "Local background radius"
                        Nothing
                        (\newValue ip -> { ip | localBgRadius = newValue })
                    ]
                ]
            ]
        ]


type alias PeakDetectorMetadata =
    { param : String, longName : String }


knownPeakDetectors : List PeakDetectorMetadata
knownPeakDetectors =
    [ PeakDetectorMetadata "zaef" "Zaefferer gradient search"
    , PeakDetectorMetadata "peakfinder8" "Radial Background Estimation"
    , PeakDetectorMetadata "peakfinder9" "Local Background Estimation"
    ]


viewPeakDetectionDetails : Model -> String -> Html (Model -> Model)
viewPeakDetectionDetails model peakDetector =
    case peakDetector of
        "zaef" ->
            viewZaefDetails model

        "peakfinder8" ->
            viewPeakfinder8Details model

        "peakfinder9" ->
            viewPeakfinder9Details model

        _ ->
            text ""


view : Model -> Html (Model -> Model)
view model =
    div [ class "mb-3" ]
        [ div [ class "form-floating" ]
            [ select
                [ class "form-select mb-1"
                , id "peak-detector"
                , onInput
                    (\newParam _ ->
                        let
                            initialModel =
                                init
                        in
                        { initialModel
                            | peakDetector =
                                if newParam == "unset" then
                                    Nothing

                                else
                                    Just newParam
                        }
                    )
                ]
                (option
                    [ value "unset"
                    , selected (Maybe.Extra.isNothing model.peakDetector)
                    ]
                    [ text "Use default" ]
                    :: List.map
                        (\{ param, longName } ->
                            option [ value param, selected (model.peakDetector == Just param) ]
                                [ text (longName ++ " (" ++ param ++ ")")
                                ]
                        )
                        knownPeakDetectors
                )
            , label [ for "peak-detector" ] [ text "Peak detection method" ]
            , case model.peakDetector of
                Nothing ->
                    text ""

                Just peakDetector ->
                    viewPeakDetectionDetails model peakDetector
            , viewMinPeaks model
            ]
        ]


fromCommandLine : CommandLineOption -> Model -> CommandLineOptionResult Model
fromCommandLine option model =
    case option of
        LongOption "peaks" peakDetector ->
            case List.Extra.find (\pf -> pf.param == peakDetector) knownPeakDetectors of
                Nothing ->
                    InvalidCommandLine ("invalid peak detector: " ++ peakDetector)

                Just { param } ->
                    ValidCommandLine { model | peakDetector = Just param }

        LongOption optionName optionValue ->
            let
                knownOptions : Dict.Dict String (Model -> String -> Model)
                knownOptions =
                    Dict.fromList
                        [ ( "threshold", \pd sv -> { pd | threshold = sv } )
                        , ( "min-squared-gradient", \pd sv -> { pd | minSquaredGradient = sv } )
                        , ( "min-snr", \pd sv -> { pd | minSnr = sv } )
                        , ( "peak-radius", \pd sv -> { pd | peakRadius = sv } )
                        , ( "min-pix-count", \pd sv -> { pd | minPixCount = sv } )
                        , ( "max-pix-count", \pd sv -> { pd | maxPixCount = sv } )
                        , ( "local-bg-radius", \pd sv -> { pd | localBgRadius = sv } )
                        , ( "min-res", \pd sv -> { pd | minRes = sv } )
                        , ( "max-res", \pd sv -> { pd | maxRes = sv } )
                        , ( "min-snr-biggest-pix", \pd sv -> { pd | minSnrBiggestPix = sv } )
                        , ( "min-snr-peak-pix", \pd sv -> { pd | minSnrPeakPix = sv } )
                        , ( "min-sig", \pd sv -> { pd | minSig = sv } )
                        , ( "min-peaks", \pd sv -> { pd | minPeaks = sv } )
                        ]
            in
            case Dict.get optionName knownOptions of
                Just processor ->
                    ValidCommandLine (processor model optionValue)

                Nothing ->
                    CommandLineUninteresting

        LongSwitch optionName ->
            let
                knownOptions =
                    Dict.fromList
                        [ ( "peakfinder8-fast", \pd -> { pd | peakfinder8Fast = True } )
                        ]
            in
            case Dict.get optionName knownOptions of
                Just processor ->
                    ValidCommandLine (processor model)

                Nothing ->
                    CommandLineUninteresting

        _ ->
            CommandLineUninteresting


peakDetectorToCommandLine : Maybe String -> Result String (List CommandLineOption)
peakDetectorToCommandLine x =
    case x of
        Nothing ->
            Ok []

        Just pd ->
            Ok [ LongOption "peaks" pd ]


toCommandLine : Model -> List (Result String (List CommandLineOption))
toCommandLine { peakDetector, threshold, minSquaredGradient, minSnr, minPixCount, maxPixCount, peakRadius, localBgRadius, minRes, maxRes, minSnrBiggestPix, minSnrPeakPix, minSig } =
    [ peakDetectorToCommandLine peakDetector
    , numberToCommandLine "Threshold" threshold (\n -> [ LongOption "threshold" n ])
    , numberToCommandLine "Minimum squared gradient" minSquaredGradient (\n -> [ LongOption "min-squared-gradient" n ])
    , numberToCommandLine "Minimum signal/noise ratio" minSnr (\n -> [ LongOption "min-snr" n ])
    , numberToCommandLine "Minimum number of pixels" minPixCount (\n -> [ LongOption "min-pix-count" n ])
    , numberToCommandLine "Maximum number of pixels" maxPixCount (\n -> [ LongOption "max-pix-count" n ])
    , commaSeparatedNumbersToCommandLine "peak-radius" peakRadius 3
    , numberToCommandLine "Local background radius" localBgRadius (\n -> [ LongOption "local-bg-radius" n ])
    , integerToCommandLine "Minimum resolution" minRes (\n -> [ LongOption "min-res" n ])
    , integerToCommandLine "Maximum resolution" maxRes (\n -> [ LongOption "max-res" n ])
    , numberToCommandLine "Minimum signal/noise ratio of brightest pixel in peak" minSnrBiggestPix (\n -> [ LongOption "min-snr-biggest-pix" n ])
    , numberToCommandLine "Minimum signal/noise ratio of peak pixel" minSnrPeakPix (\n -> [ LongOption "min-snr-peak-pix" n ])
    , numberToCommandLine "Minimum background standard deviation" minSig (\n -> [ LongOption "min-sig" n ])
    ]
