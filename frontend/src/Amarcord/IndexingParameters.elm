module Amarcord.IndexingParameters exposing (Model, Msg(..), convertCommandLineToModel, init, isEditOpen, toCommandLine, update, view)

import Amarcord.Bootstrap exposing (AlertProperty(..), viewAlert)
import Amarcord.CommandLineParser exposing (CommandLineOption(..), coparseCommandLine, coparseOption, parseCommandLine)
import Amarcord.Html exposing (br_, code_, div_, em_, form_, h5_, input_, li_, p_, span_, strongText, tbody_, td_, th_, thead_, tr_, ul_)
import Amarcord.Util exposing (collectResults, join3)
import Dict exposing (Dict)
import Html exposing (Html, a, button, dd, div, dl, dt, label, li, ol, option, p, select, span, table, td, text, textarea, ul)
import Html.Attributes exposing (checked, class, disabled, for, href, id, selected, style, type_, value)
import Html.Events exposing (onClick, onInput)
import List
import List.Extra
import Maybe.Extra
import Parser exposing (deadEndsToString)
import Result.Extra


knownIndexingMethods : List String
knownIndexingMethods =
    [ "dirax", "mosflm", "asdf", "felix", "xds", "taketwo", "xgandalf", "pinkIndexer" ]


type alias IndexingMethod =
    { methodName : String
    , enabled : Bool
    , latticeInformation : Bool
    , cellInformation : Bool
    }


type alias Tolerances a =
    { toleranceAPercent : a
    , toleranceBPercent : a
    , toleranceCPercent : a
    , toleranceAlphaDegrees : a
    , toleranceBetaDegrees : a
    , toleranceGammaDegrees : a
    }


defaultTolerances : Tolerances String
defaultTolerances =
    { toleranceAPercent = "5.0"
    , toleranceBPercent = "5.0"
    , toleranceCPercent = "5.0"
    , toleranceAlphaDegrees = "1.5"
    , toleranceBetaDegrees = "1.5"
    , toleranceGammaDegrees = "1.5"
    }


type alias IntRadii a =
    { inner : a
    , middle : a
    , outer : a
    }


defaultRadii : IntRadii String
defaultRadii =
    IntRadii "4" "5" "7"


radiiToString : IntRadii Float -> String
radiiToString { inner, middle, outer } =
    String.fromFloat inner ++ "," ++ String.fromFloat middle ++ "," ++ String.fromFloat outer


andThenRadii : IntRadii a -> (a -> Result String b) -> Result String (IntRadii b)
andThenRadii { inner, middle, outer } f =
    f inner
        |> Result.andThen
            (\mappedInner ->
                f middle
                    |> Result.andThen (\mappedMiddle -> f outer |> Result.map (\mappedOuter -> { inner = mappedInner, middle = mappedMiddle, outer = mappedOuter }))
            )


type TabType
    = PeakDetection
    | Indexing
    | Integration
    | Misc


tabTypeToString : TabType -> String
tabTypeToString x =
    case x of
        PeakDetection ->
            "peak-detection"

        Indexing ->
            "indexing"

        Integration ->
            "integration"

        Misc ->
            "misc"


type IntegrationDiag a
    = None
    | All
    | Random
    | Implausible
    | Negative
    | Strong
    | Indices a


mapIntegrationDiag : (a -> b) -> IntegrationDiag a -> IntegrationDiag b
mapIntegrationDiag f x =
    case x of
        Indices a ->
            Indices (f a)

        None ->
            None

        All ->
            All

        Random ->
            Random

        Implausible ->
            Implausible

        Negative ->
            Negative

        Strong ->
            Strong


intDiagToCommandLine : IntegrationDiag String -> Result String (List CommandLineOption)
intDiagToCommandLine x =
    let
        resolved : Result String (List String)
        resolved =
            case x of
                None ->
                    Ok []

                All ->
                    Ok [ "all" ]

                Random ->
                    Ok [ "random" ]

                Implausible ->
                    Ok [ "implausible" ]

                Negative ->
                    Ok [ "negative" ]

                Strong ->
                    Ok [ "strong" ]

                Indices indicesAsStr ->
                    case parseMillerIndices indicesAsStr of
                        Nothing ->
                            Err ("not a valid list of indices (3 integers): " ++ indicesAsStr)

                        Just ( h, k, l ) ->
                            Ok [ String.fromInt h ++ "," ++ String.fromInt k ++ "," ++ String.fromInt l ]
    in
    Result.map (List.map (LongOption "int-diag")) resolved


parseMillerIndices : String -> Maybe ( Int, Int, Int )
parseMillerIndices indicesAsString =
    case String.split "," indicesAsString of
        [ h, k, l ] ->
            Maybe.map3 (\hParsed kParsed lParsed -> ( hParsed, kParsed, lParsed )) (String.toInt h) (String.toInt k) (String.toInt l)

        _ ->
            Nothing


isEditOpen : Model -> Bool
isEditOpen { commandLineEdit } =
    Maybe.Extra.isJust commandLineEdit


commandLineToIntDiag : String -> Result String (IntegrationDiag ( Int, Int, Int ))
commandLineToIntDiag x =
    case x of
        "none" ->
            Ok None

        "all" ->
            Ok All

        "random" ->
            Ok Random

        "implausible" ->
            Ok Implausible

        "negative" ->
            Ok Negative

        "strong" ->
            Ok Strong

        indicesAsString ->
            case parseMillerIndices indicesAsString of
                Nothing ->
                    Err ("not a valid list of indices (3 integers): " ++ indicesAsString)

                Just v ->
                    Ok (Indices v)


type alias PeakDetectionModel =
    { -- Numbers
      threshold : String
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
    }


type alias Model =
    { openTab : TabType
    , peakDetection : PeakDetectionModel
    , sources : List String

    -- for online indexing, the cell description is inferred, so we don't want the user to be able to change it
    , mutableCellDescription : Bool

    -- Specials
    , source : String
    , geometryFile : String
    , cellDescription : String
    , peakDetector : Maybe String
    , integrator : Maybe String
    , fixProfileRadius : Maybe String
    , fixDivergence : Maybe String
    , indexingMethods : Maybe (Dict String IndexingMethod)
    , indexingChooserOpen : String
    , customTolerances : Maybe (Tolerances String)
    , intRadii : Maybe (IntRadii String)
    , commandLineEdit : Maybe String
    , commandLineEditError : Maybe String
    , pushRes : Maybe String
    , integrationCenter : Bool
    , integrationSat : Bool
    , integrationGrad : Bool
    , integrationDiag : IntegrationDiag String

    -- Options
    , mille : Bool
    , multi : Bool
    , cellParametersOnly : Bool
    , asdfFast : Bool
    , noRefine : Bool
    , noRetry : Bool
    , noCheckPeaks : Bool
    , noCheckCell : Bool
    , overpredict : Bool
    , profile : Bool
    }


type Msg
    = Change (Model -> Model)
    | ToggleIndexingMethods
    | ToggleAllIndexingMethods Bool
    | ChangeOpenTab TabType
    | ChangeIndexingMethod String (IndexingMethod -> IndexingMethod)
    | ChangeIndexingChooserOpen String
    | StartCommandLineEdit
    | CancelCommandLineEdit
    | FinishCommandLineEdit String


indexingMethodsToCommandLine : List IndexingMethod -> Result String (List CommandLineOption)
indexingMethodsToCommandLine methods =
    let
        singleMethodToString { methodName, enabled, latticeInformation, cellInformation } =
            if not enabled then
                Nothing

            else
                Just
                    (methodName
                        ++ (if cellInformation then
                                "-cell"

                            else
                                "-nocell"
                           )
                        ++ (if latticeInformation then
                                "-latt"

                            else
                                "-nolatt"
                           )
                    )
    in
    Ok [ LongOption "indexing" <| String.join "," (List.filterMap singleMethodToString methods) ]


peakDetectorToCommandLine : Maybe String -> Result String (List CommandLineOption)
peakDetectorToCommandLine x =
    case x of
        Nothing ->
            Ok []

        Just pd ->
            Ok [ LongOption "peaks" pd ]


boolToSwitchCommandLine : String -> Bool -> Result error (List CommandLineOption)
boolToSwitchCommandLine switch value =
    if not value then
        Ok []

    else
        Ok [ LongSwitch switch ]


stringToFloatResult : String -> String -> Result String Float
stringToFloatResult description input =
    case String.toFloat input of
        Nothing ->
            Err (description ++ ": not a number")

        Just result ->
            Ok result


numberToCommandLine : String -> String -> (String -> List CommandLineOption) -> Result String (List CommandLineOption)
numberToCommandLine optionName numberString converter =
    if String.isEmpty numberString then
        Ok []

    else
        case String.toFloat numberString of
            Nothing ->
                Err (optionName ++ ": not a number: " ++ numberString)

            Just _ ->
                Ok (converter numberString)


integerToCommandLine : String -> String -> (String -> List CommandLineOption) -> Result String (List CommandLineOption)
integerToCommandLine optionName numberString converter =
    if String.isEmpty numberString then
        Ok []

    else
        case String.toInt numberString of
            Nothing ->
                Err (optionName ++ ": not an integer: " ++ numberString)

            Just _ ->
                Ok (converter numberString)


type alias PeakDetectorMetadata =
    { param : String, longName : String }


knownPeakDetectors : List PeakDetectorMetadata
knownPeakDetectors =
    [ PeakDetectorMetadata "zaef" "Zaefferer gradient search"
    , PeakDetectorMetadata "peakfinder8" "Radial Background Estimation"
    , PeakDetectorMetadata "peakfinder9" "Local Background Estimation"
    ]


type alias IntegratorMetadata =
    { param : String, longName : String }


knownIntegrators : List IntegratorMetadata
knownIntegrators =
    [ IntegratorMetadata "none" "No integration (only spot prediction)"
    , IntegratorMetadata "rings" "Ring summation"
    , IntegratorMetadata "prof2d" "Two-dimensional profile fitting"
    ]


commaSeparatedNumbersToCommandLine : String -> String -> Int -> Result String (List CommandLineOption)
commaSeparatedNumbersToCommandLine optionName commaSeparated numberOfValues =
    let
        numbers =
            Maybe.Extra.values <| List.map (String.toFloat << String.trim) <| String.split "," commaSeparated

        numbersLen =
            List.length numbers
    in
    if numbersLen == 0 then
        Ok []

    else if numbersLen == numberOfValues then
        Ok [ LongOption optionName (String.join "," (List.map String.fromFloat numbers)) ]

    else
        Err (optionName ++ ": invalid format: not " ++ String.fromInt numberOfValues ++ " numbers")


tolerancesToCommandLine : Tolerances String -> Result String (List CommandLineOption)
tolerancesToCommandLine tolerances =
    let
        parseNumber : String -> String -> Result String Float
        parseNumber x desc =
            case String.toFloat x of
                Nothing ->
                    Err (desc ++ ": not a number: " ++ x)

                Just n ->
                    Ok n

        sidesTolerances : Result String ( Float, Float, Float )
        sidesTolerances =
            Result.map3
                join3
                (parseNumber tolerances.toleranceAPercent "tolerance a")
                (parseNumber tolerances.toleranceBPercent "tolerance b")
                (parseNumber tolerances.toleranceCPercent "tolerance c")

        anglesTolerances : Result String ( Float, Float, Float )
        anglesTolerances =
            Result.map3
                join3
                (parseNumber tolerances.toleranceAlphaDegrees "tolerance α")
                (parseNumber tolerances.toleranceBetaDegrees "tolerance β")
                (parseNumber tolerances.toleranceGammaDegrees "tolerance γ")

        resolvedTolerances : Result String ( ( Float, Float, Float ), ( Float, Float, Float ) )
        resolvedTolerances =
            Result.map2 (\x y -> ( x, y )) sidesTolerances anglesTolerances

        tolerancesToString : ( ( Float, Float, Float ), ( Float, Float, Float ) ) -> String
        tolerancesToString ( ( a, b, c ), ( alpha, beta, gamma ) ) =
            String.join "," <| List.map String.fromFloat [ a, b, c, alpha, beta, gamma ]
    in
    Result.map (List.singleton << LongOption "tolerance" << tolerancesToString) resolvedTolerances


integratorToCommandLine : Model -> Result String (List CommandLineOption)
integratorToCommandLine m =
    case m.integrator of
        Nothing ->
            Ok []

        Just "none" ->
            Ok [ LongOption "integration" "none" ]

        Just integrator ->
            let
                cen =
                    if m.integrationCenter then
                        "-cen"

                    else
                        "-nocen"

                sat =
                    if m.integrationSat then
                        "-sat"

                    else
                        "-nosat"

                grad =
                    if m.integrationGrad then
                        "-grad"

                    else
                        "-nograd"

                suffix =
                    cen ++ sat ++ grad
            in
            Ok [ LongOption "integration" (integrator ++ suffix) ]


toCommandLine : Model -> Result (List String) (List CommandLineOption)
toCommandLine ip =
    let
        mapMaybe f field =
            Maybe.withDefault (Ok []) (Maybe.map f field)
    in
    Result.map List.concat <|
        collectResults
            [ peakDetectorToCommandLine ip.peakDetector
            , numberToCommandLine "Threshold" ip.peakDetection.threshold (\n -> [ LongOption "threshold" n ])
            , numberToCommandLine "Minimum squared gradient" ip.peakDetection.minSquaredGradient (\n -> [ LongOption "min-squared-gradient" n ])
            , numberToCommandLine "Minimum signal/noise ratio" ip.peakDetection.minSnr (\n -> [ LongOption "min-snr" n ])
            , numberToCommandLine "Minimum number of pixels" ip.peakDetection.minPixCount (\n -> [ LongOption "min-pix-count" n ])
            , numberToCommandLine "Maximum number of pixels" ip.peakDetection.maxPixCount (\n -> [ LongOption "max-pix-count" n ])
            , commaSeparatedNumbersToCommandLine "peak-radius" ip.peakDetection.peakRadius 3
            , numberToCommandLine "Local background radius" ip.peakDetection.localBgRadius (\n -> [ LongOption "local-bg-radius" n ])
            , integerToCommandLine "Minimum resolution" ip.peakDetection.minRes (\n -> [ LongOption "min-res" n ])
            , integerToCommandLine "Maximum resolution" ip.peakDetection.maxRes (\n -> [ LongOption "max-res" n ])
            , numberToCommandLine "Minimum signal/noise ratio of brightest pixel in peak" ip.peakDetection.minSnrBiggestPix (\n -> [ LongOption "min-snr-biggest-pix" n ])
            , numberToCommandLine "Minimum signal/noise ratio of peak pixel" ip.peakDetection.minSnrPeakPix (\n -> [ LongOption "min-snr-peak-pix" n ])
            , numberToCommandLine "Minimum background standard deviation" ip.peakDetection.minSig (\n -> [ LongOption "min-sig" n ])
            , mapMaybe (indexingMethodsToCommandLine << Dict.values) ip.indexingMethods
            , intDiagToCommandLine ip.integrationDiag
            , boolToSwitchCommandLine "multi" ip.multi
            , boolToSwitchCommandLine "mille" ip.mille
            , boolToSwitchCommandLine "asdf-fast" ip.asdfFast
            , boolToSwitchCommandLine "peakfinder8-fast" ip.peakDetection.peakfinder8Fast
            , boolToSwitchCommandLine "no-refine" ip.noRefine
            , boolToSwitchCommandLine "no-retry" ip.noRetry
            , boolToSwitchCommandLine "no-check-peaks" ip.noCheckPeaks
            , boolToSwitchCommandLine "no-check-cell" ip.noCheckCell
            , mapMaybe tolerancesToCommandLine ip.customTolerances
            , integerToCommandLine "Min peaks"
                ip.peakDetection.minPeaks
                (\n ->
                    if n /= "0" then
                        [ LongOption "min-peaks" n ]

                    else
                        []
                )
            , integratorToCommandLine ip
            , boolToSwitchCommandLine "overpredict" ip.overpredict
            , boolToSwitchCommandLine "profile" ip.profile
            , boolToSwitchCommandLine "cell-parameters-only" ip.cellParametersOnly
            , mapMaybe (\pushRes -> numberToCommandLine "Limit prediction" pushRes (\n -> [ LongOption "push-res" n ])) ip.pushRes
            , mapMaybe (\fixProfileRadius -> numberToCommandLine "Profile radius" fixProfileRadius (\n -> [ LongOption "fix-profile-radius" n ])) ip.fixProfileRadius
            , mapMaybe (\fixDivergence -> numberToCommandLine "Divergence" fixDivergence (\n -> [ LongOption "fix-divergence" n ])) ip.fixDivergence
            , mapMaybe
                (\radii ->
                    andThenRadii
                        radii
                        (stringToFloatResult "int radii")
                        |> Result.map (\parsedRadii -> [ LongOption "int-radius" (radiiToString parsedRadii) ])
                )
                ip.intRadii
            ]


convertCommandLineToModel : Model -> String -> Result String Model
convertCommandLineToModel model cli =
    let
        convertIndexingMethod : String -> Result String IndexingMethod
        convertIndexingMethod s =
            case String.split "-" s of
                method :: rest ->
                    if List.member method knownIndexingMethods then
                        Ok
                            { methodName = method
                            , enabled = True
                            , latticeInformation = not (List.member "nolatt" rest)
                            , cellInformation = not (List.member "nocell" rest)
                            }

                    else
                        Err ("invalid indexing method " ++ method)

                _ ->
                    Err "invalid indexing method"

        convertSingle : CommandLineOption -> Result String Model -> Result String Model
        convertSingle option priorModelMaybe =
            case priorModelMaybe of
                Err e ->
                    Err e

                Ok priorModel ->
                    case option of
                        LongOption "peaks" peakDetector ->
                            case List.Extra.find (\pf -> pf.param == peakDetector) knownPeakDetectors of
                                Nothing ->
                                    Err ("invalid peak detector: " ++ peakDetector)

                                Just { param } ->
                                    Ok { priorModel | peakDetector = Just param }

                        LongOption "integration" integration ->
                            case String.split "-" integration of
                                integrator :: rest ->
                                    if List.member integrator (List.map .param knownIntegrators) then
                                        Ok
                                            { priorModel
                                                | integrator = Just integrator
                                                , integrationCenter = List.member "cen" rest && not (List.member "nocen" rest)
                                                , integrationSat = List.member "sat" rest && not (List.member "nosat" rest)
                                                , integrationGrad = List.member "grad" rest && not (List.member "nograd" rest)
                                            }

                                    else
                                        Err ("invalid integration method " ++ integrator)

                                _ ->
                                    Err "invalid indexing method"

                        LongOption "push-res" res ->
                            case String.toFloat res of
                                Nothing ->
                                    Err ("invalid push-res value " ++ res)

                                Just _ ->
                                    Ok { priorModel | pushRes = Just res }

                        LongOption "fix-profile-radius" res ->
                            case String.toFloat res of
                                Nothing ->
                                    Err ("invalid fix-profile-radius value " ++ res)

                                Just _ ->
                                    Ok { priorModel | fixProfileRadius = Just res }

                        LongOption "fix-divergence" res ->
                            case String.toFloat res of
                                Nothing ->
                                    Err ("invalid fix-divergence value " ++ res)

                                Just _ ->
                                    Ok { priorModel | fixDivergence = Just res }

                        LongOption "int-diag" diag ->
                            case commandLineToIntDiag diag of
                                Err e ->
                                    Err e

                                Ok v ->
                                    Ok { priorModel | integrationDiag = mapIntegrationDiag (\( h, k, l ) -> String.fromInt h ++ "," ++ String.fromInt k ++ "," ++ String.fromInt l) v }

                        LongOption "indexing" indexingMethods ->
                            case Result.Extra.combineMap convertIndexingMethod (String.split "," indexingMethods) of
                                Err e ->
                                    Err e

                                Ok methodsFromCli ->
                                    let
                                        allDisabledMethods =
                                            List.foldr
                                                (\methodName ->
                                                    Dict.insert methodName
                                                        { methodName = methodName
                                                        , latticeInformation = True
                                                        , cellInformation = True
                                                        , enabled = False
                                                        }
                                                )
                                                Dict.empty
                                                knownIndexingMethods

                                        allMethods =
                                            -- union will favor the left side if there are two equivalent keys
                                            -- so we take the cli methods and "append" the other methods as disabled
                                            Dict.union
                                                (List.foldr
                                                    (\newMethod -> Dict.insert newMethod.methodName newMethod)
                                                    Dict.empty
                                                    methodsFromCli
                                                )
                                                allDisabledMethods
                                    in
                                    Ok { priorModel | indexingMethods = Just allMethods }

                        LongOption "tolerance" toleranceString ->
                            case Maybe.Extra.combineMap String.toFloat (String.split "," toleranceString) of
                                Nothing ->
                                    Err ("tolerances are not all comma-separated numbers: " ++ toleranceString)

                                Just [ a, b, c, al, be, ga ] ->
                                    Ok
                                        { priorModel
                                            | customTolerances =
                                                Just
                                                    { toleranceAPercent = String.fromFloat a
                                                    , toleranceBPercent = String.fromFloat b
                                                    , toleranceCPercent = String.fromFloat c
                                                    , toleranceAlphaDegrees = String.fromFloat al
                                                    , toleranceBetaDegrees = String.fromFloat be
                                                    , toleranceGammaDegrees = String.fromFloat ga
                                                    }
                                        }

                                Just _ ->
                                    Err ("didn't get six comma-separated numbers as tolerance: " ++ toleranceString)

                        LongOption "int-radius" intRadiiString ->
                            case Maybe.Extra.combineMap String.toFloat (String.split "," intRadiiString) of
                                Nothing ->
                                    Err ("radii are not all comma-separated numbers: " ++ intRadiiString)

                                Just [ inner, middle, outer ] ->
                                    Ok
                                        { priorModel
                                            | intRadii =
                                                Just
                                                    { inner = String.fromFloat inner
                                                    , middle = String.fromFloat middle
                                                    , outer = String.fromFloat outer
                                                    }
                                        }

                                Just _ ->
                                    Err ("didn't get three comma-separated numbers as int radius: " ++ intRadiiString)

                        LongOption optionName optionValue ->
                            let
                                updatePeakDetection : (PeakDetectionModel -> String -> PeakDetectionModel) -> String -> Model
                                updatePeakDetection f newValue =
                                    let
                                        newPd =
                                            f priorModel.peakDetection newValue
                                    in
                                    { priorModel | peakDetection = newPd }

                                knownOptions : Dict String (String -> Model)
                                knownOptions =
                                    Dict.fromList
                                        [ ( "threshold", updatePeakDetection (\pd sv -> { pd | threshold = sv }) )
                                        , ( "min-squared-gradient", updatePeakDetection (\pd sv -> { pd | minSquaredGradient = sv }) )
                                        , ( "min-snr", updatePeakDetection (\pd sv -> { pd | minSnr = sv }) )
                                        , ( "peak-radius", updatePeakDetection (\pd sv -> { pd | peakRadius = sv }) )
                                        , ( "min-pix-count", updatePeakDetection (\pd sv -> { pd | minPixCount = sv }) )
                                        , ( "max-pix-count", updatePeakDetection (\pd sv -> { pd | maxPixCount = sv }) )
                                        , ( "local-bg-radius", updatePeakDetection (\pd sv -> { pd | localBgRadius = sv }) )
                                        , ( "min-res", updatePeakDetection (\pd sv -> { pd | minRes = sv }) )
                                        , ( "max-res", updatePeakDetection (\pd sv -> { pd | maxRes = sv }) )
                                        , ( "min-snr-biggest-pix", updatePeakDetection (\pd sv -> { pd | minSnrBiggestPix = sv }) )
                                        , ( "min-snr-peak-pix", updatePeakDetection (\pd sv -> { pd | minSnrPeakPix = sv }) )
                                        , ( "min-sig", updatePeakDetection (\pd sv -> { pd | minSig = sv }) )
                                        , ( "min-peaks", updatePeakDetection (\pd sv -> { pd | minPeaks = sv }) )
                                        ]
                            in
                            case Dict.get optionName knownOptions of
                                Just processor ->
                                    Ok (processor optionValue)

                                Nothing ->
                                    Err ("unknown option " ++ optionName)

                        LongSwitch optionName ->
                            let
                                updatePeakDetection : (PeakDetectionModel -> PeakDetectionModel) -> () -> Model
                                updatePeakDetection f _ =
                                    let
                                        newPd =
                                            f priorModel.peakDetection
                                    in
                                    { priorModel | peakDetection = newPd }

                                knownOptions =
                                    Dict.fromList
                                        [ ( "multi", \_ -> { priorModel | multi = True } )
                                        , ( "mille", \_ -> { priorModel | mille = True } )
                                        , ( "peakfinder8-fast", updatePeakDetection (\pd -> { pd | peakfinder8Fast = True }) )
                                        , ( "asdf-fast", \_ -> { priorModel | asdfFast = True } )
                                        , ( "no-refine", \_ -> { priorModel | noRefine = True } )
                                        , ( "no-retry", \_ -> { priorModel | noRetry = True } )
                                        , ( "no-check-peaks", \_ -> { priorModel | noCheckPeaks = True } )
                                        , ( "no-check-cell", \_ -> { priorModel | noCheckCell = True } )
                                        , ( "overpredict", \_ -> { priorModel | overpredict = True } )
                                        , ( "profile", \_ -> { priorModel | profile = True } )
                                        , ( "cell-parameters-only", \_ -> { priorModel | cellParametersOnly = True } )
                                        ]
                            in
                            case Dict.get optionName knownOptions of
                                Just processor ->
                                    Ok (processor ())

                                Nothing ->
                                    Err ("unknown switch " ++ optionName)

                        _ ->
                            Err ("unknown option " ++ coparseOption option)
    in
    case parseCommandLine cli of
        Err e ->
            Err (deadEndsToString e)

        Ok options ->
            List.foldl convertSingle (Ok model) options


initialPeakDetection : PeakDetectionModel
initialPeakDetection =
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
    }


init : List String -> String -> String -> Bool -> Model
init sources cellDescription geometryFile mutableCellDescription =
    { peakDetector = Nothing
    , peakDetection =
        initialPeakDetection
    , sources = sources
    , mutableCellDescription = mutableCellDescription

    -- The list of sources can be empty. Then we have the "current source" as empty and let it be a freetext field
    , source = Maybe.withDefault "" (List.head sources)
    , geometryFile = geometryFile
    , cellDescription = cellDescription
    , openTab = PeakDetection
    , indexingMethods = Nothing
    , indexingChooserOpen = "general"
    , asdfFast = False
    , multi = True
    , mille = False
    , noRefine = False
    , noRetry = False
    , noCheckPeaks = False
    , noCheckCell = False
    , customTolerances = Nothing
    , integrator = Nothing
    , fixProfileRadius = Nothing
    , fixDivergence = Nothing
    , integrationCenter = False
    , integrationSat = False
    , integrationGrad = False
    , integrationDiag = None
    , overpredict = False
    , profile = False
    , cellParametersOnly = False
    , pushRes = Nothing
    , intRadii = Nothing
    , commandLineEdit = Nothing
    , commandLineEditError = Nothing
    }


viewCitation : String -> String -> Html msg
viewCitation title link =
    a [ href link ] [ text title ]


viewNumericInput : String -> String -> String -> Maybe String -> (String -> Model -> Model) -> Html Msg
viewNumericInput inputId currentValue description optionalDetails changer =
    div
        [ class "form-floating mb-3"
        ]
        [ input_
            [ type_ "text"
            , class "form-control"
            , id inputId
            , onInput
                (\newValue ->
                    Change (changer newValue)
                )
            , value currentValue
            ]
        , label [ for inputId ] [ text description ]
        , case optionalDetails of
            Nothing ->
                text ""

            Just details ->
                div [ class "form-text" ] [ text details ]
        ]


viewNumericPdInput : String -> String -> String -> Maybe String -> (String -> PeakDetectionModel -> PeakDetectionModel) -> Html Msg
viewNumericPdInput inputId currentValue description optionalDetails changer =
    viewNumericInput inputId currentValue description optionalDetails (\newValue priorModel -> { priorModel | peakDetection = changer newValue priorModel.peakDetection })


viewMinPeaks : Model -> Html Msg
viewMinPeaks model =
    viewNumericPdInput "min-peaks" model.peakDetection.minPeaks "Skip frames with fewer than peaks" (Just "Do not try to index frames with fewer than this peaks. These frames will still be described in the output stream. Leave at 0 to have all frames be considered hits, even if they have no peaks at all.") (\newValue m -> { m | minPeaks = newValue })


viewThresholdInput : Model -> Html Msg
viewThresholdInput model =
    viewNumericPdInput "threshold"
        model.peakDetection.threshold
        "Threshold"
        (Just "The threshold has arbitrary units matching the pixel values in the data.")
        (\newValue ip -> { ip | threshold = newValue })


viewMinSnrInput : Model -> Html Msg
viewMinSnrInput model =
    viewNumericPdInput
        "min-snr"
        model.peakDetection.minSnr
        "Minimum signal/noise ratio"
        Nothing
        (\newValue ip -> { ip | minSnr = newValue })


viewZaefDetails : Model -> Html Msg
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
                    model.peakDetection.minSquaredGradient
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
                        model.peakDetection.peakRadius
                        "Peak radius (inner, middle, outer)"
                        (Just "Specify three numbers, comma-separated. These are the inner, middle and outer radii for three-ring integration during the peak search.")
                        (\newValue ip -> { ip | peakRadius = newValue })
                    ]
                ]
            ]
        ]


viewPeakfinder8Details : Model -> Html Msg
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
                [ viewNumericPdInput "felix-domega"
                    model.peakDetection.minPixCount
                    "Minimum number of pixels"
                    Nothing
                    (\newValue ip -> { ip | minPixCount = newValue })
                ]
            , div [ class "col-6" ]
                [ viewNumericPdInput "max-pix-count"
                    model.peakDetection.maxPixCount
                    "Maximum number of pixels"
                    Nothing
                    (\newValue ip -> { ip | maxPixCount = newValue })
                ]
            , div [ class "row" ]
                [ div [ class "col-6" ]
                    [ viewNumericPdInput "local-bg-radius"
                        model.peakDetection.localBgRadius
                        "Local background radius"
                        Nothing
                        (\newValue ip -> { ip | localBgRadius = newValue })
                    ]
                , div [ class "col-6" ]
                    [ viewFormCheckPd
                        "peakfinder8-fast"
                        "Fast"
                        (Just (span_ [ text "Increase speed by restricting the number of sampling points used for the background statistics calculation." ]))
                        model.peakDetection.peakfinder8Fast
                        (\m -> { m | peakfinder8Fast = not m.peakfinder8Fast })
                    ]
                ]
            ]
        , div [ class "row" ]
            [ div [ class "col-6" ]
                [ viewNumericPdInput "min-res"
                    model.peakDetection.minRes
                    "Minimum resolution"
                    (Just "This is in pixels, so it must be an integer.")
                    (\newValue ip -> { ip | minRes = newValue })
                ]
            , div [ class "col-6" ]
                [ viewNumericPdInput "max-res"
                    model.peakDetection.maxRes
                    "Maximum resolution"
                    (Just "This is in pixels, so it must be an integer.")
                    (\newValue ip -> { ip | maxRes = newValue })
                ]
            ]
        ]


viewPeakfinder9Details : Model -> Html Msg
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
                    model.peakDetection.minSnrBiggestPix
                    "Minimum signal/noise ratio of brightest pixel in peak"
                    Nothing
                    (\newValue ip -> { ip | minSnrBiggestPix = newValue })
                ]
            , div [ class "col-6" ]
                [ viewNumericPdInput "min-snr-peak-pix"
                    model.peakDetection.minSnrPeakPix
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
                    model.peakDetection.minSig
                    "Minimum background standard deviation"
                    Nothing
                    (\newValue ip -> { ip | minSig = newValue })
                ]
            , div [ class "row" ]
                [ div [ class "col-6" ]
                    [ viewNumericPdInput "local-bg-radius"
                        model.peakDetection.localBgRadius
                        "Local background radius"
                        Nothing
                        (\newValue ip -> { ip | localBgRadius = newValue })
                    ]
                ]
            ]
        ]


viewFormCheck : String -> String -> Maybe (Html Msg) -> Bool -> (Model -> Model) -> Html Msg
viewFormCheck elementId description details checkedValue f =
    div [ class "form-check" ]
        [ input_
            [ class "form-check-input"
            , type_ "checkbox"
            , checked checkedValue
            , id elementId
            , onInput (always (Change f))
            ]
        , label [ class "form-check-label", for elementId ] [ text description ]
        , case details of
            Nothing ->
                text ""

            Just detailsHtml ->
                div [ class "form-text" ] [ detailsHtml ]
        ]


viewFormCheckPd : String -> String -> Maybe (Html Msg) -> Bool -> (PeakDetectionModel -> PeakDetectionModel) -> Html Msg
viewFormCheckPd elementId description details checkedValue f =
    viewFormCheck elementId description details checkedValue (\model -> { model | peakDetection = f model.peakDetection })


viewFixProfileRadiusInput : Maybe String -> Html Msg
viewFixProfileRadiusInput fixProfileRadius =
    div [ class "input-group" ]
        [ div [ class "input-group-text" ]
            [ input_
                [ class "form-check-input"
                , type_ "checkbox"
                , value ""
                , checked (Maybe.Extra.isJust fixProfileRadius)
                , onInput (always (Change (\m -> { m | fixProfileRadius = Maybe.Extra.unwrap (Just "") (always Nothing) fixProfileRadius })))
                ]
            ]
        , span [ class "input-group-text" ] [ text "Fix the reflection radius to " ]
        , input_
            [ class "form-control"
            , type_ "text"
            , disabled (Maybe.Extra.isNothing fixProfileRadius)
            , onInput (\newValue -> Change (\m -> { m | fixProfileRadius = Just newValue }))
            , value (Maybe.Extra.unwrap "" identity fixProfileRadius)
            ]
        , span [ class "input-group-text" ] [ text "nm⁻¹" ]
        , div
            [ class "form-text" ]
            [ text "Fix the beam and crystal parameters to the given values. The default is to automatically determine the profile radius." ]
        ]


viewFixDivergenceInput : Maybe String -> Html Msg
viewFixDivergenceInput fixDivergence =
    div [ class "input-group" ]
        [ div [ class "input-group-text" ]
            [ input_
                [ class "form-check-input"
                , type_ "checkbox"
                , value ""
                , checked (Maybe.Extra.isJust fixDivergence)
                , onInput (always (Change (\m -> { m | fixDivergence = Maybe.Extra.unwrap (Just "") (always Nothing) fixDivergence })))
                ]
            ]
        , span [ class "input-group-text" ] [ text "Fix divergence angle to " ]
        , input_
            [ class "form-control"
            , type_ "text"
            , disabled (Maybe.Extra.isNothing fixDivergence)
            , onInput (\newValue -> Change (\m -> { m | fixDivergence = Just newValue }))
            , value (Maybe.Extra.unwrap "" identity fixDivergence)
            ]
        , span [ class "input-group-text" ] [ text "rad" ]
        ]


viewFixProfileRadiusAndDivergenceInputs : Model -> Html Msg
viewFixProfileRadiusAndDivergenceInputs model =
    div [ class "row" ]
        [ div [ class "col-6" ] [ viewFixProfileRadiusInput model.fixProfileRadius ]
        , div [ class "col-6" ] [ viewFixDivergenceInput model.fixDivergence ]
        ]


viewPushResInput : Maybe String -> Html Msg
viewPushResInput pushRes =
    div [ class "input-group" ]
        [ div [ class "input-group-text" ]
            [ input_
                [ class "form-check-input"
                , type_ "checkbox"
                , value ""
                , checked (Maybe.Extra.isJust pushRes)
                , onInput (always (Change (\m -> { m | pushRes = Maybe.Extra.unwrap (Just "") (always Nothing) pushRes })))
                ]
            ]
        , span [ class "input-group-text" ] [ text "Limit prediction to " ]
        , input_
            [ class "form-control"
            , type_ "text"
            , disabled (Maybe.Extra.isNothing pushRes)
            , onInput (\newValue -> Change (\m -> { m | pushRes = Just newValue }))
            , value (Maybe.Extra.unwrap "" identity pushRes)
            ]
        , span [ class "input-group-text" ] [ text "nm⁻¹ above apparent resolution limit" ]
        , div
            [ class "form-text" ]
            [ text "Integrate by nm⁻¹ higher than the apparent resolution limit of each individual crystal.  This value can be negative to integrate lower than the apparent resolution limit.  The default is “infinity”, which means that no cutoff is applied. Note that you can also apply this cutoff at the merging stage, which is usually better: reflections which are thrown away at the integration stage cannot be brought back later. However, applying a resolution cutoff during integration will make the stream file significantly smaller and faster to merge." ]
        ]


viewIntDiag : Model -> Html Msg
viewIntDiag model =
    let
        isSame x y =
            case x of
                Indices _ ->
                    case y of
                        Indices _ ->
                            True

                        yv ->
                            x == yv

                xv ->
                    xv == y

        selectValueToIntDiagValue newValue =
            case newValue of
                "none" ->
                    None

                "all" ->
                    All

                "random" ->
                    Random

                "implausible" ->
                    Implausible

                "negative" ->
                    Negative

                "strong" ->
                    Strong

                other ->
                    Indices other

        viewIntDiagOption paramValue description =
            option
                [ value paramValue
                , selected
                    (isSame
                        model.integrationDiag
                        (selectValueToIntDiagValue paramValue)
                    )
                ]
                [ text description ]

        inputHandler newValue =
            Change (\ip -> { ip | integrationDiag = selectValueToIntDiagValue newValue })

        floatingSelect =
            div [ class "form-floating" ]
                [ select
                    [ class "form-select"
                    , id "int-diag"
                    , onInput inputHandler
                    ]
                    [ viewIntDiagOption "none" "none"
                    , viewIntDiagOption "all" "all"
                    , viewIntDiagOption "random" "random"
                    , viewIntDiagOption "implausible" "implausible"
                    , viewIntDiagOption "negative" "negative"
                    , viewIntDiagOption "strong" "strong"
                    , viewIntDiagOption "indices" "Miller indices"
                    ]
                , label [ for "int-diag" ] [ text "Integration diagnostics" ]
                ]

        indicesInput indices =
            input_
                [ type_ "text"
                , class "form-control"
                , value indices
                , onInput
                    (\newValue ->
                        Change
                            (\m ->
                                { m
                                    | integrationDiag = Indices newValue
                                }
                            )
                    )
                ]
    in
    case model.integrationDiag of
        Indices indices ->
            div [ class "row" ]
                [ div [ class "col-6" ] [ floatingSelect ]
                , div [ class "col-6" ]
                    [ indicesInput indices ]
                ]

        _ ->
            floatingSelect


viewIntRadii : Model -> Html Msg
viewIntRadii model =
    div_
        [ viewFormCheck
            "change-int-radii"
            "Custom integration radii"
            (Just (text "Set the inner, middle and outer radii for three-ring integration."))
            (Maybe.Extra.isJust model.intRadii)
            (\m ->
                { m
                    | intRadii =
                        case m.intRadii of
                            Nothing ->
                                Just defaultRadii

                            Just _ ->
                                Nothing
                }
            )
        , case model.intRadii of
            Nothing ->
                text ""

            Just { inner, middle, outer } ->
                let
                    viewInput v f =
                        input_
                            [ type_ "text"
                            , class "form-control"
                            , value v
                            , onInput
                                (\newValue ->
                                    Change
                                        (\m ->
                                            { m
                                                | intRadii =
                                                    Maybe.map (f newValue) m.intRadii
                                            }
                                        )
                                )
                            ]
                in
                div [ class "input-group" ]
                    [ span [ class "input-group-text" ] [ text "inner" ]
                    , viewInput inner (\newValue prevRadii -> { prevRadii | inner = newValue })
                    , span [ class "input-group-text" ] [ text "middle" ]
                    , viewInput middle (\newValue prevRadii -> { prevRadii | middle = newValue })
                    , span [ class "input-group-text" ] [ text "outer" ]
                    , viewInput outer (\newValue prevRadii -> { prevRadii | outer = newValue })
                    ]
        ]


viewIntegratorDetails : Model -> String -> Html Msg
viewIntegratorDetails model integrator =
    let
        viewCenterCheckbox =
            viewFormCheck
                "integration-center"
                "Center integration boxes on observed reflections"
                Nothing
                model.integrationCenter
                (\m -> { m | integrationCenter = not m.integrationCenter })

        viewSatCheckbox =
            viewFormCheck
                "integration-sat"
                "Skip saturation check"
                (Just
                    (span_
                        [ text "Normally, reflections which contain one or more pixels above "
                        , code_ [ text "max_adu" ]
                        , text " (defined in the detector geometry file) will not be integrated and written to the stream.  Using this option skips this check, and allows saturated reflections to be passed to the later merging stages.  However, note that the saturation check will only be done if "
                        , code_ [ text "max_adu" ]
                        , text " is set in the geometry file. Usually, it’s better to exclude saturated reflections at the merging stage.  See the documentation for "
                        , code_ [ text "max_adu" ]
                        , text " in the "
                        , a
                            [ href "https://gitlab.desy.de/thomas.white/crystfel/-/blob/master/doc/man/crystfel_geometry.5.md" ]
                            [ text "CrystFEL geometry description" ]
                        ]
                    )
                )
                model.integrationSat
                (\m -> { m | integrationSat = not m.integrationSat })

        viewGradCheckbox =
            viewFormCheck
                "integration-grad"
                "Fit the background around the reflection using gradients in two dimensions"
                (Just (text "Without the option, the background will be considered to have the same value across the entire integration box, which gives better results in most cases."))
                model.integrationGrad
                (\m -> { m | integrationGrad = not m.integrationGrad })

        viewOverpredictCheckbox =
            viewFormCheck
                "overpredict"
                "Over-predict reflections (for post-refinement)"
                (Just (text "This is needed to provide a buffer zone when using post-refinement, but makes it difficult to judge the accuracy of the predictions because there are so many reflections.  It will also reduce the quality of the merged data if you merge without partiality estimation."))
                model.overpredict
                (\m -> { m | overpredict = not m.overpredict })

        viewCellParametersOnlyCheckbox =
            viewFormCheck
                "cell-parameters-only"
                "Cell parameters only"
                (Just (text "Do not predict reflections at all. Use this option if you're not at all interested in the integrated reflection intensities or even the positions of the reflections. You will still get unit cell parameters, and the process will be much faster, especially for large unit cells."))
                model.cellParametersOnly
                (\m -> { m | cellParametersOnly = not m.cellParametersOnly })

        viewNotNoneElements =
            div_
                [ viewCenterCheckbox
                , viewSatCheckbox
                , viewGradCheckbox
                , viewOverpredictCheckbox
                , viewCellParametersOnlyCheckbox
                , viewPushResInput model.pushRes
                , viewFixProfileRadiusAndDivergenceInputs model
                , viewIntDiag model
                , viewIntRadii model
                ]
    in
    case integrator of
        "rings" ->
            div_
                [ p [ class "mb-3 mt-3" ]
                    [ text "Use three concentric rings to determine the peak, buffer and background estimation regions.  The radius of the smallest circle sets the peak region. The radius of the middle and outer circles describe an annulus from which the background will be estimated."
                    ]
                , viewNotNoneElements
                ]

        "prof2d" ->
            div_
                [ p [ class "mb-3 mt-3" ]
                    [ text "Integrate the peaks using 2D profile fitting with a planar background, close to the method described by "
                    , viewCitation "Rossmann, M. G. (1979). J. Appl. Cryst. 12, 225-238." "https://doi.org/10.1107/S0021889879012218"
                    ]
                , viewNotNoneElements
                ]

        _ ->
            text ""


viewPeakDetectionDetails : Model -> String -> Html Msg
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


viewPeakDetection : Model -> Html Msg
viewPeakDetection model =
    div [ class "mb-3" ]
        [ div [ class "form-floating" ]
            [ select
                [ class "form-select"
                , id "peak-detector"
                , onInput
                    (\newParam ->
                        Change
                            (\ip ->
                                { ip
                                    | peakDetector =
                                        if newParam == "unset" then
                                            Nothing

                                        else
                                            Just newParam
                                    , peakDetection = initialPeakDetection
                                }
                            )
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
            ]
        ]


viewIndexingMethodRow : IndexingMethod -> Html Msg
viewIndexingMethodRow { methodName, enabled, latticeInformation, cellInformation } =
    tr_
        [ td []
            [ div [ class "form-check form-switch mx-auto" ]
                [ input_
                    [ class "form-check-input"
                    , type_ "checkbox"
                    , checked enabled
                    , onInput (always (ChangeIndexingMethod methodName (\m -> { m | enabled = not m.enabled })))
                    ]
                ]
            ]
        , td_ [ text methodName ]
        , td_
            [ div [ class "form-check form-switch" ]
                [ input_
                    [ class "form-check-input"
                    , type_ "checkbox"
                    , checked latticeInformation
                    , onInput (always (ChangeIndexingMethod methodName (\m -> { m | latticeInformation = not m.latticeInformation })))
                    ]
                ]
            ]
        , td_
            [ div [ class "form-check form-switch" ]
                [ input_
                    [ class "form-check-input"
                    , type_ "checkbox"
                    , checked cellInformation
                    , onInput (always (ChangeIndexingMethod methodName (\m -> { m | cellInformation = not m.cellInformation })))
                    ]
                ]
            ]
        ]


viewIndexingChooserGeneralTab : Html Msg
viewIndexingChooserGeneralTab =
    div []
        [ p_ [ text "You can choose between a variety of indexing methods.  You can choose more than one method, in which case each method will be tried in turn until one of them reports that the pattern has been successfully indexed. If you don't specify any indexing methods, indexamajig will try to automatically determine which indexing methods are available." ]
        , p_
            [ text "You can control what information should be provided to the indexers. Note that indexamajig performs a series of checks on the indexing results, including checking that the result is consistent with the target unit cell parameters." ]
        , p_ [ text "Usually, you do not need to explicitly specify anything more than the indexing method itself (e.g. mosflm or asdf).  The default behaviour for all indexing methods is to make the maximum possible use of prior information such as the lattice type and cell parameters.  If you do not provide this information, for example if you do not give any unit cell file or if the unit cell file does not contain cell parameters (only lattice type information), the indexing methods you give will be modified accordingly. If you only specify the indexing methods themselves, in most cases indexamajig will do what you want and intuitively expect!  However, the options are available if you need finer control." ]
        , dl []
            [ dt [] [ text "dirax" ]
            , dd [] [ text "Invoke DirAx. See ", viewCitation "Barty, A., Kirian, R. A., Maia, F. R. N. C., Hantke, M., Yoon, C. H., White, T. A. & Chapman, H. (2014). J. Appl. Cryst. 47, 1118-1131." "https://doi.org/10.1107/S0021889891010634" ]
            , dt [] [ text "mosflm" ]
            , dd [] [ text "Invoke Mosflm. See ", viewCitation "Powell, H. R. (1999). Acta Cryst. D55, 1690-1695." "https://doi.org/10.1107/S0907444999009506" ]
            ]
        ]


viewFelixOptions : Html Msg
viewFelixOptions =
    div []
        [ p [] [ text "Invoke Felix, which will use your cell parameters to find multiple crystals in each pattern. See ", viewCitation "Beyerlein, K. R., White, T. A., Yefanov, O., Gati, C., Kazantsev, I. G., Nielsen, N. F.-G., Larsen, P. M., Chapman, H. N. & Schmidt, S. (2017). J. Appl. Cryst. 50, 1075-1083." "https://doi.org/10.1107/s1600576717007506" ] ]


viewTakeTwoOptions : Html Msg
viewTakeTwoOptions =
    div []
        [ text "Use the TakeTwo algorithm. See ", viewCitation "Ginn, H. M., Roedig, P., Kuo, A., Evans, G., Sauter, N. K., Ernst, O. P., Meents, A., Mueller-Werkmeister, H., Miller, R. J. D. & Stuart, D. I. (2016). Acta Cryst. D72, 956-965." "https://doi.org/10.1107/S2059798316010706" ]


viewXgandalfOptions : Html Msg
viewXgandalfOptions =
    div []
        [ text "Use the eXtended GrAdieNt Descent Algorithm for Lattice Finding. See ", viewCitation "Gevorkov, Y., Yefanov, O., Barty, A., White, T. A., Mariani, V., Brehm, W., Tolstikova, A., Grigat, R.-R. & Chapman, H. N. (2019). Acta Cryst. A75, 694-704." "https://doi.org/10.1107/S2053273319010593" ]


viewPinkIndexerOptions : Html Msg
viewPinkIndexerOptions =
    div []
        [ text "Use the pinkIndexer algorithm. See ", viewCitation "Gevorkov, Y., Barty, A., Brehm, W., White, T. A., Tolstikova, A., Wiedorn, M. O., Meents, A., Grigat, R.-R., Chapman, H. N. & Yefanov, O. (2020). Acta Cryst. A76, 121-131." "https://doi.org/10.1107/S2053273319015559" ]


viewAsdfOptions : Model -> Html Msg
viewAsdfOptions model =
    div []
        [ text "This is an implementation of the dirax algorithm, with some very small changes such as using a 1D Fourier transform for finding the lattice repeats. This algorithm is implemented natively within CrystFEL meaning that no external software is required."
        , viewFormCheck
            "asdf-fast"
            "Fast"
            (Just (span_ [ text "This enables a faster mode of operation for asdf indexing, which is around 3 times faster but only about 7% less successful." ]))
            model.asdfFast
            (\m -> { m | asdfFast = not m.asdfFast })
        ]


viewIndexingMethodsTabs : Model -> Html Msg
viewIndexingMethodsTabs model =
    let
        viewTabHeader tabId tabDescription =
            li [ class "nav-item" ]
                [ button
                    [ class
                        ("nav-link"
                            ++ (if model.indexingChooserOpen == tabId then
                                    " active"

                                else
                                    ""
                               )
                        )
                    , id ("indexing-tab-" ++ tabId)
                    , onClick (ChangeIndexingChooserOpen tabId)
                    , type_ "button"
                    ]
                    [ text tabDescription ]
                ]

        viewTabPane tabId content =
            div
                [ class
                    ("tab-pane"
                        ++ (if model.indexingChooserOpen == tabId then
                                "show active"

                            else
                                ""
                           )
                    )
                , id ("indexing-tab-" ++ tabId)
                ]
                [ content
                ]
    in
    div []
        [ ul [ class "nav nav-tabs mb-3" ]
            [ viewTabHeader "general" "General"
            , viewTabHeader "felix" "Felix"
            , viewTabHeader "taketwo" "TakeTwo"
            , viewTabHeader "xgandalf" "Xgandalf"
            , viewTabHeader "pinkindexer" "pinkIndexer"
            , viewTabHeader "asdf" "asdf"
            ]
        , div [ class "tab-content" ]
            [ viewTabPane "general" (div_ [ h5_ [ text "Indexers" ], viewIndexingChooserGeneralTab ])
            , viewTabPane "felix" (div_ [ h5_ [ text "Felix-specific options" ], viewFelixOptions ])
            , viewTabPane "taketwo" (div_ [ h5_ [ text "TakeTwo-specific options" ], viewTakeTwoOptions ])
            , viewTabPane "xgandalf" (div_ [ h5_ [ text "Xgandalf-specific options" ], viewXgandalfOptions ])
            , viewTabPane "pinkindexer" (div_ [ h5_ [ text "pinkIndexer-specific options" ], viewPinkIndexerOptions ])
            , viewTabPane "asdf" (div_ [ h5_ [ text "asdf-specific options" ], viewAsdfOptions model ])
            ]
        ]


viewCustomToleranceForm : Tolerances String -> Html Msg
viewCustomToleranceForm tolerances =
    let
        viewCol : String -> String -> (Tolerances String -> String) -> (String -> Tolerances String -> Tolerances String) -> Html Msg
        viewCol prefix suffix getter setter =
            div [ class "col" ]
                [ div [ class "input-group" ]
                    [ span [ class "input-group-text" ] [ text prefix ]
                    , input_
                        [ type_ "text"
                        , class "form-control"
                        , onInput
                            (\newValue ->
                                Change
                                    (\m ->
                                        { m
                                            | customTolerances =
                                                Maybe.map (setter newValue) m.customTolerances
                                        }
                                    )
                            )
                        , value (getter tolerances)
                        ]
                    , span [ class "input-group-text" ] [ text suffix ]
                    ]
                ]
    in
    div_
        [ div [ class "row mb-1" ]
            [ viewCol "A" "%" .toleranceAPercent (\newValue t -> { t | toleranceAPercent = newValue })
            , viewCol "B" "%" .toleranceBPercent (\newValue t -> { t | toleranceBPercent = newValue })
            , viewCol "C" "%" .toleranceCPercent (\newValue t -> { t | toleranceCPercent = newValue })
            ]
        , div [ class "row mb-1" ]
            [ viewCol "α" "°" .toleranceAlphaDegrees (\newValue t -> { t | toleranceAlphaDegrees = newValue })
            , viewCol "β" "°" .toleranceBetaDegrees (\newValue t -> { t | toleranceBetaDegrees = newValue })
            , viewCol "γ" "°" .toleranceGammaDegrees (\newValue t -> { t | toleranceGammaDegrees = newValue })
            ]
        ]


viewGeneralIndexingParameters : Model -> Html Msg
viewGeneralIndexingParameters model =
    div [ class "mb-3" ]
        [ h5_ [ text "Indexing Parameters" ]
        , viewFormCheck
            "multi"
            "Attempt to find multiple lattices per frame"
            (Just (span_ [ text "Enable the “subtract and retry” method, where after a successful indexing attempt the spots accounted for by the indexing solution are removed before trying to index again in the hope of finding a second lattice.  This doesn’t have anything to do with the multi-lattice indexing algorithms such as Felix. This option also adjusts the thresholds for identifying successful indexing results (see ", em_ [ text "Check indexing solutions match peaks" ], text ")." ]))
            model.multi
            (\m -> { m | multi = not m.multi })
        , viewFormCheck
            "no-refine"
            "Refine the indexing solution"
            (Just (text "Skip the prediction refinement step.  Usually this will decrease the quality of the results and allow false solutions to get through, but occasionally it might be necessary."))
            (not model.noRefine)
            (\m -> { m | noRefine = not m.noRefine })
        , viewFormCheck
            "no-retry"
            "Retry indexing if unsuccessful"
            (Just
                (text "Disable retry indexing.  After an unsuccessful indexing attempt, indexamajig would normally remove the 10% weakest peaks and try again.  This option disables that, which makes things much faster but decreases the indexing success rate.")
            )
            (not model.noRetry)
            (\m -> { m | noRetry = not m.noRetry })
        , viewFormCheck "no-check-peaks"
            "Check indexing solutions match peaks"
            (Just
                (span_
                    [ text "Do not check that most of the peaks can be accounted for by the indexing solution.  The thresholds for a successful result are influenced by the option "
                    , em_ [ text "Attempt to find multiple lattices per frame" ]
                    ]
                )
            )
            (not model.noCheckPeaks)
            (\m -> { m | noCheckPeaks = not m.noCheckPeaks })
        , viewFormCheck
            "no-check-cell"
            "Check indexing solutions against reference cell"
            (Just (span_ [ text "Do not check the cell parameters against the reference unit cell (see ", em_ [ text "Cell description" ], text ")." ]))
            (not model.noCheckCell)
            (\m -> { m | noCheckCell = not m.noCheckCell })
        , viewFormCheck
            "custom-tolerances-enabled"
            "Change default unit cell tolerances"
            (Just (text "Set the tolerances for unit cell comparison. If the unit cell is centered, the tolerances are applied to the corresponding primitive unit cell."))
            (Maybe.Extra.isJust model.customTolerances)
            (\m ->
                { m
                    | customTolerances =
                        case m.customTolerances of
                            Nothing ->
                                Just defaultTolerances

                            Just _ ->
                                Nothing
                }
            )
        , Maybe.withDefault (text "") (Maybe.map viewCustomToleranceForm model.customTolerances)
        , viewMinPeaks model
        ]


viewMiscParameters : Model -> Html Msg
viewMiscParameters model =
    let
        viewProfileCheckbox =
            viewFormCheck
                "profile"
                "Display timing data for performance monitoring"
                Nothing
                model.profile
                (\m -> { m | profile = not m.profile })

        viewMilleCheckbox =
            viewFormCheck
                "mille"
                "Use millepede to refine detector geometry"
                (Just (span_ [ text "Write detector calibration data in Millepede-II format." ]))
                model.mille
                (\m -> { m | mille = not m.mille })
    in
    div [ class "mb-3" ]
        [ viewProfileCheckbox, viewMilleCheckbox ]


viewIntegrationParameters : Model -> Html Msg
viewIntegrationParameters model =
    div [ class "mb-3" ]
        [ p_ [ text "If the pattern could be successfully indexed, peaks will be predicted in the pattern and their intensities measured." ]
        , div [ class "form-floating" ]
            [ select
                [ class "form-select"
                , id "integrator"
                , onInput
                    (\newParam ->
                        Change
                            (\ip ->
                                { ip
                                    | integrator =
                                        if newParam == "unset" then
                                            Nothing

                                        else
                                            Just newParam
                                    , integrationCenter = newParam == "prof2d"
                                }
                            )
                    )
                ]
                (option
                    [ value "unset"
                    , selected (Maybe.Extra.isNothing model.integrator)
                    ]
                    [ text "Use default" ]
                    :: List.map
                        (\{ param, longName } ->
                            option [ value param, selected (model.integrator == Just param) ]
                                [ text (longName ++ " (" ++ param ++ ")")
                                ]
                        )
                        knownIntegrators
                )
            , label [ for "integrator" ] [ text "Integration method" ]
            , case model.integrator of
                Nothing ->
                    text ""

                Just integrator ->
                    viewIntegratorDetails model integrator
            ]
        ]


viewIndexingMethods : Model -> Html Msg
viewIndexingMethods model =
    div [ class "mb-3" ]
        [ div [ class "form-check form-switch mb-3" ]
            [ input_
                [ class "form-check-input"
                , type_ "checkbox"
                , id "choose-indexing-automatically"
                , checked (Maybe.Extra.isNothing model.indexingMethods)
                , onInput (always ToggleIndexingMethods)
                ]
            , label
                [ for "choose-indexing-automatically"
                , class "form-check-label"
                ]
                [ text "Automatically choose the indexing methods" ]
            ]
        , case model.indexingMethods of
            Nothing ->
                text ""

            Just methods ->
                div [ class "row" ]
                    [ div [ class "col-6" ]
                        [ table
                            [ class "table table-striped text-center", style "width" "fit-content" ]
                            [ thead_
                                [ tr_ [ th_ [ text "Enabled" ], th_ [ text "Method" ], th_ [ text "Prior unit cell" ], th_ [ text "Prior lattice type" ] ]
                                ]
                            , tbody_
                                (List.map viewIndexingMethodRow (Dict.values methods))
                            ]
                        , p_
                            [ div [ class "form-check form-switch" ]
                                [ input_
                                    [ class "form-check-input"
                                    , type_ "checkbox"
                                    , id "toggle-all-indexing-methods"
                                    , checked
                                        (Maybe.withDefault False <|
                                            Maybe.map (List.all (\x -> x.enabled) << Dict.values) model.indexingMethods
                                        )
                                    , onInput
                                        (always <|
                                            ToggleAllIndexingMethods <|
                                                Maybe.withDefault False <|
                                                    Maybe.map (List.all (\x -> x.enabled) << Dict.values) model.indexingMethods
                                        )
                                    ]
                                , label
                                    [ for "toggle-all-indexing-methods"
                                    , class "form-check-label"
                                    ]
                                    [ text "Toggle all methods" ]
                                ]
                            ]
                        ]
                    , div [ class "col-6" ]
                        [ viewIndexingMethodsTabs model
                        ]
                    ]
        , viewGeneralIndexingParameters model
        ]


viewCommandLine : Model -> Html Msg
viewCommandLine model =
    case toCommandLine model of
        Err errors ->
            div_
                [ viewAlert [ AlertDanger ]
                    [ h5_ [ text "Invalid indexing parameters" ]
                    , case errors of
                        [ x ] ->
                            strongText x

                        [] ->
                            text ""

                        xs ->
                            ul_ (List.map (li_ << List.singleton << strongText) xs)
                    ]
                ]

        Ok commandLine ->
            div [ class "mb-3" ]
                [ p_ [ text "Command line:" ]
                , case model.commandLineEdit of
                    Nothing ->
                        div [ class "input-group" ]
                            [ textarea [ class "form-control", disabled True ] [ text (coparseCommandLine commandLine) ]
                            , button [ class "btn btn-outline-primary", type_ "button", onClick StartCommandLineEdit ] [ text "Edit" ]
                            ]

                    Just editValue ->
                        div [ class "input-group" ]
                            [ textarea
                                [ class "form-control"
                                , onInput (\newCommandLine -> Change (\m -> { m | commandLineEdit = Just newCommandLine }))
                                ]
                                [ text editValue ]
                            , button [ class "btn btn-outline-primary", type_ "button", onClick (FinishCommandLineEdit editValue) ] [ text "Apply" ]
                            , button [ class "btn btn-outline-secondary", type_ "button", onClick CancelCommandLineEdit ] [ text "Cancel" ]
                            ]
                , case model.commandLineEditError of
                    Nothing ->
                        text ""

                    Just error ->
                        viewAlert [ AlertDanger ] [ text error ]
                ]


viewCellDescription : Model -> Html Msg
viewCellDescription model =
    div [ class "form-floating mb-3" ]
        [ input_
            [ type_ "text"
            , class "form-control"
            , id "pp-cell-description"
            , value model.cellDescription
            , onInput
                (\newCellDescription ->
                    Change
                        (\ip ->
                            { ip | cellDescription = newCellDescription }
                        )
                )
            ]
        , label [ for "pp-cell-description" ] [ text "Cell Description" ]
        , div
            [ class "form-text"
            ]
            [ text
                ("This field is filled from the chemical attributi."
                    ++ " Leave blank if you want to determine the parameters."
                )
            ]
        ]


viewGeometry : Model -> Html Msg
viewGeometry model =
    div [ class "form-floating mb-3" ]
        [ input_
            [ type_ "text"
            , class "form-control"
            , id "pp-geometry-file"
            , value model.geometryFile
            , onInput
                (\newGeometryFile ->
                    Change
                        (\ip ->
                            { ip | geometryFile = newGeometryFile }
                        )
                )
            ]
        , label [ for "pp-geometry-file" ] [ text "Geometry file" ]
        , div
            [ class "form-text"
            ]
            [ text "Leave blank to autodetect the geometry file by going up the directory hierarchy of the beamtime."
            ]
        ]


viewSource : Model -> Html Msg
viewSource model =
    div [ class "form-floating mb-3" ] <|
        if List.isEmpty model.sources then
            [ input_
                [ type_ "text"
                , class "form-control"
                , id "indexing-source"
                , onInput
                    (\newSource ->
                        Change (\ip -> { ip | source = newSource })
                    )
                , value model.source
                ]
            , label [ for "indexing-source" ] [ text "Source" ]
            , div [ class "form-text" ] [ text "Leave blank to use the default source." ]
            ]

        else
            [ select
                [ class "form-select"
                , id "indexing-source"
                , onInput (\newSource -> Change (\ip -> { ip | source = newSource }))
                ]
                (List.map (\currentSource -> option [ selected (currentSource == model.source), value currentSource ] [ text currentSource ]) model.sources)
            , label [ for "indexing-source" ] [ text "Source" ]
            , div [ class "form-text" ] [ text "Usually this is nothing you have to change. If you have processed the images using a pipeline that emits, for example, the raw images, as well as a compressed version, you can select both here." ]
            ]


view : Model -> Html Msg
view model =
    let
        viewTabHeader tabId tabDescription =
            li [ class "nav-item" ]
                [ button
                    [ class
                        ("nav-link"
                            ++ (if model.openTab == tabId then
                                    " active"

                                else
                                    ""
                               )
                        )
                    , id ("top-level-tab-" ++ tabTypeToString tabId)
                    , onClick (ChangeOpenTab tabId)
                    , type_ "button"
                    ]
                    [ text tabDescription ]
                ]

        viewTabPane tabId content =
            div
                [ class
                    ("tab-pane"
                        ++ (if model.openTab == tabId then
                                "show active"

                            else
                                ""
                           )
                    )
                , id ("top-level-tab-" ++ tabTypeToString tabId)
                ]
                [ content
                ]
    in
    form_
        [ viewSource model
        , viewGeometry model
        , if model.mutableCellDescription then
            viewCellDescription model

          else
            text ""
        , viewCommandLine model
        , ul [ class "nav nav-tabs mb-3" ]
            [ viewTabHeader PeakDetection "Peak Detection"
            , viewTabHeader Indexing "Indexing"
            , viewTabHeader Integration "Integration"
            , viewTabHeader Misc "Misc"
            ]
        , div [ class "tab-content" ]
            [ viewTabPane PeakDetection (viewPeakDetection model)
            , viewTabPane Indexing (viewIndexingMethods model)
            , viewTabPane Integration (viewIntegrationParameters model)
            , viewTabPane Misc (viewMiscParameters model)
            ]
        ]


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        ChangeOpenTab newTab ->
            ( { model | openTab = newTab }, Cmd.none )

        CancelCommandLineEdit ->
            ( { model | commandLineEdit = Nothing, commandLineEditError = Nothing }, Cmd.none )

        StartCommandLineEdit ->
            case toCommandLine model of
                Err _ ->
                    ( model, Cmd.none )

                Ok commandLineList ->
                    ( { model | commandLineEdit = Just (coparseCommandLine commandLineList), commandLineEditError = Nothing }, Cmd.none )

        FinishCommandLineEdit commandLineString ->
            case convertCommandLineToModel model commandLineString of
                Err e ->
                    ( { model | commandLineEditError = Just e }, Cmd.none )

                Ok newModel ->
                    ( { newModel | commandLineEdit = Nothing, commandLineEditError = Nothing }, Cmd.none )

        ChangeIndexingChooserOpen newChooser ->
            ( { model | indexingChooserOpen = newChooser }, Cmd.none )

        ToggleAllIndexingMethods before ->
            case model.indexingMethods of
                Nothing ->
                    ( model, Cmd.none )

                Just methods ->
                    ( { model
                        | indexingMethods =
                            Just <|
                                Dict.map (\_ m -> { m | enabled = not before }) methods
                      }
                    , Cmd.none
                    )

        Change f ->
            ( f model, Cmd.none )

        ChangeIndexingMethod methodName modifier ->
            case model.indexingMethods of
                Nothing ->
                    ( model, Cmd.none )

                Just methods ->
                    let
                        newMethods =
                            Dict.map
                                (\_ method ->
                                    if method.methodName == methodName then
                                        modifier method

                                    else
                                        method
                                )
                                methods
                    in
                    ( { model | indexingMethods = Just newMethods }, Cmd.none )

        ToggleIndexingMethods ->
            let
                newIndexingMethods =
                    case model.indexingMethods of
                        Nothing ->
                            Just <|
                                List.foldr
                                    (\methodName ->
                                        Dict.insert
                                            methodName
                                            { methodName = methodName, latticeInformation = True, cellInformation = True, enabled = True }
                                    )
                                    Dict.empty
                                    knownIndexingMethods

                        _ ->
                            Nothing
            in
            ( { model | indexingMethods = newIndexingMethods }, Cmd.none )
