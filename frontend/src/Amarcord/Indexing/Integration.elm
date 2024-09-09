module Amarcord.Indexing.Integration exposing (Model, fromCommandLine, init, toCommandLine, view)

import Amarcord.CommandLineParser exposing (CommandLineOption(..))
import Amarcord.Html exposing (code_, div_, input_, p_, span_)
import Amarcord.Indexing.Util exposing (CommandLineOptionResult(..), boolToSwitchCommandLine, mapMaybe, numberToCommandLine, stringToFloatResult, viewCitation, viewFormCheck)
import Dict
import Html exposing (Html, a, div, label, option, p, select, span, text)
import Html.Attributes exposing (checked, class, disabled, for, href, id, selected, type_, value)
import Html.Events exposing (onInput)
import Maybe.Extra
import String


type alias IntegratorMetadata =
    { param : String, longName : String }


knownIntegrators : List IntegratorMetadata
knownIntegrators =
    [ IntegratorMetadata "none" "No integration (only spot prediction)"
    , IntegratorMetadata "rings" "Ring summation"
    , IntegratorMetadata "prof2d" "Two-dimensional profile fitting"
    ]


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


parseMillerIndices : String -> Maybe ( Int, Int, Int )
parseMillerIndices indicesAsString =
    case String.split "," indicesAsString of
        [ h, k, l ] ->
            Maybe.map3 (\hParsed kParsed lParsed -> ( hParsed, kParsed, lParsed )) (String.toInt h) (String.toInt k) (String.toInt l)

        _ ->
            Nothing


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


type alias Model =
    { integrator : Maybe String
    , integrationCenter : Bool
    , integrationSat : Bool
    , integrationGrad : Bool
    , integrationDiag : IntegrationDiag String
    , pushRes : Maybe String
    , intRadii : Maybe (IntRadii String)
    , fixProfileRadius : Maybe String
    , fixDivergence : Maybe String
    , overpredict : Bool
    , cellParametersOnly : Bool
    }


init : Model
init =
    { integrator = Nothing
    , fixProfileRadius = Nothing
    , fixDivergence = Nothing
    , integrationCenter = False
    , integrationSat = False
    , integrationGrad = False
    , integrationDiag = None
    , pushRes = Nothing
    , intRadii = Nothing
    , cellParametersOnly = False
    , overpredict = False
    }


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


toCommandLine : Model -> List (Result String (List CommandLineOption))
toCommandLine ip =
    [ intDiagToCommandLine ip.integrationDiag
    , integratorToCommandLine ip
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


fromCommandLine : CommandLineOption -> Model -> CommandLineOptionResult Model
fromCommandLine option model =
    case option of
        LongOption "integration" integration ->
            case String.split "-" integration of
                integrator :: rest ->
                    if List.member integrator (List.map .param knownIntegrators) then
                        ValidCommandLine
                            { model
                                | integrator = Just integrator
                                , integrationCenter = List.member "cen" rest && not (List.member "nocen" rest)
                                , integrationSat = List.member "sat" rest && not (List.member "nosat" rest)
                                , integrationGrad = List.member "grad" rest && not (List.member "nograd" rest)
                            }

                    else
                        InvalidCommandLine ("invalid integration method " ++ integrator)

                _ ->
                    InvalidCommandLine "invalid indexing method"

        LongOption "push-res" res ->
            case String.toFloat res of
                Nothing ->
                    InvalidCommandLine ("invalid push-res value " ++ res)

                Just _ ->
                    ValidCommandLine { model | pushRes = Just res }

        LongOption "fix-profile-radius" res ->
            case String.toFloat res of
                Nothing ->
                    InvalidCommandLine ("invalid fix-profile-radius value " ++ res)

                Just _ ->
                    ValidCommandLine { model | fixProfileRadius = Just res }

        LongOption "fix-divergence" res ->
            case String.toFloat res of
                Nothing ->
                    InvalidCommandLine ("invalid fix-divergence value " ++ res)

                Just _ ->
                    ValidCommandLine { model | fixDivergence = Just res }

        LongOption "int-diag" diag ->
            case commandLineToIntDiag diag of
                Err e ->
                    InvalidCommandLine e

                Ok v ->
                    ValidCommandLine { model | integrationDiag = mapIntegrationDiag (\( h, k, l ) -> String.fromInt h ++ "," ++ String.fromInt k ++ "," ++ String.fromInt l) v }

        LongOption "int-radius" intRadiiString ->
            case Maybe.Extra.combineMap String.toFloat (String.split "," intRadiiString) of
                Nothing ->
                    InvalidCommandLine ("radii are not all comma-separated numbers: " ++ intRadiiString)

                Just [ inner, middle, outer ] ->
                    ValidCommandLine
                        { model
                            | intRadii =
                                Just
                                    { inner = String.fromFloat inner
                                    , middle = String.fromFloat middle
                                    , outer = String.fromFloat outer
                                    }
                        }

                Just _ ->
                    InvalidCommandLine ("didn't get three comma-separated numbers as int radius: " ++ intRadiiString)

        LongSwitch optionName ->
            let
                knownOptions =
                    Dict.fromList
                        [ ( "overpredict", \_ -> { model | overpredict = True } )
                        , ( "cell-parameters-only", \_ -> { model | cellParametersOnly = True } )
                        ]
            in
            case Dict.get optionName knownOptions of
                Just processor ->
                    ValidCommandLine (processor ())

                Nothing ->
                    CommandLineUninteresting

        _ ->
            CommandLineUninteresting


view : Model -> Html (Model -> Model)
view model =
    div [ class "mb-3" ]
        [ p_ [ text "If the pattern could be successfully indexed, peaks will be predicted in the pattern and their intensities measured." ]
        , div [ class "form-floating" ]
            [ select
                [ class "form-select"
                , id "integrator"
                , onInput
                    (\newParam ip ->
                        { ip
                            | integrator =
                                if newParam == "unset" then
                                    Nothing

                                else
                                    Just newParam
                            , integrationCenter = newParam == "prof2d"
                        }
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


viewFixProfileRadiusInput : Maybe String -> Html (Model -> Model)
viewFixProfileRadiusInput fixProfileRadius =
    div [ class "input-group" ]
        [ div [ class "input-group-text" ]
            [ input_
                [ class "form-check-input"
                , type_ "checkbox"
                , value ""
                , checked (Maybe.Extra.isJust fixProfileRadius)
                , onInput (\_ m -> { m | fixProfileRadius = Maybe.Extra.unwrap (Just "") (always Nothing) fixProfileRadius })
                ]
            ]
        , span [ class "input-group-text" ] [ text "Fix the reflection radius to " ]
        , input_
            [ class "form-control"
            , type_ "text"
            , disabled (Maybe.Extra.isNothing fixProfileRadius)
            , onInput (\newValue m -> { m | fixProfileRadius = Just newValue })
            , value (Maybe.Extra.unwrap "" identity fixProfileRadius)
            ]
        , span [ class "input-group-text" ] [ text "nm⁻¹" ]
        , div
            [ class "form-text" ]
            [ text "Fix the beam and crystal parameters to the given values. The default is to automatically determine the profile radius." ]
        ]


viewFixDivergenceInput : Maybe String -> Html (Model -> Model)
viewFixDivergenceInput fixDivergence =
    div [ class "input-group" ]
        [ div [ class "input-group-text" ]
            [ input_
                [ class "form-check-input"
                , type_ "checkbox"
                , value ""
                , checked (Maybe.Extra.isJust fixDivergence)
                , onInput (\_ m -> { m | fixDivergence = Maybe.Extra.unwrap (Just "") (always Nothing) fixDivergence })
                ]
            ]
        , span [ class "input-group-text" ] [ text "Fix divergence angle to " ]
        , input_
            [ class "form-control"
            , type_ "text"
            , disabled (Maybe.Extra.isNothing fixDivergence)
            , onInput (\newValue m -> { m | fixDivergence = Just newValue })
            , value (Maybe.Extra.unwrap "" identity fixDivergence)
            ]
        , span [ class "input-group-text" ] [ text "rad" ]
        ]


viewFixProfileRadiusAndDivergenceInputs : Model -> Html (Model -> Model)
viewFixProfileRadiusAndDivergenceInputs model =
    div [ class "row" ]
        [ div [ class "col-6" ] [ viewFixProfileRadiusInput model.fixProfileRadius ]
        , div [ class "col-6" ] [ viewFixDivergenceInput model.fixDivergence ]
        ]


viewPushResInput : Maybe String -> Html (Model -> Model)
viewPushResInput pushRes =
    div [ class "input-group" ]
        [ div [ class "input-group-text" ]
            [ input_
                [ class "form-check-input"
                , type_ "checkbox"
                , value ""
                , checked (Maybe.Extra.isJust pushRes)
                , onInput (\_ m -> { m | pushRes = Maybe.Extra.unwrap (Just "") (always Nothing) pushRes })
                ]
            ]
        , span [ class "input-group-text" ] [ text "Limit prediction to " ]
        , input_
            [ class "form-control"
            , type_ "text"
            , disabled (Maybe.Extra.isNothing pushRes)
            , onInput (\newValue m -> { m | pushRes = Just newValue })
            , value (Maybe.Extra.unwrap "" identity pushRes)
            ]
        , span [ class "input-group-text" ] [ text "nm⁻¹ above apparent resolution limit" ]
        , div
            [ class "form-text" ]
            [ text "Integrate by nm⁻¹ higher than the apparent resolution limit of each individual crystal.  This value can be negative to integrate lower than the apparent resolution limit.  The default is “infinity”, which means that no cutoff is applied. Note that you can also apply this cutoff at the merging stage, which is usually better: reflections which are thrown away at the integration stage cannot be brought back later. However, applying a resolution cutoff during integration will make the stream file significantly smaller and faster to merge." ]
        ]


viewIntDiag : Model -> Html (Model -> Model)
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

        inputHandler newValue ip =
            { ip | integrationDiag = selectValueToIntDiagValue newValue }

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
                    (\newValue m ->
                        { m
                            | integrationDiag = Indices newValue
                        }
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


viewIntRadii : Model -> Html (Model -> Model)
viewIntRadii model =
    div_
        [ viewFormCheck
            "change-int-radii"
            "Custom integration radii"
            (Just (text "Set the inner, middle and outer radii for three-ring integration."))
            (Maybe.Extra.isJust model.intRadii)
            (\_ m ->
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
                                (\newValue m ->
                                    { m
                                        | intRadii =
                                            Maybe.map (f newValue) m.intRadii
                                    }
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


viewIntegratorDetails : Model -> String -> Html (Model -> Model)
viewIntegratorDetails model integrator =
    let
        viewCenterCheckbox =
            viewFormCheck
                "integration-center"
                "Center integration boxes on observed reflections"
                Nothing
                model.integrationCenter
                (\_ m -> { m | integrationCenter = not m.integrationCenter })

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
                (\_ m -> { m | integrationSat = not m.integrationSat })

        viewGradCheckbox =
            viewFormCheck
                "integration-grad"
                "Fit the background around the reflection using gradients in two dimensions"
                (Just (text "Without the option, the background will be considered to have the same value across the entire integration box, which gives better results in most cases."))
                model.integrationGrad
                (\_ m -> { m | integrationGrad = not m.integrationGrad })

        viewOverpredictCheckbox =
            viewFormCheck
                "overpredict"
                "Over-predict reflections (for post-refinement)"
                (Just (text "This is needed to provide a buffer zone when using post-refinement, but makes it difficult to judge the accuracy of the predictions because there are so many reflections.  It will also reduce the quality of the merged data if you merge without partiality estimation."))
                model.overpredict
                (\_ m -> { m | overpredict = not m.overpredict })

        viewCellParametersOnlyCheckbox =
            viewFormCheck
                "cell-parameters-only"
                "Cell parameters only"
                (Just (text "Do not predict reflections at all. Use this option if you're not at all interested in the integrated reflection intensities or even the positions of the reflections. You will still get unit cell parameters, and the process will be much faster, especially for large unit cells."))
                model.cellParametersOnly
                (\_ m -> { m | cellParametersOnly = not m.cellParametersOnly })

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
