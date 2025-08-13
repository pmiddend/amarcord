module Amarcord.IndexingParametersEdit exposing (Model, Msg(..), convertCommandLineToModel, extractGeometryId, init, isEditOpen, noGeometrySelected, toCommandLine, update, view)

import Amarcord.Bootstrap exposing (AlertProperty(..), viewAlert)
import Amarcord.CellDescriptionEdit as CellDescriptionEdit
import Amarcord.CommandLineParser exposing (CommandLineOption(..), coparseCommandLine, coparseOption, parseCommandLine)
import Amarcord.GeometryEdit as GeometryEdit
import Amarcord.GeometryMetadata exposing (GeometryId, GeometryMetadata)
import Amarcord.Html exposing (code_, div_, em_, form_, h5_, input_, li_, p_, span_, strongText, tbody_, td_, th_, thead_, tr_, ul_)
import Amarcord.Indexing.Felix as Felix
import Amarcord.Indexing.Integration as Integration
import Amarcord.Indexing.PeakDetection as PeakDetection
import Amarcord.Indexing.PinkIndexer as PinkIndexer
import Amarcord.Indexing.TakeTwo as TakeTwo
import Amarcord.Indexing.Util exposing (CommandLineOptionResult(..), boolToSwitchCommandLine, integerToCommandLine, mapMaybe, numberToCommandLine, viewCitation, viewNumericInput)
import Amarcord.Indexing.Xgandalf as Xgandalf
import Amarcord.Util exposing (collectResults, deadEndsToString, join3)
import Dict exposing (Dict)
import Html exposing (Html, button, dd, div, dl, dt, label, li, option, select, span, table, td, text, textarea, ul)
import Html.Attributes exposing (checked, class, for, id, rows, selected, style, type_, value)
import Html.Events exposing (onClick, onInput)
import List
import Maybe.Extra
import Result.Extra
import String


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


isEditOpen : Model -> Bool
isEditOpen { commandLineEdit } =
    Maybe.Extra.isJust commandLineEdit


noGeometrySelected : Model -> Bool
noGeometrySelected =
    Maybe.Extra.isNothing << GeometryEdit.extractCurrentId << .geometry


type alias Model =
    { openTab : TabType
    , peakDetection : PeakDetection.Model
    , sources : List String
    , maxMilleLevel : String
    , highRes : String

    -- for online indexing, the cell description is inferred, so we don't want the user to be able to change it
    , mutableCellDescription : Bool

    -- Specials
    , source : String
    , geometry : GeometryEdit.Model
    , geometries : List GeometryMetadata
    , cellDescription : CellDescriptionEdit.Model
    , peakDetector : Maybe String
    , indexingMethods : Maybe (Dict String IndexingMethod)
    , indexingChooserOpen : String
    , customTolerances : Maybe (Tolerances String)
    , commandLineEdit : Maybe String
    , commandLineEditError : Maybe String
    , integration : Integration.Model
    , felixOptions : Felix.Model
    , takeTwoOptions : TakeTwo.Model
    , xgandalfOptions : Xgandalf.Model
    , pinkIndexerOptions : PinkIndexer.Model

    -- Options
    , mille : Bool
    , multi : Bool
    , asdfFast : Bool
    , noRefine : Bool
    , noRetry : Bool
    , noCheckPeaks : Bool
    , noCheckCell : Bool
    , profile : Bool
    }


extractGeometryId : Model -> Maybe GeometryId
extractGeometryId { geometry } =
    GeometryEdit.extractCurrentId geometry


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
    | CellDescriptionChange CellDescriptionEdit.Msg
    | GeometryChange GeometryEdit.Msg


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


toCommandLine : Model -> Result (List String) (List CommandLineOption)
toCommandLine ip =
    Result.map List.concat <|
        collectResults <|
            Felix.toCommandLine ip.felixOptions
                ++ TakeTwo.toCommandLine ip.takeTwoOptions
                ++ Xgandalf.toCommandLine ip.xgandalfOptions
                ++ PeakDetection.toCommandLine ip.peakDetection
                ++ Integration.toCommandLine ip.integration
                ++ PinkIndexer.toCommandLine ip.pinkIndexerOptions
                ++ [ mapMaybe (indexingMethodsToCommandLine << Dict.values) ip.indexingMethods
                   , boolToSwitchCommandLine "multi" ip.multi
                   , boolToSwitchCommandLine "mille" ip.mille
                   , integerToCommandLine "max mille level"
                        ip.maxMilleLevel
                        (\n ->
                            if String.trim n /= "" then
                                [ LongOption "max-mille-level" n ]

                            else
                                []
                        )
                   , numberToCommandLine "highres"
                        ip.highRes
                        (\n ->
                            if String.trim n /= "" then
                                [ LongOption "highres" n ]

                            else
                                []
                        )
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
                   , boolToSwitchCommandLine "profile" ip.profile
                   ]



-- When we enter a new command line, we want to start from an empty model and fill it with the options given.


makeEmptyModel : Model -> Model
makeEmptyModel m =
    init m.sources
        (CellDescriptionEdit.modelAsText m.cellDescription)
        (GeometryEdit.extractCurrentId m.geometry)
        m.geometries
        m.mutableCellDescription


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

        convertAllNonCategorizedOptions : CommandLineOption -> Model -> Result String Model
        convertAllNonCategorizedOptions option priorModel =
            case option of
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

                LongOption "max-mille-level" maxMilleLevelString ->
                    case String.toInt maxMilleLevelString of
                        Nothing ->
                            Err ("max-mille-level must be empty or an integer, not: " ++ maxMilleLevelString)

                        Just _ ->
                            Ok
                                { priorModel
                                    | maxMilleLevel = maxMilleLevelString
                                }

                LongOption "highres" highResString ->
                    case String.toFloat highResString of
                        Nothing ->
                            Err ("highres must be empty or a decimal, not: " ++ highResString)

                        Just _ ->
                            Ok
                                { priorModel
                                    | highRes = highResString
                                }

                LongOption "tolerance" toleranceString ->
                    case Maybe.Extra.combineMap String.toFloat (String.split "," toleranceString) of
                        Nothing ->
                            Err ("tolerances are not all comma-separated numbers: " ++ toleranceString)

                        Just [ a, b, c, al ] ->
                            Ok
                                { priorModel
                                    | customTolerances =
                                        Just
                                            { toleranceAPercent = String.fromFloat a
                                            , toleranceBPercent = String.fromFloat b
                                            , toleranceCPercent = String.fromFloat c
                                            , toleranceAlphaDegrees = String.fromFloat al
                                            , toleranceBetaDegrees = String.fromFloat 1.5
                                            , toleranceGammaDegrees = String.fromFloat 1.5
                                            }
                                }

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

                LongOption optionName _ ->
                    Err ("unknown option " ++ optionName)

                LongSwitch optionName ->
                    let
                        knownOptions =
                            Dict.fromList
                                [ ( "multi", \_ -> { priorModel | multi = True } )
                                , ( "mille", \_ -> { priorModel | mille = True } )
                                , ( "asdf-fast", \_ -> { priorModel | asdfFast = True } )
                                , ( "no-refine", \_ -> { priorModel | noRefine = True } )
                                , ( "no-retry", \_ -> { priorModel | noRetry = True } )
                                , ( "no-check-peaks", \_ -> { priorModel | noCheckPeaks = True } )
                                , ( "no-check-cell", \_ -> { priorModel | noCheckCell = True } )
                                , ( "profile", \_ -> { priorModel | profile = True } )
                                ]
                    in
                    case Dict.get optionName knownOptions of
                        Just processor ->
                            Ok (processor ())

                        Nothing ->
                            Err ("unknown switch " ++ optionName)

                _ ->
                    Err ("unknown option " ++ coparseOption option)

        convertSingle : CommandLineOption -> Result String Model -> Result String Model
        convertSingle option priorModelMaybe =
            case priorModelMaybe of
                Err e ->
                    Err e

                Ok priorModel ->
                    case PeakDetection.fromCommandLine option priorModel.peakDetection of
                        InvalidCommandLine err ->
                            Err err

                        ValidCommandLine newModel ->
                            Ok { priorModel | peakDetection = newModel }

                        CommandLineUninteresting ->
                            case Felix.fromCommandLine option priorModel.felixOptions of
                                InvalidCommandLine err ->
                                    Err err

                                ValidCommandLine newModel ->
                                    Ok { priorModel | felixOptions = newModel }

                                CommandLineUninteresting ->
                                    case Integration.fromCommandLine option priorModel.integration of
                                        InvalidCommandLine err ->
                                            Err err

                                        ValidCommandLine newModel ->
                                            Ok { priorModel | integration = newModel }

                                        CommandLineUninteresting ->
                                            case TakeTwo.fromCommandLine option priorModel.takeTwoOptions of
                                                InvalidCommandLine err ->
                                                    Err err

                                                ValidCommandLine newModel ->
                                                    Ok { priorModel | takeTwoOptions = newModel }

                                                CommandLineUninteresting ->
                                                    case Xgandalf.fromCommandLine option priorModel.xgandalfOptions of
                                                        InvalidCommandLine err ->
                                                            Err err

                                                        ValidCommandLine newModel ->
                                                            Ok { priorModel | xgandalfOptions = newModel }

                                                        CommandLineUninteresting ->
                                                            case PinkIndexer.fromCommandLine option priorModel.pinkIndexerOptions of
                                                                InvalidCommandLine err ->
                                                                    Err err

                                                                ValidCommandLine newModel ->
                                                                    Ok { priorModel | pinkIndexerOptions = newModel }

                                                                CommandLineUninteresting ->
                                                                    convertAllNonCategorizedOptions option priorModel
    in
    case parseCommandLine cli of
        Err e ->
            Err (deadEndsToString e)

        Ok options ->
            List.foldl convertSingle (Ok (makeEmptyModel model)) options


init : List String -> String -> Maybe GeometryId -> List GeometryMetadata -> Bool -> Model
init sources cellDescription geometry geometries mutableCellDescription =
    { peakDetector = Nothing
    , peakDetection = PeakDetection.init
    , sources = sources
    , maxMilleLevel = ""
    , highRes = ""
    , mutableCellDescription = mutableCellDescription

    -- The list of sources can be empty. Then we have the "current source" as empty and let it be a freetext field
    , source = Maybe.withDefault "" (List.head sources)
    , geometry = GeometryEdit.init geometry geometries
    , geometries = geometries
    , cellDescription = CellDescriptionEdit.init cellDescription
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
    , felixOptions = Felix.init
    , integration = Integration.init
    , takeTwoOptions = TakeTwo.init
    , xgandalfOptions = Xgandalf.init
    , pinkIndexerOptions = PinkIndexer.init
    , profile = False
    , commandLineEdit = Nothing
    , commandLineEditError = Nothing
    }


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
            , viewTabHeader "pink-indexer" "pinkIndexer"
            , viewTabHeader "asdf" "asdf"
            ]
        , div [ class "tab-content" ]
            [ viewTabPane "general" (div_ [ h5_ [ text "Indexers" ], viewIndexingChooserGeneralTab ])
            , viewTabPane "felix"
                (div_
                    [ h5_ [ text "Felix-specific options" ]
                    , Html.map
                        (\f ->
                            Change
                                (\priorModel ->
                                    { priorModel
                                        | felixOptions =
                                            f priorModel.felixOptions
                                    }
                                )
                        )
                        (Felix.view model.felixOptions)
                    ]
                )
            , viewTabPane "taketwo"
                (div_
                    [ h5_ [ text "TakeTwo-specific options" ]
                    , Html.map
                        (\f ->
                            Change
                                (\priorModel ->
                                    { priorModel
                                        | takeTwoOptions =
                                            f priorModel.takeTwoOptions
                                    }
                                )
                        )
                        (TakeTwo.view model.takeTwoOptions)
                    ]
                )
            , viewTabPane "xgandalf"
                (div_
                    [ h5_ [ text "XGandalf-specific options" ]
                    , Html.map
                        (\f ->
                            Change
                                (\priorModel ->
                                    { priorModel
                                        | xgandalfOptions =
                                            f priorModel.xgandalfOptions
                                    }
                                )
                        )
                        (Xgandalf.view model.xgandalfOptions)
                    ]
                )
            , viewTabPane "pink-indexer"
                (div_
                    [ h5_ [ text "pinkIndexer-specific options" ]
                    , Html.map
                        (\f ->
                            Change
                                (\priorModel ->
                                    { priorModel
                                        | pinkIndexerOptions =
                                            f priorModel.pinkIndexerOptions
                                    }
                                )
                        )
                        (PinkIndexer.view model.pinkIndexerOptions)
                    ]
                )
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

        viewMilleLevelInput =
            Html.map Change <|
                viewNumericInput
                    "max-mille-level"
                    model.maxMilleLevel
                    "Maximum millepede level"
                    Nothing
                    (\newValue priorModel -> { priorModel | maxMilleLevel = newValue })

        viewHighResInput =
            Html.map Change <|
                viewNumericInput
                    "highres"
                    model.highRes
                    "Mark all pixels on the detector higher than this Angstroms as bad"
                    (Just "This might be useful when you have noisy patterns and don't expect any signal above a certain resolution.")
                    (\newValue priorModel -> { priorModel | highRes = newValue })
    in
    div [ class "mb-3" ]
        [ viewProfileCheckbox, viewMilleCheckbox, viewMilleLevelInput, viewHighResInput ]


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
                        div_
                            [ div [ class "mb-2" ] [ code_ [ text (coparseCommandLine commandLine) ] ]
                            , button [ class "btn btn-outline-primary", type_ "button", onClick StartCommandLineEdit ] [ text "Edit" ]
                            ]

                    Just editValue ->
                        div_
                            [ div [ class "mb-2" ]
                                [ textarea
                                    [ class "form-control"
                                    , id "command-line-edit"
                                    , rows 5
                                    , onInput (\newCommandLine -> Change (\m -> { m | commandLineEdit = Just newCommandLine }))
                                    ]
                                    [ text editValue ]
                                ]
                            , div [ class "hstack gap-3" ]
                                [ button [ class "btn btn-outline-primary", type_ "button", onClick (FinishCommandLineEdit editValue) ] [ text "Apply" ]
                                , button [ class "btn btn-outline-secondary", type_ "button", onClick CancelCommandLineEdit ] [ text "Cancel" ]
                                ]
                            ]
                , case model.commandLineEditError of
                    Nothing ->
                        text ""

                    Just error ->
                        viewAlert [ AlertDanger ] [ text error ]
                ]


viewCellDescription : Model -> Html Msg
viewCellDescription model =
    div [ class "mb-3" ]
        [ label [] [ text "Cell Description" ]
        , Html.map CellDescriptionChange (CellDescriptionEdit.view model.cellDescription)
        , div
            [ class "form-text"
            ]
            [ text
                ("This field is filled from the chemical attributi."
                    ++ " Leave blank (select “As text”, then remove the text) if you want to determine the parameters."
                )
            ]
        ]


viewGeometry : Model -> Html Msg
viewGeometry model =
    div [ class "mb-3" ]
        [ Html.map GeometryChange (GeometryEdit.view model.geometry)
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
            [ viewTabPane PeakDetection (Html.map (\f -> Change (\oldModel -> { oldModel | peakDetection = f oldModel.peakDetection })) (PeakDetection.view model.peakDetection))
            , viewTabPane Indexing (viewIndexingMethods model)
            , viewTabPane Integration (Html.map (\f -> Change (\oldModel -> { oldModel | integration = f oldModel.integration })) (Integration.view model.integration))
            , viewTabPane Misc (viewMiscParameters model)
            ]
        ]


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        GeometryChange subMsg ->
            ( { model | geometry = GeometryEdit.update subMsg model.geometry }, Cmd.none )

        CellDescriptionChange subMsg ->
            ( { model | cellDescription = CellDescriptionEdit.update subMsg model.cellDescription }, Cmd.none )

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
