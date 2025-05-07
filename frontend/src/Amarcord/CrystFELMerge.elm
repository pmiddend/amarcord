module Amarcord.CrystFELMerge exposing (Model, Msg, init, mergeModelToString, modelToMergeParameters, quickMergeParameters, update, view)

import Amarcord.API.Requests exposing (IndexingParametersId, indexingParametersIdToInt)
import Amarcord.CellDescriptionEdit as CellDescriptionEdit
import Amarcord.Html exposing (code_, div_, enumSelect, input_, onFloatInput, onIntInput, sup_)
import Amarcord.PointGroupChooser as PointGroupChooser exposing (pointGroupToString)
import Api.Data exposing (JsonPolarisation, JsonQueueMergeJobInput, MergeModel(..), MergeNegativeHandling(..), ScaleIntensities(..))
import Html exposing (Html, button, div, form, h2, label, small, span, text)
import Html.Attributes exposing (checked, class, classList, disabled, for, id, type_, value)
import Html.Events exposing (onClick, onInput)
import Maybe.Extra as MaybeExtra exposing (isJust, isNothing)


allMergeModels : List MergeModel
allMergeModels =
    [ MergeModelUnity, MergeModelXsphere, MergeModelOffset, MergeModelGgpm ]


mergeModelSubtitleToString : MergeModel -> String
mergeModelSubtitleToString x =
    case x of
        MergeModelUnity ->
            "unity"

        MergeModelXsphere ->
            "xsphere"

        MergeModelOffset ->
            "offset"

        MergeModelGgpm ->
            "ggpm"


mergeModelSubtitleFromString : String -> Maybe MergeModel
mergeModelSubtitleFromString x =
    case x of
        "unity" ->
            Just MergeModelUnity

        "xsphere" ->
            Just MergeModelXsphere

        "offset" ->
            Just MergeModelOffset

        "ggpm" ->
            Just MergeModelGgpm

        _ ->
            Nothing


mergeModelToString : MergeModel -> String
mergeModelToString x =
    case x of
        MergeModelUnity ->
            "No partialities"

        MergeModelXsphere ->
            "Bandwidth integral"

        MergeModelOffset ->
            "Monochromatic Ewald sphere offset"

        MergeModelGgpm ->
            "Overlap integral using Gaussians"


type alias Polarisation =
    JsonPolarisation


horizontalEField : Polarisation
horizontalEField =
    { angle = 0
    , percent = 100
    }


verticalEField : Polarisation
verticalEField =
    { angle = 90
    , percent = 100
    }


unpolarized : Polarisation
unpolarized =
    { angle = 0
    , percent = 50
    }


type Msg
    = ModelChange MergeModel
    | ToggleScaleIntensities
    | ToggleDebyeWallerScaling
    | TogglePostRefinement
    | PolarisationPresetChange PolarisationPreset
    | PolarisationChange (Polarisation -> Polarisation)
    | ToggleNoDeltaCcHalf
    | ToggleMaxAdu
    | ToggleStartAfter
    | ToggleStopAfter
    | ToggleForceBandwidth
    | ToggleForceRadius
    | ToggleForceLambda
    | ToggleMinRes
    | TogglePushRes
    | ToggleLogs
    | ToggleNoPr
    | PointGroupMsg PointGroupChooser.Msg
    | ToggleW
    | ModelChangeFn (Model -> Model)
    | CellDescriptionChange CellDescriptionEdit.Msg
    | ToggleAmbigator


type PolarisationPreset
    = HorizontalEField
    | VerticalEField
    | Unpolarized
    | PolarisationOff
    | HorizontalCustomPercent
    | VerticalCustomPercent
    | PolarisationCustom


allPolarisationPresets : List PolarisationPreset
allPolarisationPresets =
    [ HorizontalEField, VerticalEField, Unpolarized, PolarisationOff, HorizontalCustomPercent, VerticalCustomPercent, PolarisationCustom ]


polarisationPresetFromString : String -> Maybe PolarisationPreset
polarisationPresetFromString x =
    case x of
        "horiz-e" ->
            Just HorizontalEField

        "vert-e" ->
            Just VerticalEField

        "unpol" ->
            Just Unpolarized

        "off" ->
            Just PolarisationOff

        "horiz-custom-percent" ->
            Just HorizontalCustomPercent

        "vert-custom-percent" ->
            Just VerticalCustomPercent

        "custom" ->
            Just PolarisationCustom

        _ ->
            Nothing


polarisationPresetToString : PolarisationPreset -> String
polarisationPresetToString x =
    case x of
        HorizontalEField ->
            "horiz-e"

        VerticalEField ->
            "vert-e"

        Unpolarized ->
            "unpol"

        PolarisationOff ->
            "off"

        HorizontalCustomPercent ->
            "horiz-custom-percent"

        VerticalCustomPercent ->
            "vert-custom-percent"

        PolarisationCustom ->
            "custom"


polarisationPresetToDescription : PolarisationPreset -> String
polarisationPresetToDescription x =
    case x of
        HorizontalEField ->
            "Horizontal e-field (most synchrotrons and FELs)"

        VerticalEField ->
            "Vertical e-field (LCLS-II and X-ray undulator)"

        Unpolarized ->
            "Unpolarized incident beam"

        PolarisationOff ->
            "No correction"

        HorizontalCustomPercent ->
            "Horizontal, specify percent"

        VerticalCustomPercent ->
            "Vertical, specify percent"

        PolarisationCustom ->
            "Specify angle and percent"


type alias Model =
    { dataSetId : Int
    , indexingParametersId : IndexingParametersId
    , mergeModel : MergeModel
    , scaleIntensities : ScaleIntensities
    , postRefinement : Bool
    , iterations : Int
    , polarisationPreset : PolarisationPreset
    , polarisation : Polarisation
    , startAfter : Maybe Int
    , stopAfter : Maybe Int
    , relB : Float
    , forceBandwidth : Maybe Float
    , forceRadius : Maybe Float
    , forceLambda : Maybe Float
    , noPr : Bool
    , noDeltaCcHalf : Bool
    , maxAdu : Maybe Float
    , minMeasurements : Int
    , logs : Bool
    , minRes : Maybe Float
    , pushRes : Maybe Float
    , w : Maybe PointGroupChooser.Model
    , cellDescription : CellDescriptionEdit.Model
    , pointGroup : String
    , spaceGroup : String
    , ambigatorCommandLine : Maybe String
    , cutoffLowres : String
    , cutoffHighres : String
    }


modelToMergeParameters : Model -> JsonQueueMergeJobInput
modelToMergeParameters { dataSetId, indexingParametersId, mergeModel, scaleIntensities, postRefinement, iterations, polarisationPreset, polarisation, startAfter, stopAfter, relB, noPr, forceBandwidth, forceRadius, forceLambda, noDeltaCcHalf, maxAdu, minMeasurements, logs, minRes, pushRes, w, cellDescription, pointGroup, spaceGroup, ambigatorCommandLine, cutoffLowres, cutoffHighres } =
    let
        polarisationModelToPolarisation =
            case polarisationPreset of
                HorizontalEField ->
                    Just horizontalEField

                VerticalEField ->
                    Just verticalEField

                Unpolarized ->
                    Just unpolarized

                PolarisationOff ->
                    Nothing

                HorizontalCustomPercent ->
                    Just { angle = 0, percent = polarisation.percent }

                VerticalCustomPercent ->
                    Just { angle = 90, percent = polarisation.percent }

                PolarisationCustom ->
                    Just polarisation
    in
    { strictMode = False
    , indexingParametersId = indexingParametersIdToInt indexingParametersId
    , dataSetId = dataSetId
    , mergeParameters =
        { mergeModel = mergeModel
        , pointGroup = pointGroup
        , spaceGroup =
            if String.trim spaceGroup /= "" then
                Just spaceGroup

            else
                Nothing
        , cellDescription = CellDescriptionEdit.modelAsText cellDescription
        , scaleIntensities = scaleIntensities
        , postRefinement = postRefinement
        , iterations = iterations
        , polarisation = polarisationModelToPolarisation
        , startAfter = startAfter
        , stopAfter = stopAfter
        , relB = relB
        , forceBandwidth = forceBandwidth
        , forceRadius = forceRadius
        , forceLambda = forceLambda
        , noPr = noPr
        , noDeltaCcHalf = noDeltaCcHalf
        , maxAdu = maxAdu
        , minMeasurements = minMeasurements
        , logs = logs
        , minRes = minRes
        , pushRes = pushRes
        , w = Maybe.map pointGroupToString <| Maybe.andThen .chosenPointGroup w
        , negativeHandling = Just MergeNegativeHandlingIgnore
        , ambigatorCommandLine = Maybe.withDefault "" ambigatorCommandLine
        , cutoffLowres = String.toFloat cutoffLowres
        , cutoffHighres =
            case List.map String.trim (String.split "," cutoffHighres) of
                [ a, b, c ] ->
                    Maybe.map3 (\af bf cf -> [ af, bf, cf ]) (String.toFloat a) (String.toFloat b) (String.toFloat c)

                [ a ] ->
                    Maybe.map (\af -> [ af ]) (String.toFloat a)

                _ ->
                    Nothing
        }
    }


view : Model -> Html Msg
view model =
    let
        makeModelSelect : Html Msg
        makeModelSelect =
            enumSelect [ class "form-select", id "crystfel-model" ]
                allMergeModels
                mergeModelSubtitleToString
                mergeModelToString
                (Maybe.map ModelChange << mergeModelSubtitleFromString)
                model.mergeModel

        makePolarisationPresetSelect : Html Msg
        makePolarisationPresetSelect =
            enumSelect [ class "form-select", id "crystfel-polarisation" ]
                allPolarisationPresets
                polarisationPresetToString
                polarisationPresetToDescription
                (Maybe.map PolarisationPresetChange << polarisationPresetFromString)
                model.polarisationPreset

        modelSelect =
            div [ class "mb-3" ]
                [ label [ for "crystfel-model" ] [ text "Model" ]
                , makeModelSelect
                ]

        scalingAndRefinementCheckboxes =
            div [ class "d-flex justify-content-around mb-3" ]
                [ div [ class "form-check form-check-inline" ]
                    [ input_ [ class "form-check-input", id "crystfel-scaling", type_ "checkbox", checked (model.scaleIntensities /= ScaleIntensitiesOff), onClick ToggleScaleIntensities ]
                    , label [ class "form-check-label", for "crystfel-scaling" ] [ text "Scale intensities" ]
                    ]
                , div [ class "form-check form-check-inline" ]
                    [ input_ [ class "form-check-input", id "crystfel-debye-waller-scaling", type_ "checkbox", checked (model.scaleIntensities == ScaleIntensitiesDebyewaller), disabled (model.scaleIntensities == ScaleIntensitiesOff), onClick ToggleDebyeWallerScaling ]
                    , label [ class "form-check-label", for "crystfel-debye-waller-scaling" ] [ text "Debye-Waller scaling" ]
                    ]
                , div [ class "form-check form-check-inline" ]
                    [ input_ [ class "form-check-input", id "crystfel-post-refinement", type_ "checkbox", checked model.postRefinement, onClick TogglePostRefinement ]
                    , label [ class "form-check-label", for "crystfel-post-refinement" ] [ text "Post-refinement" ]
                    ]
                ]

        iterationsRow =
            div [ class "mb-3" ]
                [ label [ for "crystfel-iterations" ] [ text "Number of scaling/post-refinement cycles" ]
                , input_
                    [ id "crystfel-iterations"
                    , type_ "number"
                    , class "form-control"
                    , value (String.fromInt model.iterations)
                    , onIntInput (\i -> ModelChangeFn (\m -> { m | iterations = i }))
                    ]
                ]

        polarisationRow =
            div [ class "row mb-3" ] <|
                div [ class "col" ]
                    [ div [ class "mb-3" ]
                        [ label [ for "crystfel-polarisation-preset" ] [ text "Polarisation" ]
                        , makePolarisationPresetSelect
                        ]
                    ]
                    :: (case model.polarisationPreset of
                            PolarisationOff ->
                                []

                            HorizontalEField ->
                                []

                            VerticalEField ->
                                []

                            Unpolarized ->
                                []

                            HorizontalCustomPercent ->
                                [ div [ class "col" ]
                                    [ div [ class "form-floating" ]
                                        [ input_ [ id "crystfel-polarisation-percent", class "form-control", onIntInput (\i -> PolarisationChange (\p -> { p | percent = i })) ]
                                        , label [ for "crystfel-polarisation-percent" ] [ text "Percent" ]
                                        ]
                                    ]
                                ]

                            VerticalCustomPercent ->
                                [ div [ class "col" ]
                                    [ div [ class "form-floating" ]
                                        [ input_ [ id "crystfel-polarisation-percent", class "form-control", onIntInput (\i -> PolarisationChange (\p -> { p | percent = i })) ]
                                        , label [ for "crystfel-polarisation-percent" ] [ text "Percent" ]
                                        ]
                                    ]
                                ]

                            PolarisationCustom ->
                                [ div [ class "col" ]
                                    [ div [ class "form-floating" ]
                                        [ input_ [ id "crystfel-polarisation-angle", class "form-control", onIntInput (\i -> PolarisationChange (\p -> { p | angle = i })) ]
                                        , label [ for "crystfel-polarisation-angle" ] [ text "Angle (°)" ]
                                        ]
                                    ]
                                , div [ class "col" ]
                                    [ div [ class "form-floating" ]
                                        [ input_ [ id "crystfel-polarisation-percent", class "form-control", onIntInput (\i -> PolarisationChange (\p -> { p | percent = i })) ]
                                        , label [ for "crystfel-polarisation-percent" ] [ text "Percent" ]
                                        ]
                                    ]
                                ]
                       )

        forceRow =
            div [ class "row mb-3" ]
                [ div [ class "col" ]
                    [ div [ class "d-flex align-items-center" ]
                        [ div [ class "form-check me-3" ]
                            [ input_ [ type_ "checkbox", class "form-check-input", id "force-bandwidth-label", onClick ToggleForceBandwidth ]
                            , label [ for "start-force-bandwidth", class "form-check-label text-nowrap" ] [ text "Force bandwidth" ]
                            ]
                        , input_
                            [ type_ "number"
                            , class "form-control"
                            , onFloatInput (\i -> ModelChangeFn (\m -> { m | forceBandwidth = Just i }))
                            , disabled (MaybeExtra.isNothing model.forceBandwidth)
                            , value (Maybe.withDefault "" <| Maybe.map String.fromFloat model.forceBandwidth)
                            ]
                        ]
                    ]
                , div [ class "col" ]
                    [ div [ class "d-flex align-items-center" ]
                        [ div [ class "form-check me-3" ]
                            [ input_ [ type_ "checkbox", class "form-check-input", id "force-radius-label", onClick ToggleForceRadius ]
                            , label [ for "start-force-radius", class "form-check-label text-nowrap" ] [ text "Force initial profile radius" ]
                            ]
                        , div [ class "input-group" ]
                            [ input_
                                [ type_ "number"
                                , class "form-control"
                                , onFloatInput (\i -> ModelChangeFn (\m -> { m | forceRadius = Just i }))
                                , disabled (MaybeExtra.isNothing model.forceRadius)
                                , value (Maybe.withDefault "" <| Maybe.map String.fromFloat model.forceRadius)
                                ]
                            , span [ class "input-group-text" ] [ text "nm", sup_ [ text "-1" ] ]
                            ]
                        ]
                    ]
                , div [ class "col" ]
                    [ div [ class "d-flex align-items-center" ]
                        [ div [ class "form-check me-3" ]
                            [ input_ [ type_ "checkbox", class "form-check-input", id "force-lambda-label", onClick ToggleForceLambda ]
                            , label [ for "start-force-lambda", class "form-check-label text-nowrap" ] [ text "Force wavelength" ]
                            ]
                        , div [ class "input-group" ]
                            [ input_
                                [ type_ "number"
                                , class "form-control"
                                , onFloatInput (\i -> ModelChangeFn (\m -> { m | forceLambda = Just i }))
                                , disabled (MaybeExtra.isNothing model.forceLambda)
                                , value (Maybe.withDefault "" <| Maybe.map String.fromFloat model.forceLambda)
                                ]
                            , span [ class "input-group-text" ] [ text "Å" ]
                            ]
                        ]
                    ]
                ]

        startStopRow =
            div [ class "row mb-3" ]
                [ div [ class "col" ]
                    [ div [ class "d-flex align-items-center" ]
                        [ div [ class "form-check me-3" ]
                            [ input_ [ type_ "checkbox", class "form-check-input", id "start-after-label", onClick ToggleStartAfter ]
                            , label [ for "start-after-label", class "form-check-label text-nowrap" ] [ text "Ignore crystals at the start" ]
                            ]
                        , input_
                            [ type_ "number"
                            , class "form-control"
                            , onIntInput (\i -> ModelChangeFn (\m -> { m | startAfter = Just i }))
                            , disabled (MaybeExtra.isNothing model.startAfter)
                            , value (Maybe.withDefault "" <| Maybe.map String.fromInt model.startAfter)
                            ]
                        ]
                    ]
                , div [ class "col" ]
                    [ div [ class "d-flex align-items-start" ]
                        [ div [ class "form-check me-3" ]
                            [ input_ [ type_ "checkbox", class "form-check-input", id "stop-after-label", onClick ToggleStopAfter ]
                            , label [ for "stop-after-label", class "form-check-label text-nowrap" ] [ text "Stop after amount of crystals" ]
                            , div [ class "form-text" ] [ small [] [ text "This will randomly take, from all processing results, the specified number of crystals. Running it multiple times with the same processing inputs will use the same random images (i.e. we don't reshuffle every time)." ] ]
                            ]
                        , input_
                            [ type_ "number"
                            , class "form-control"
                            , onIntInput (\i -> ModelChangeFn (\m -> { m | stopAfter = Just i }))
                            , disabled (MaybeExtra.isNothing model.stopAfter)
                            , value (Maybe.withDefault "" <| Maybe.map String.fromInt model.stopAfter)
                            ]
                        ]
                    ]
                ]

        noDeltaCcHalfRow =
            div [ class "row g-2 mb-3" ]
                [ div [ class "form-check col" ]
                    [ input_ [ class "form-check-input", id "crystfel-no-delta-cchalf", type_ "checkbox", checked model.noDeltaCcHalf, onClick ToggleNoDeltaCcHalf ]
                    , label [ class "form-check-label", for "crystfel-no-delta-cchalf" ] [ text "Reject bad patterns according to ΔCC½" ]
                    ]
                ]

        maxAduRow =
            div [ class "mb-3 d-flex align-items-center" ]
                [ div [ class "form-check me-3" ]
                    [ input_ [ type_ "checkbox", class "form-check-input", id "max-adu-label", onClick ToggleMaxAdu ]
                    , label [ for "max-adu-label", class "form-check-label text-nowrap" ] [ text "Detector saturation cutoff" ]
                    ]
                , input_
                    [ type_ "number"
                    , class "form-control"
                    , onFloatInput (\f -> ModelChangeFn (\m -> { m | maxAdu = Just f }))
                    , disabled (MaybeExtra.isNothing model.maxAdu)
                    , value (Maybe.withDefault "" <| Maybe.map String.fromFloat model.maxAdu)
                    ]
                ]

        minMeasurementsRow =
            div [ class "mb-3" ]
                [ label [ for "crystfel-min-measurements" ] [ text "Minimum number of measurements per merged reflection" ]
                , input_
                    [ id "crystfel-min-measurements"
                    , type_ "number"
                    , class "form-control"
                    , value (String.fromInt model.minMeasurements)
                    , onIntInput (\i -> ModelChangeFn (\m -> { m | minMeasurements = i }))
                    ]
                ]

        relBRow =
            div [ class "mb-3" ]
                [ label [ for "crystfel-rel-b" ] [ text "Reject crystals with absolute B factors ≥ Å²" ]
                , input_
                    [ id "crystfel-rel-b"
                    , type_ "number"
                    , class "form-control"
                    , value (String.fromFloat model.relB)
                    , onFloatInput (\f -> ModelChangeFn (\m -> { m | relB = f }))
                    ]
                ]

        logsRow =
            div [ class "row g-2 mb-3" ]
                [ div [ class "form-check col" ]
                    [ input_ [ class "form-check-input", id "crystfel-logs", type_ "checkbox", checked model.logs, onClick ToggleLogs ]
                    , label [ class "form-check-label", for "crystfel-logs" ] [ text "Write partiality model diagnostics" ]
                    ]
                ]

        noPrRow =
            div [ class "row g-2 mb-3" ]
                [ div [ class "form-check col" ]
                    [ input_ [ class "form-check-input", id "crystfel-no-pr", type_ "checkbox", checked model.noPr, onClick ToggleNoPr ]
                    , label [ class "form-check-label", for "crystfel-no-pr" ] [ text "Disable the orientation/physics model part of the refinement calculation" ]
                    ]
                ]

        minResRow =
            div [ class "mb-3 d-flex align-items-center" ]
                [ div [ class "form-check me-3" ]
                    [ label [ for "min-res-label", class "form-check-label text-nowrap" ]
                        [ text "Require minimum estimated pattern resolution" ]
                    , input_
                        [ type_ "checkbox"
                        , class "form-check-input"
                        , id "min-res-label"
                        , onClick ToggleMinRes
                        ]
                    ]
                , div [ class "input-group" ]
                    [ input_
                        [ type_ "number"
                        , class "form-control"
                        , onFloatInput (\f -> ModelChangeFn (\m -> { m | minRes = Just f }))
                        , disabled (MaybeExtra.isNothing model.minRes)
                        , value (Maybe.withDefault "" <| Maybe.map String.fromFloat model.minRes)
                        ]
                    , span [ class "input-group-text" ] [ text "Å" ]
                    ]
                ]

        pushResRow =
            div [ class "mb-3 d-flex align-items-center" ]
                [ div [ class "form-check me-3" ]
                    [ label [ for "push-res-label", class "form-check-label text-nowrap" ]
                        [ text "Exclude measurements above resolution limit" ]
                    , input_
                        [ type_ "checkbox"
                        , class "form-check-input"
                        , id "push-res-label"
                        , onClick TogglePushRes
                        ]
                    ]
                , div [ class "input-group" ]
                    [ input_
                        [ type_ "number"
                        , class "form-control"
                        , onFloatInput (\f -> ModelChangeFn (\m -> { m | pushRes = Just f }))
                        , disabled (MaybeExtra.isNothing model.pushRes)
                        , value (Maybe.withDefault "" <| Maybe.map String.fromFloat model.pushRes)
                        ]
                    , span [ class "input-group-text" ] [ text "nm", sup_ [ text "-1" ] ]
                    ]
                ]

        wRow =
            div [ class "accordion mb-3", id "refine-indexing-params-parent" ]
                [ div [ class "accordion-item" ]
                    [ h2 [ class "accordion-header" ]
                        [ button
                            [ type_ "button"
                            , classList [ ( "accordion-button", True ), ( "collapsed", isNothing model.w ) ]
                            , onClick ToggleW
                            ]
                            [ text "Refine indexing assignments" ]
                        ]
                    , div
                        [ id "refine-indexing-params"
                        , classList
                            [ ( "accordion-collapse", True )
                            , ( "collapse", True )
                            , ( "show", isJust model.w )
                            ]
                        ]
                        [ div [ class "accordion-body" ]
                            [ case model.w of
                                Nothing ->
                                    text ""

                                Just symmetry ->
                                    Html.map PointGroupMsg <| PointGroupChooser.view symmetry
                            ]
                        ]
                    ]
                ]

        cutoffs =
            div_
                [ div [ class "mb-3" ]
                    [ label [ for "merge-cutoff-lowres" ]
                        [ text "Low resolution cutoff ("
                        , code_ [ text "get_hkl" ]
                        , text ": "
                        , code_ [ text "--lowres" ]
                        , text ")"
                        ]
                    , div [ class "input-group" ]
                        [ input_
                            [ id "merge-cutoff-lowres"
                            , type_ "text"
                            , class "form-control"
                            , value model.cutoffLowres
                            , onInput (\f -> ModelChangeFn (\m -> { m | cutoffLowres = f }))
                            ]
                        , span [ class "input-group-text" ] [ text "Å" ]
                        ]
                    , div [ class "form-text" ] [ text "After merging, remove reflections with resolution lower than the number." ]
                    ]
                , div [ class "mb-3" ]
                    [ label [ for "merge-cutoff-highres" ]
                        [ text "High resolution cutoff("
                        , code_ [ text "get_hkl" ]
                        , text ": "
                        , code_ [ text "--cutoff-angstroms" ]
                        , text ")"
                        ]
                    , div [ class "input-group" ]
                        [ input_
                            [ id "merge-cutoff-highres"
                            , type_ "text"
                            , class "form-control"
                            , value model.cutoffHighres
                            , onInput (\f -> ModelChangeFn (\m -> { m | cutoffHighres = f }))
                            ]
                        , span [ class "input-group-text" ] [ text "Å" ]
                        ]
                    , div [ class "form-text" ] [ text "After merging, remove reflections with resolution higher than the number. Entering three numbers, separated by comma, will apply an anisotropic cutoff." ]
                    ]
                ]

        cellDescriptionInput =
            div [ class "mb-3" ]
                [ label [] [ text "Cell description" ]
                , Html.map CellDescriptionChange (CellDescriptionEdit.view model.cellDescription)
                ]

        pointGroupInput =
            div [ class "mb-3" ]
                [ label [ for "merge-point-group" ] [ text "Point group" ]
                , input_
                    [ id "merge-point-group"
                    , type_ "text"
                    , class "form-control"
                    , value model.pointGroup
                    , onInput (\f -> ModelChangeFn (\m -> { m | pointGroup = f }))
                    ]
                ]

        spaceGroupInput =
            div [ class "mb-3" ]
                [ label [ for "merge-space-group" ] [ text "Space group" ]
                , input_
                    [ id "merge-space-group"
                    , type_ "text"
                    , class "form-control"
                    , value model.spaceGroup
                    , onInput (\f -> ModelChangeFn (\m -> { m | spaceGroup = f }))
                    ]
                , div [ class "form-text" ] [ text "If this is left empty, will derive space group from the point group." ]
                ]

        ambigatorInput =
            div [ class "mb-3 d-flex align-items-center" ]
                [ div [ class "form-check me-3" ]
                    [ input_ [ type_ "checkbox", class "form-check-input", id "ambigator-label", onClick ToggleAmbigator ]
                    , label [ for "ambigator-label", class "form-check-label text-nowrap" ] [ text "Use ambigator" ]
                    ]
                , input_
                    [ type_ "text"
                    , class "form-control"
                    , onInput (\f -> ModelChangeFn (\m -> { m | ambigatorCommandLine = Just f }))
                    , disabled (isNothing model.ambigatorCommandLine)
                    , value (Maybe.withDefault "" model.ambigatorCommandLine)
                    ]
                ]
    in
    form [ class "p-3" ]
        [ cellDescriptionInput
        , pointGroupInput
        , spaceGroupInput
        , ambigatorInput
        , modelSelect
        , scalingAndRefinementCheckboxes
        , iterationsRow
        , polarisationRow
        , noDeltaCcHalfRow
        , maxAduRow
        , minMeasurementsRow
        , logsRow
        , noPrRow
        , minResRow
        , pushResRow
        , relBRow
        , startStopRow
        , forceRow
        , wRow
        , cutoffs
        ]


init : String -> String -> String -> Int -> IndexingParametersId -> Model
init cellDescription pointGroup spaceGroup dataSetId indexingParametersId =
    { dataSetId = dataSetId
    , indexingParametersId = indexingParametersId
    , mergeModel = MergeModelUnity
    , scaleIntensities = ScaleIntensitiesOff
    , postRefinement = False
    , iterations = 3
    , polarisationPreset = HorizontalEField
    , polarisation = { angle = 0, percent = 100 }
    , startAfter = Nothing
    , stopAfter = Nothing
    , relB = 100.0
    , forceBandwidth = Nothing
    , forceRadius = Nothing
    , forceLambda = Nothing
    , noPr = False
    , noDeltaCcHalf = True
    , maxAdu = Nothing
    , minMeasurements = 2
    , logs = True
    , minRes = Nothing
    , pushRes = Nothing
    , w = Nothing
    , cellDescription = CellDescriptionEdit.init cellDescription
    , pointGroup = pointGroup
    , spaceGroup = spaceGroup
    , ambigatorCommandLine = Nothing
    , cutoffLowres = ""
    , cutoffHighres = ""
    }


quickMergeParameters : Int -> IndexingParametersId -> JsonQueueMergeJobInput
quickMergeParameters dataSetId indexingParametersId =
    { dataSetId = dataSetId
    , indexingParametersId = indexingParametersIdToInt indexingParametersId
    , strictMode = False
    , mergeParameters =
        { mergeModel = MergeModelUnity

        -- Auto-detect from chemicals
        , pointGroup = ""
        , spaceGroup = Nothing

        -- See below for an explanation
        , cellDescription = ""
        , scaleIntensities = ScaleIntensitiesOff
        , postRefinement = False
        , iterations = 3
        , polarisation = Just horizontalEField
        , startAfter = Nothing
        , stopAfter = Nothing
        , relB = 100.0
        , noPr = False
        , forceBandwidth = Nothing
        , forceRadius = Nothing
        , forceLambda = Nothing
        , noDeltaCcHalf = True
        , maxAdu = Nothing
        , minMeasurements = 2
        , logs = True
        , minRes = Nothing
        , pushRes = Nothing
        , w = Nothing
        , negativeHandling = Just MergeNegativeHandlingIgnore
        , ambigatorCommandLine = ""
        , cutoffLowres = Nothing
        , cutoffHighres = Nothing
        }
    }


toggleInt : Maybe Int -> Maybe Int
toggleInt v =
    case v of
        Nothing ->
            Just 0

        _ ->
            Nothing


toggleFloat : Maybe Float -> Maybe Float
toggleFloat v =
    case v of
        Nothing ->
            Just 0

        _ ->
            Nothing


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        ToggleAmbigator ->
            ( { model
                | ambigatorCommandLine =
                    if isJust model.ambigatorCommandLine then
                        Nothing

                    else
                        Just ""
              }
            , Cmd.none
            )

        ModelChange mergeModel ->
            ( { model | mergeModel = mergeModel }, Cmd.none )

        CellDescriptionChange subMsg ->
            ( { model | cellDescription = CellDescriptionEdit.update subMsg model.cellDescription }, Cmd.none )

        ModelChangeFn f ->
            ( f model, Cmd.none )

        PolarisationChange f ->
            ( { model | polarisation = f model.polarisation }, Cmd.none )

        ToggleScaleIntensities ->
            let
                newScaleIntensities =
                    case model.scaleIntensities of
                        ScaleIntensitiesOff ->
                            ScaleIntensitiesNormal

                        _ ->
                            ScaleIntensitiesOff
            in
            ( { model | scaleIntensities = newScaleIntensities }, Cmd.none )

        ToggleDebyeWallerScaling ->
            let
                newScaleIntensities =
                    case model.scaleIntensities of
                        ScaleIntensitiesDebyewaller ->
                            ScaleIntensitiesNormal

                        ScaleIntensitiesNormal ->
                            ScaleIntensitiesDebyewaller

                        ScaleIntensitiesOff ->
                            ScaleIntensitiesOff
            in
            ( { model | scaleIntensities = newScaleIntensities }, Cmd.none )

        TogglePostRefinement ->
            ( { model | postRefinement = not model.postRefinement }, Cmd.none )

        ToggleNoDeltaCcHalf ->
            ( { model | noDeltaCcHalf = not model.noDeltaCcHalf }, Cmd.none )

        ToggleW ->
            case model.w of
                Nothing ->
                    ( { model | w = Just PointGroupChooser.init }, Cmd.none )

                Just _ ->
                    ( { model | w = Nothing }, Cmd.none )

        ToggleLogs ->
            ( { model | logs = not model.logs }, Cmd.none )

        ToggleNoPr ->
            ( { model | noPr = not model.noPr }, Cmd.none )

        ToggleMaxAdu ->
            ( { model | maxAdu = toggleFloat model.maxAdu }, Cmd.none )

        ToggleMinRes ->
            ( { model | minRes = toggleFloat model.minRes }, Cmd.none )

        TogglePushRes ->
            ( { model | pushRes = toggleFloat model.pushRes }, Cmd.none )

        PointGroupMsg pgMsg ->
            case model.w of
                Nothing ->
                    ( model, Cmd.none )

                Just symmetry ->
                    let
                        ( newSymmetryChooser, _ ) =
                            PointGroupChooser.update pgMsg symmetry
                    in
                    ( { model | w = Just newSymmetryChooser }, Cmd.none )

        PolarisationPresetChange polarisationPreset ->
            ( { model | polarisationPreset = polarisationPreset }, Cmd.none )

        ToggleStartAfter ->
            ( { model | startAfter = toggleInt model.startAfter }, Cmd.none )

        ToggleForceBandwidth ->
            ( { model | forceBandwidth = toggleFloat model.forceBandwidth }, Cmd.none )

        ToggleForceRadius ->
            ( { model | forceRadius = toggleFloat model.forceRadius }, Cmd.none )

        ToggleForceLambda ->
            ( { model | forceLambda = toggleFloat model.forceLambda }, Cmd.none )

        ToggleStopAfter ->
            ( { model | stopAfter = toggleInt model.stopAfter }, Cmd.none )
