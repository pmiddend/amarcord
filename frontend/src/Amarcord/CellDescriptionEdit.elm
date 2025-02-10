module Amarcord.CellDescriptionEdit exposing (AsText(..), Model, Msg, init, modelAsText, parseModel, update, validateCellDescription, view)

import Amarcord.Crystallography exposing (CellDescription, Centering, allCenterings, bravaisLatticeCentering, bravaisLatticeToStringNoCentering, centeringFromString, centeringToString, centeringsForSystem, latticeSystemFromString, parseCellDescription, parseCentering, parseLatticeSystem, possibleIssue)
import Amarcord.Html exposing (div_, input_)
import Html exposing (Html, button, div, option, select, small, span, text)
import Html.Attributes exposing (class, disabled, selected, type_, value)
import Html.Events exposing (onClick, onInput)
import Maybe.Extra


type AsText
    = IsStructured
    | IsTextual String (Maybe String)


type alias Model =
    { bravaisLattice : String
    , crystalCentering : String
    , cellA : String
    , cellB : String
    , cellC : String
    , cellAlpha : String
    , cellBeta : String
    , cellGamma : String
    , asText : AsText
    }


modelIsEmpty : Model -> Bool
modelIsEmpty { bravaisLattice, crystalCentering, cellA, cellB, cellC, cellAlpha, cellBeta, cellGamma } =
    String.trim bravaisLattice == "" && String.trim crystalCentering == "" && String.trim cellA == "" && String.trim cellB == "" && String.trim cellC == "" && String.trim cellAlpha == "" && String.trim cellBeta == "" && String.trim cellGamma == ""


init : String -> Model
init cellDescription =
    if String.trim cellDescription == "" then
        { bravaisLattice = ""
        , crystalCentering = ""
        , cellA = ""
        , cellB = ""
        , cellC = ""
        , cellAlpha = ""
        , cellBeta = ""
        , cellGamma = ""
        , asText = IsStructured
        }

    else
        case parseCellDescription cellDescription of
            Err e ->
                { bravaisLattice = ""
                , crystalCentering = ""
                , cellA = ""
                , cellB = ""
                , cellC = ""
                , cellAlpha = ""
                , cellBeta = ""
                , cellGamma = ""
                , asText = IsTextual cellDescription (Just e)
                }

            Ok { bravaisLattice, cellA, cellB, cellC, cellAlpha, cellBeta, cellGamma } ->
                { bravaisLattice = bravaisLatticeToStringNoCentering bravaisLattice
                , crystalCentering = centeringToString (bravaisLatticeCentering bravaisLattice)
                , cellA = String.fromFloat cellA
                , cellB = String.fromFloat cellB
                , cellC = String.fromFloat cellC
                , cellAlpha = String.fromFloat cellAlpha
                , cellBeta = String.fromFloat cellBeta
                , cellGamma = String.fromFloat cellGamma
                , asText = IsStructured
                }


parseModel : Model -> Result String CellDescription
parseModel =
    parseCellDescription << modelAsText


type Msg
    = ChangeA String
    | ChangeB String
    | ChangeC String
    | ChangeAlpha String
    | ChangeBeta String
    | ChangeGamma String
    | ChangeSystem String
    | ChangeCentering String
    | ToggleAsText
    | ChangeAsText String
    | CancelAsText
    | Clear


validateCellDescription : Model -> Result (Html msg) ()
validateCellDescription model =
    -- Special case: empty cell descriptions are fine
    if String.trim (modelAsText model) == "" then
        Ok ()

    else
        case parseModel model of
            Err e ->
                Err (text e)

            Ok _ ->
                Ok ()


viewPossibleIssue : CellDescription -> Html msg
viewPossibleIssue m =
    case possibleIssue m of
        Nothing ->
            text ""

        Just issue ->
            small [] [ span [ class "text-danger" ] [ text issue ] ]


view : Model -> Html Msg
view model =
    case model.asText of
        IsStructured ->
            viewStructured model

        IsTextual t e ->
            viewAsText model t e


viewAsText : Model -> String -> Maybe String -> Html Msg
viewAsText model t errorMessage =
    div_
        [ div [ class "input-group input-group-sm" ]
            [ input_
                [ type_ "text"
                , class
                    ("form-control"
                        ++ (if Maybe.Extra.isJust errorMessage then
                                " is-invalid"

                            else
                                ""
                           )
                    )
                , value t
                , onInput ChangeAsText
                ]
            , button
                [ type_ "button"
                , class "btn btn-secondary"
                , onClick ToggleAsText
                , disabled (Maybe.Extra.isJust errorMessage)
                ]
                [ text "Structured" ]
            , button
                [ type_ "button"
                , class "btn btn-light"
                , onClick CancelAsText
                , disabled (Maybe.Extra.isJust errorMessage && not (modelIsEmpty model))
                ]
                [ text
                    (if modelIsEmpty model then
                        "Clear"

                     else
                        "Cancel"
                    )
                ]
            ]
        , case errorMessage of
            Nothing ->
                text ""

            Just error ->
                small [] [ span [ class "text-danger" ] [ text error ] ]
        ]


allCrystalSystems : List String
allCrystalSystems =
    [ "triclinic"
    , "monoclinic a"
    , "monoclinic b"
    , "monoclinic c"
    , "orthorhombic"
    , "tetragonal a"
    , "tetragonal b"
    , "tetragonal c"
    , "rhombohedral"
    , "hexagonal a"
    , "hexagonal b"
    , "hexagonal c"
    , "cubic"
    ]


latticeSystemAndAxisToLatticeSystem : String -> String
latticeSystemAndAxisToLatticeSystem bravaisLattice =
    case String.split " " bravaisLattice of
        [ noAx, _ ] ->
            noAx

        _ ->
            bravaisLattice


latticeSystemAndAxisToAxis : String -> String
latticeSystemAndAxisToAxis bravaisLattice =
    case String.split " " bravaisLattice of
        [ _, axis ] ->
            axis

        _ ->
            "?"


validCenterings : Model -> List Centering
validCenterings model =
    case parseLatticeSystem model.bravaisLattice of
        Err _ ->
            allCenterings

        Ok cs ->
            centeringsForSystem cs


viewStructured : Model -> Html Msg
viewStructured model =
    div_
        [ div [ class "d-lg-flex gap-3 amarcord-input-no-arrows" ]
            [ div [ class "input-group input-group-sm" ]
                [ select
                    [ class "form-select"
                    , onInput ChangeSystem
                    ]
                    (List.map (\cs -> option [ selected (model.bravaisLattice == cs) ] [ text cs ]) allCrystalSystems
                        ++ (if not (List.member model.bravaisLattice allCrystalSystems) then
                                [ option [ selected True ]
                                    [ if model.bravaisLattice == "" then
                                        text "-"

                                      else
                                        text ("invalid: " ++ model.bravaisLattice)
                                    ]
                                ]

                            else
                                []
                           )
                    )
                , select
                    [ class "form-select"
                    , onInput ChangeCentering
                    ]
                    (List.map
                        (\cs ->
                            option [ selected (model.crystalCentering == centeringToString cs) ]
                                [ text
                                    (centeringToString cs)
                                ]
                        )
                        (validCenterings model)
                        ++ (if
                                not
                                    (List.member model.crystalCentering
                                        (List.map centeringToString (validCenterings model))
                                    )
                            then
                                [ option [ selected True ]
                                    [ if model.crystalCentering == "" then
                                        text "-"

                                      else
                                        text ("invalid: " ++ model.crystalCentering)
                                    ]
                                ]

                            else
                                []
                           )
                    )
                ]
            , div [ class "input-group input-group-sm" ]
                [ span [ class "input-group-text" ] [ text "a" ]
                , input_
                    [ type_ "number"
                    , class "form-control"
                    , value model.cellA
                    , onInput ChangeA
                    ]
                , span [ class "input-group-text" ] [ text "b" ]
                , input_
                    [ type_ "number"
                    , class "form-control"
                    , value model.cellB
                    , onInput ChangeB
                    ]
                , span [ class "input-group-text" ] [ text "c" ]
                , input_
                    [ type_ "number"
                    , class "form-control"
                    , value model.cellC
                    , onInput ChangeC
                    ]
                , span [ class "text-nowrap input-group-text" ] [ text "Å" ]
                ]
            , div [ class "input-group input-group-sm" ]
                [ span [ class "input-group-text" ] [ text "α" ]
                , input_
                    [ type_ "number"
                    , class "form-control"
                    , value model.cellAlpha
                    , onInput ChangeAlpha
                    ]
                , span [ class "input-group-text" ] [ text "β" ]
                , input_
                    [ type_ "number"
                    , class "form-control"
                    , value model.cellBeta
                    , onInput ChangeBeta
                    ]
                , span [ class "input-group-text" ] [ text "γ" ]
                , input_
                    [ type_ "number"
                    , class "form-control"
                    , value model.cellGamma
                    , onInput ChangeGamma
                    ]
                , span [ class "input-group-text" ] [ text "°" ]
                ]
            , div_
                [ button
                    [ type_ "button"
                    , class "btn btn-secondary btn-sm text-nowrap"
                    , onClick ToggleAsText
                    ]
                    [ text "As text" ]
                ]
            , div_
                [ button
                    [ type_ "button"
                    , class "btn btn-secondary btn-sm text-nowrap"
                    , onClick Clear
                    ]
                    [ text "Clear" ]
                ]
            ]
        , case parseModel model of
            -- Here we have an incomplete cell description - no need to show parse errors
            Err _ ->
                text ""

            Ok parsedModel ->
                div_ [ viewPossibleIssue parsedModel ]
        ]


modelAsText : Model -> String
modelAsText model =
    case model.asText of
        IsTextual t _ ->
            t

        IsStructured ->
            String.trim <|
                let
                    lengths =
                        if String.trim (model.cellA ++ model.cellB ++ model.cellC) == "" then
                            ""

                        else
                            "("
                                ++ model.cellA
                                ++ " "
                                ++ model.cellB
                                ++ " "
                                ++ model.cellC
                                ++ ")"

                    angles =
                        if String.trim (model.cellAlpha ++ model.cellBeta ++ model.cellGamma) == "" then
                            ""

                        else
                            "("
                                ++ model.cellAlpha
                                ++ " "
                                ++ model.cellBeta
                                ++ " "
                                ++ model.cellGamma
                                ++ ")"

                    latticeSystem =
                        latticeSystemAndAxisToLatticeSystem model.bravaisLattice

                    centering =
                        model.crystalCentering

                    uniqueAxis =
                        if String.trim (latticeSystem ++ centering) == "" then
                            ""

                        else
                            latticeSystemAndAxisToAxis model.bravaisLattice
                in
                latticeSystem
                    ++ " "
                    ++ centering
                    ++ " "
                    ++ uniqueAxis
                    ++ " "
                    ++ lengths
                    ++ " "
                    ++ angles


update : Msg -> Model -> Model
update msg model =
    case msg of
        CancelAsText ->
            { model | asText = IsStructured }

        ToggleAsText ->
            case model.asText of
                IsStructured ->
                    { model | asText = IsTextual (modelAsText model) Nothing }

                IsTextual asText _ ->
                    if String.trim asText == "" then
                        { model
                            | bravaisLattice = ""
                            , crystalCentering = ""
                            , cellA = ""
                            , cellB = ""
                            , cellC = ""
                            , cellAlpha = ""
                            , cellBeta = ""
                            , cellGamma = ""
                            , asText = IsStructured
                        }

                    else
                        case parseCellDescription asText of
                            Err e ->
                                { model | asText = IsTextual asText (Just e) }

                            Ok { bravaisLattice, cellA, cellB, cellC, cellAlpha, cellBeta, cellGamma } ->
                                { model
                                    | bravaisLattice = bravaisLatticeToStringNoCentering bravaisLattice
                                    , crystalCentering = centeringToString (bravaisLatticeCentering bravaisLattice)
                                    , cellA = String.fromFloat cellA
                                    , cellB = String.fromFloat cellB
                                    , cellC = String.fromFloat cellC
                                    , cellAlpha = String.fromFloat cellAlpha
                                    , cellBeta = String.fromFloat cellBeta
                                    , cellGamma = String.fromFloat cellGamma
                                    , asText = IsStructured
                                }

        ChangeCentering newCentering ->
            case parseCentering newCentering of
                Nothing ->
                    model

                Just _ ->
                    { model | crystalCentering = newCentering }

        ChangeSystem newSystem ->
            case ( latticeSystemFromString newSystem, centeringFromString model.crystalCentering ) of
                ( Just parsedSystem, Just parsedCentering ) ->
                    if List.member parsedCentering (centeringsForSystem parsedSystem) then
                        { model | bravaisLattice = newSystem }

                    else
                        -- Centering doesn't apply for the new system anymore - reset it
                        { model | bravaisLattice = newSystem, crystalCentering = "" }

                -- must be the centering is unset yet, so we just change the lattice
                _ ->
                    { model | bravaisLattice = newSystem }

        ChangeA newA ->
            { model | cellA = newA }

        ChangeB newB ->
            { model | cellB = newB }

        ChangeC newC ->
            { model | cellC = newC }

        ChangeAlpha newAlpha ->
            { model | cellAlpha = newAlpha }

        ChangeBeta newBeta ->
            { model | cellBeta = newBeta }

        ChangeGamma newGamma ->
            { model | cellGamma = newGamma }

        ChangeAsText newText ->
            case model.asText of
                IsStructured ->
                    model

                IsTextual _ _ ->
                    { model | asText = IsTextual newText Nothing }

        Clear ->
            init ""
