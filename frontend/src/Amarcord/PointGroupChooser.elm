module Amarcord.PointGroupChooser exposing (Model, Msg(..), PointGroup(..), init, pointGroupToString, update, view)

import Amarcord.Html exposing (div_, enumSelect, h3_, input_)
import Html exposing (Html, div, label, p, text)
import Html.Attributes exposing (checked, class, for, id, name, type_)
import Html.Events exposing (onInput)


type PointGroup
    = PointGroup String


pointGroupToString : PointGroup -> String
pointGroupToString (PointGroup s) =
    s


type LatticeType
    = Triclinic
    | Monoclinic
    | Orthorhombic
    | Tetragonal
    | Rhombohedral
    | Hexagonal
    | Cubic


allLatticeTypes : List LatticeType
allLatticeTypes =
    [ Triclinic, Monoclinic, Orthorhombic, Tetragonal, Rhombohedral, Hexagonal, Cubic ]


latticeTypeFromString : String -> Maybe LatticeType
latticeTypeFromString x =
    case x of
        "Triclinic" ->
            Just Triclinic

        "Monoclinic" ->
            Just Monoclinic

        "Orthorhombic" ->
            Just Orthorhombic

        "Tetragonal" ->
            Just Tetragonal

        "Rhombohedral" ->
            Just Rhombohedral

        "Hexagonal" ->
            Just Hexagonal

        "Cubic" ->
            Just Cubic

        _ ->
            Nothing


latticeTypeToDescription : LatticeType -> String
latticeTypeToDescription =
    latticeTypeToValue


latticeTypeToValue : LatticeType -> String
latticeTypeToValue lt =
    case lt of
        Triclinic ->
            "Triclinic"

        Monoclinic ->
            "Monoclinic"

        Orthorhombic ->
            "Orthorhombic"

        Tetragonal ->
            "Tetragonal"

        Rhombohedral ->
            "Rhombohedral"

        Hexagonal ->
            "Hexagonal"

        Cubic ->
            "Cubic"


type Axis
    = A
    | B
    | C


allAxes : List Axis
allAxes =
    [ A, B, C ]


type SohnkeOrCentrosymmetric
    = Sohnke
    | Centrosymmetric


type alias Model =
    { latticeType : LatticeType
    , sohnkeOrCentrosymmetric : SohnkeOrCentrosymmetric
    , uniqueAxis : Axis
    , chosenPointGroup : Maybe PointGroup
    }


latticeTypeHasUniqueAxis : LatticeType -> Bool
latticeTypeHasUniqueAxis lt =
    case lt of
        Triclinic ->
            False

        Monoclinic ->
            True

        Orthorhombic ->
            False

        Tetragonal ->
            True

        Rhombohedral ->
            False

        Hexagonal ->
            True

        Cubic ->
            False


uniqueAxisToString : Axis -> String
uniqueAxisToString a =
    case a of
        A ->
            "uaa"

        B ->
            "uab"

        C ->
            "uac"


additionalNote : LatticeType -> Maybe (Html msg)
additionalNote lt =
    case lt of
        Rhombohedral ->
            Just <| text <| "Looking for “H3”/“H32”? Select “Hexagonal” and use “3_H”/“321_H” respectively, or “-3_H”/“-3m1_H” to merge Friedel pairs."

        _ ->
            Nothing


modelToPointGroups : Model -> List PointGroup
modelToPointGroups { latticeType, sohnkeOrCentrosymmetric, uniqueAxis } =
    let
        uniqueAxify baseGroups =
            let
                axisString =
                    case uniqueAxis of
                        C ->
                            ""

                        _ ->
                            "_" ++ uniqueAxisToString uniqueAxis
            in
            List.map (PointGroup << (\x -> x ++ axisString)) baseGroups
    in
    case latticeType of
        Triclinic ->
            case sohnkeOrCentrosymmetric of
                Sohnke ->
                    [ PointGroup "1" ]

                Centrosymmetric ->
                    [ PointGroup "-1" ]

        Monoclinic ->
            uniqueAxify <|
                case sohnkeOrCentrosymmetric of
                    Sohnke ->
                        [ "2" ]

                    Centrosymmetric ->
                        [ "2/m" ]

        Orthorhombic ->
            [ PointGroup <|
                case sohnkeOrCentrosymmetric of
                    Sohnke ->
                        "222"

                    Centrosymmetric ->
                        "mmm"
            ]

        Tetragonal ->
            uniqueAxify <|
                case sohnkeOrCentrosymmetric of
                    Sohnke ->
                        [ "4", "422" ]

                    Centrosymmetric ->
                        [ "4/m", "4/mmm" ]

        Rhombohedral ->
            case sohnkeOrCentrosymmetric of
                Sohnke ->
                    [ PointGroup "3_R", PointGroup "32_R" ]

                Centrosymmetric ->
                    [ PointGroup "-3_R", PointGroup "-3m_R" ]

        Hexagonal ->
            uniqueAxify <|
                case sohnkeOrCentrosymmetric of
                    Sohnke ->
                        [ "3_H", "312_H", "321_H", "6", "622" ]

                    Centrosymmetric ->
                        [ "-3_H", "-31m_H", "-3m1_H", "6/m", "6/mmm" ]

        Cubic ->
            List.map PointGroup <|
                case sohnkeOrCentrosymmetric of
                    Sohnke ->
                        [ "23", "432" ]

                    Centrosymmetric ->
                        [ "m-3", "m-3m" ]


type Msg
    = LatticeTypeChange LatticeType
    | SohnkeOrCentrosymmetricChange SohnkeOrCentrosymmetric
    | PointGroupChosen PointGroup
    | UniqueAxisChange Axis


init : Model
init =
    { latticeType = Triclinic
    , sohnkeOrCentrosymmetric = Sohnke
    , uniqueAxis = C
    , chosenPointGroup = Nothing
    }


uniqueAxisToValue : Axis -> String
uniqueAxisToValue x =
    case x of
        A ->
            "a"

        B ->
            "b"

        C ->
            "c"


uniqueAxisFromValue : String -> Maybe Axis
uniqueAxisFromValue x =
    case x of
        "a" ->
            Just A

        "b" ->
            Just B

        "c" ->
            Just C

        _ ->
            Nothing


uniqueAxisToDescription : LatticeType -> Axis -> String
uniqueAxisToDescription lt x =
    case x of
        A ->
            "a (very uncommon)"

        B ->
            case lt of
                Monoclinic ->
                    "b"

                _ ->
                    "b (uncommon)"

        C ->
            "c"


view : Model -> Html Msg
view model =
    let
        makeLatticeTypeSelect =
            enumSelect [ class "form-select", id "crystfel-lattice-type" ]
                allLatticeTypes
                latticeTypeToValue
                latticeTypeToDescription
                (Maybe.map LatticeTypeChange << latticeTypeFromString)
                model.latticeType

        latticeSelectRow =
            div [ class "row form-floating mb-3" ]
                [ makeLatticeTypeSelect
                , label [ for "crystfel-lattice-type" ] [ text "Lattice type" ]
                ]

        sohnkeOrCentrosymmetricRow =
            div [ class "mb-3" ]
                [ div [ class "form-check form-check-inline" ]
                    [ input_
                        [ id "crystfel-lattice-sohnke"
                        , type_ "radio"
                        , name "crystfel-lattice-sohnke"
                        , class "form-check-input"
                        , checked (model.sohnkeOrCentrosymmetric == Sohnke)
                        , onInput (always <| SohnkeOrCentrosymmetricChange Sohnke)
                        ]
                    , label [ for "crystfel-lattice-sohnke-sohnke" ] [ text "Sohnke" ]
                    ]
                , div [ class "form-check form-check-inline" ]
                    [ input_
                        [ id "crystfel-lattice-sohnke-centrosymmetric"
                        , type_ "radio"
                        , name "crystfel-lattice-sohnke"
                        , class "form-check-input"
                        , checked (model.sohnkeOrCentrosymmetric == Centrosymmetric)
                        , onInput (always <| SohnkeOrCentrosymmetricChange Centrosymmetric)
                        ]
                    , label [ for "crystfel-lattice-sohnke-centrosymmetric" ] [ text "Centrosymmetric" ]
                    ]
                ]

        additionalNoteRow =
            case additionalNote model.latticeType of
                Nothing ->
                    text ""

                Just note ->
                    p [ class "text-muted" ] [ note ]

        makePointGroup : PointGroup -> Html Msg
        makePointGroup pg =
            div [ class "form-check form-check-inline" ]
                [ input_
                    [ id ("crystfel-point-group-" ++ pointGroupToString pg)
                    , name "crystfel-chosen-point-group"
                    , class "form-check-input"
                    , type_ "radio"
                    , checked (model.chosenPointGroup == Just pg)
                    , onInput (always <| PointGroupChosen pg)
                    ]
                , label [ for ("crystfel-point-group-" ++ pointGroupToString pg) ] [ text (pointGroupToString pg) ]
                ]

        pointGroupsRow =
            List.map makePointGroup <| modelToPointGroups model
    in
    div_
        [ latticeSelectRow
        , sohnkeOrCentrosymmetricRow
        , if latticeTypeHasUniqueAxis model.latticeType then
            let
                makeUniqueAxisSelect =
                    enumSelect [ class "form-select", id "crystfel-unique-axis" ]
                        allAxes
                        uniqueAxisToValue
                        (uniqueAxisToDescription model.latticeType)
                        (Maybe.map UniqueAxisChange << uniqueAxisFromValue)
                        model.uniqueAxis

                uniqueAxisRow =
                    div [ class "row form-floating mb-3" ]
                        [ makeUniqueAxisSelect
                        , label [ for "crystfel-unique-axis" ] [ text "Unique axis" ]
                        ]
            in
            uniqueAxisRow

          else
            text ""
        , additionalNoteRow
        , div_ <| h3_ [ text "Point groups" ] :: pointGroupsRow
        ]


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        LatticeTypeChange latticeType ->
            ( { latticeType = latticeType
              , sohnkeOrCentrosymmetric = Sohnke
              , uniqueAxis = C
              , chosenPointGroup = Nothing
              }
            , Cmd.none
            )

        SohnkeOrCentrosymmetricChange sohnkeOrCentrosymmetric ->
            ( { latticeType = model.latticeType
              , sohnkeOrCentrosymmetric = sohnkeOrCentrosymmetric
              , uniqueAxis = model.uniqueAxis
              , chosenPointGroup = Nothing
              }
            , Cmd.none
            )

        PointGroupChosen pointGroup ->
            ( { model | chosenPointGroup = Just pointGroup }, Cmd.none )

        UniqueAxisChange axis ->
            ( { model | uniqueAxis = axis, chosenPointGroup = Nothing }, Cmd.none )
