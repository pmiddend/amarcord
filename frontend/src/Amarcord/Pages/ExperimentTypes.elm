module Amarcord.Pages.ExperimentTypes exposing (..)

import Amarcord.API.ExperimentType exposing (ExperimentTypeId)
import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.Attributo exposing (Attributo, AttributoId, AttributoType, attributoIsChemicalId, convertAttributoFromApi)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert, viewRemoteDataHttp)
import Amarcord.Chemical exposing (chemicalTypeFromApi, chemicalTypeToString)
import Amarcord.Html exposing (br_, div_, form_, h1_, h4_, h5_, input_, li_, onIntInput, tbody_, td_, th_, thead_, tr_, ul_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.Util exposing (forgetMsgInput, monthToNumericString)
import Api.Data exposing (ChemicalType(..), JsonAttributiIdAndRole, JsonBeamtime, JsonCopyExperimentTypesOutput, JsonCreateExperimentTypeOutput, JsonDeleteExperimentTypeOutput, JsonExperimentType, JsonExperimentTypeAndRuns, JsonReadBeamtime, JsonReadExperimentTypes)
import Api.Request.Beamtimes exposing (readBeamtimesApiBeamtimesGet)
import Api.Request.Experimenttypes exposing (copyExperimentTypesApiCopyExperimentTypesPost, createExperimentTypeApiExperimentTypesPost, deleteExperimentTypeApiExperimentTypesDelete, readExperimentTypesApiExperimentTypesBeamtimeIdGet)
import Html exposing (Html, button, div, h4, input, label, option, select, table, td, text)
import Html.Attributes exposing (checked, class, disabled, for, id, selected, type_, value)
import Html.Events exposing (onClick, onInput)
import List.Extra as ListExtra
import Maybe.Extra exposing (isNothing)
import RemoteData exposing (RemoteData(..), fromResult, isLoading)
import Set exposing (Set)
import String
import Time exposing (Zone, millisToPosix, toMonth, toYear)


type alias CopyFromOtherBeamtimeModel =
    { beamtimeRequest : RemoteData HttpError JsonReadBeamtime
    , copyRequest : RemoteData HttpError JsonCopyExperimentTypesOutput
    , selectedBeamtime : Maybe BeamtimeId
    , zone : Zone
    , beamtimeId : BeamtimeId
    }


type CopyFromOtherBeamtimeMsg
    = BeamtimesReceived (Result HttpError JsonReadBeamtime)
    | SelectionChanged BeamtimeId
    | CopyRequestReceived (Result HttpError JsonCopyExperimentTypesOutput)
    | SubmitCopy
    | CancelCopy


copyFromOtherBeamtimeInit : Zone -> BeamtimeId -> ( CopyFromOtherBeamtimeModel, Cmd CopyFromOtherBeamtimeMsg )
copyFromOtherBeamtimeInit zone beamtimeId =
    ( { beamtimeRequest = Loading
      , copyRequest = NotAsked
      , selectedBeamtime = Nothing
      , zone = zone
      , beamtimeId = beamtimeId
      }
    , send BeamtimesReceived readBeamtimesApiBeamtimesGet
    )


copyFromOtherBeamtimeView : CopyFromOtherBeamtimeModel -> Html CopyFromOtherBeamtimeMsg
copyFromOtherBeamtimeView { zone, beamtimeRequest, copyRequest, selectedBeamtime } =
    let
        viewBeamtime : JsonBeamtime -> Html CopyFromOtherBeamtimeMsg
        viewBeamtime { id, title, start } =
            let
                startAsPosix =
                    millisToPosix start
            in
            option
                [ value (String.fromInt id)
                , selected (selectedBeamtime == Just id)
                ]
                [ text <|
                    title
                        ++ " / "
                        ++ String.fromInt
                            (toYear zone startAsPosix)
                        ++ "-"
                        ++ monthToNumericString (toMonth zone startAsPosix)
                ]
    in
    case beamtimeRequest of
        NotAsked ->
            text ""

        Loading ->
            text ""

        Failure e ->
            makeAlert [ AlertDanger ] <|
                [ h4 [ class "alert-heading" ] [ text "Failed to retrieve beamtimes" ]
                , showError e
                ]

        Success { beamtimes } ->
            form_
                [ h4_ [ icon { name = "terminal" }, text " Select beamtime to copy from" ]
                , select
                    [ id "beam-time-to-copy"
                    , class "form-select mb-3"
                    , onIntInput SelectionChanged
                    ]
                    (option
                        [ disabled True
                        , value ""
                        , selected (isNothing selectedBeamtime)
                        ]
                        [ text "« choose a chemical »" ]
                        :: List.map viewBeamtime beamtimes
                    )
                , div [ class "hstack gap-3 mb-3" ]
                    [ button
                        [ class "btn btn-primary"
                        , onClick SubmitCopy
                        , disabled (isNothing selectedBeamtime || isLoading copyRequest)
                        , type_ "button"
                        ]
                        [ icon { name = "plus-lg" }, text " Copy into this beamtime" ]
                    , button
                        [ class "btn btn-secondary"
                        , onClick CancelCopy
                        , type_ "button"
                        ]
                        [ icon { name = "x-lg" }, text " Cancel" ]
                    ]
                , case copyRequest of
                    Loading ->
                        loadingBar "Copying..."

                    NotAsked ->
                        text ""

                    Success _ ->
                        div [ class "badge text-bg-success" ] [ text "Copy complete! Please refresh." ]

                    Failure e ->
                        makeAlert [ AlertDanger ] <|
                            [ h4 [ class "alert-heading" ] [ text "Failed to copy!" ]
                            , showError e
                            ]
                ]


copyFromOtherBeamtimeUpdate : CopyFromOtherBeamtimeMsg -> CopyFromOtherBeamtimeModel -> ( CopyFromOtherBeamtimeModel, Cmd CopyFromOtherBeamtimeMsg )
copyFromOtherBeamtimeUpdate msg model =
    case msg of
        BeamtimesReceived r ->
            ( { model | beamtimeRequest = fromResult r }, Cmd.none )

        SelectionChanged new ->
            ( { model | selectedBeamtime = Just new }, Cmd.none )

        CopyRequestReceived r ->
            ( { model | copyRequest = fromResult r }, Cmd.none )

        SubmitCopy ->
            case model.selectedBeamtime of
                Nothing ->
                    ( model, Cmd.none )

                Just copySource ->
                    ( { model | copyRequest = Loading }
                    , send CopyRequestReceived
                        (copyExperimentTypesApiCopyExperimentTypesPost
                            { fromBeamtime = copySource
                            , toBeamtime = model.beamtimeId
                            }
                        )
                    )

        CancelCopy ->
            ( { model | copyRequest = NotAsked }, Cmd.none )


type Msg
    = ExperimentTypeCreated (Result HttpError JsonCreateExperimentTypeOutput)
    | ExperimentTypeDeleted (Result HttpError JsonDeleteExperimentTypeOutput)
    | ExperimentTypesReceived (Result HttpError JsonReadExperimentTypes)
    | ExperimentTypeNameChange String
    | ExperimentTypeDeleteSubmit ExperimentTypeId
    | ExperimentTypeAttributoRoleChange AttributoId ChemicalType
    | ExperimentTypeSubmit
    | AddOrRemoveAttributo AttributoId Bool
    | AddExperimentType
    | CancelAddExperimentType
    | InitCopyFromOtherBeamtime
    | CopyFromOtherBeamtimeMessage CopyFromOtherBeamtimeMsg


type alias NewExperimentType =
    { name : String
    , attributi : List AttributoId
    , solutionAttributi : Set AttributoId
    , createRequest : RemoteData HttpError {}
    }


type alias Model =
    { deleteRequest : RemoteData HttpError {}
    , experimentTypes : RemoteData HttpError JsonReadExperimentTypes
    , newExperimentType : Maybe NewExperimentType
    , beamtimeId : BeamtimeId
    , zone : Zone
    , copyFromOtherBeamtime : Maybe CopyFromOtherBeamtimeModel
    }


pageTitle : Model -> String
pageTitle _ =
    "Experiment Types"


init : Zone -> BeamtimeId -> ( Model, Cmd Msg )
init zone beamtimeId =
    ( { deleteRequest = NotAsked
      , experimentTypes = Loading
      , newExperimentType = Nothing
      , beamtimeId = beamtimeId
      , copyFromOtherBeamtime = Nothing
      , zone = zone
      }
    , send ExperimentTypesReceived (readExperimentTypesApiExperimentTypesBeamtimeIdGet beamtimeId)
    )


updateNewExperimentType :
    Model
    -> (NewExperimentType -> ( NewExperimentType, Cmd Msg ))
    -> ( Model, Cmd Msg )
updateNewExperimentType model f =
    case model.newExperimentType of
        Nothing ->
            ( model, Cmd.none )

        Just newExperimentType ->
            let
                ( result, cmds ) =
                    f newExperimentType
            in
            ( { model | newExperimentType = Just result }, cmds )


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        InitCopyFromOtherBeamtime ->
            let
                ( newModel, cmds ) =
                    copyFromOtherBeamtimeInit model.zone model.beamtimeId
            in
            ( { model | copyFromOtherBeamtime = Just newModel }, Cmd.map CopyFromOtherBeamtimeMessage cmds )

        CopyFromOtherBeamtimeMessage CancelCopy ->
            ( { model | copyFromOtherBeamtime = Nothing }, Cmd.none )

        CopyFromOtherBeamtimeMessage subMsg ->
            case model.copyFromOtherBeamtime of
                Nothing ->
                    ( model, Cmd.none )

                Just copyFromOtherBeamtimeModel ->
                    let
                        ( newModel, cmds ) =
                            copyFromOtherBeamtimeUpdate subMsg copyFromOtherBeamtimeModel
                    in
                    ( { model | copyFromOtherBeamtime = Just newModel }, Cmd.map CopyFromOtherBeamtimeMessage cmds )

        ExperimentTypeCreated result ->
            updateNewExperimentType model
                (\newExperimentType ->
                    ( { newExperimentType | createRequest = fromResult (forgetMsgInput result) }
                    , send ExperimentTypesReceived (readExperimentTypesApiExperimentTypesBeamtimeIdGet model.beamtimeId)
                    )
                )

        ExperimentTypeDeleted result ->
            ( { model | deleteRequest = fromResult (forgetMsgInput result) }, send ExperimentTypesReceived (readExperimentTypesApiExperimentTypesBeamtimeIdGet model.beamtimeId) )

        ExperimentTypesReceived result ->
            ( { model | experimentTypes = fromResult result }, Cmd.none )

        ExperimentTypeNameChange newName ->
            updateNewExperimentType model (\newExperimentType -> ( { newExperimentType | name = newName }, Cmd.none ))

        ExperimentTypeAttributoRoleChange attributoId newRole ->
            let
                newSolutionAttributi newExType =
                    if newRole == ChemicalTypeSolution then
                        Set.insert attributoId newExType.solutionAttributi

                    else
                        Set.remove attributoId newExType.solutionAttributi
            in
            updateNewExperimentType
                model
                (\newExperimentType -> ( { newExperimentType | solutionAttributi = newSolutionAttributi newExperimentType }, Cmd.none ))

        ExperimentTypeDeleteSubmit experimentTypeId ->
            ( { model | deleteRequest = Loading }, send ExperimentTypeDeleted (deleteExperimentTypeApiExperimentTypesDelete { id = experimentTypeId }) )

        ExperimentTypeSubmit ->
            let
                createAttributoWithRole : NewExperimentType -> AttributoId -> JsonAttributiIdAndRole
                createAttributoWithRole newExp attributoId =
                    { id = attributoId
                    , role =
                        if Set.member attributoId newExp.solutionAttributi then
                            ChemicalTypeSolution

                        else
                            ChemicalTypeCrystal
                    }
            in
            updateNewExperimentType
                model
                (\newExperimentType ->
                    ( { newExperimentType | createRequest = Loading, name = "" }
                    , send ExperimentTypeCreated
                        (createExperimentTypeApiExperimentTypesPost
                            { name = newExperimentType.name
                            , beamtimeId = model.beamtimeId
                            , attributi = List.map (createAttributoWithRole newExperimentType) newExperimentType.attributi
                            }
                        )
                    )
                )

        AddOrRemoveAttributo attributoName add ->
            updateNewExperimentType model
                (\newExperimentType ->
                    ( { newExperimentType
                        | attributi =
                            if add then
                                attributoName :: newExperimentType.attributi

                            else
                                List.filter (\x -> x /= attributoName) newExperimentType.attributi
                      }
                    , Cmd.none
                    )
                )

        AddExperimentType ->
            ( { model
                | newExperimentType =
                    Just
                        { name = ""
                        , attributi = []
                        , createRequest = NotAsked
                        , solutionAttributi = Set.empty
                        }
              }
            , Cmd.none
            )

        CancelAddExperimentType ->
            ( { model | newExperimentType = Nothing }, Cmd.none )


view : Model -> Html Msg
view model =
    div [ class "container" ] <| viewExperimentType model


viewExperimentType : Model -> List (Html Msg)
viewExperimentType model =
    let
        viewAttributoWithRole : List (Attributo AttributoType) -> JsonAttributiIdAndRole -> Html msg
        viewAttributoWithRole attributi { id, role } =
            case ListExtra.find (\a -> a.id == id) attributi of
                Nothing ->
                    li_ [ text ("attributo " ++ String.fromInt id ++ "not found in attributo list") ]

                Just { type_, name } ->
                    if attributoIsChemicalId type_ then
                        li_ [ text (name ++ " (" ++ chemicalTypeToString (chemicalTypeFromApi role) ++ ")") ]

                    else
                        li_ [ text name ]

        viewRow : List (Attributo AttributoType) -> List JsonExperimentTypeAndRuns -> JsonExperimentType -> Html Msg
        viewRow attributi etWithRuns et =
            let
                runs =
                    Maybe.withDefault []
                        (ListExtra.findMap
                            (\etwr ->
                                if etwr.id == et.id then
                                    Just etwr.runs

                                else
                                    Nothing
                            )
                            etWithRuns
                        )
            in
            tr_
                [ td_ [ text (String.fromInt et.id) ]
                , td_ [ text et.name ]
                , td_ [ ul_ (List.map (viewAttributoWithRole attributi) et.attributi) ]
                , td [ class "text-nowrap" ] (List.intersperse br_ <| List.map text runs)
                , td_ [ button [ class "btn btn-danger btn-sm", onClick (ExperimentTypeDeleteSubmit et.id), disabled (not <| List.isEmpty runs) ] [ icon { name = "trash" } ] ]
                ]

        viewAttributoCheckbox : List AttributoId -> Attributo AttributoType -> Html Msg
        viewAttributoCheckbox attributi a =
            div [ class "form-check" ]
                [ input
                    [ class "form-check-input"
                    , type_ "checkbox"
                    , id ("et-" ++ a.name)
                    , checked (List.member a.id attributi)
                    , onInput (always <| AddOrRemoveAttributo a.id (not (List.member a.id attributi)))
                    ]
                    []
                , label [ class "form-check-label", for ("et-" ++ a.name) ] [ text a.name ]
                ]

        newExperimentTypeForm =
            case model.newExperimentType of
                Nothing ->
                    case model.copyFromOtherBeamtime of
                        Nothing ->
                            div [ class "hstack gap-3 mb-3" ]
                                [ button
                                    [ onClick AddExperimentType
                                    , class "btn btn-primary"
                                    , type_ "button"
                                    ]
                                    [ icon { name = "plus-lg" }, text " Add Experiment Type" ]
                                , div [ class "vr" ] []
                                , button
                                    [ class "btn btn-primary"
                                    , onClick InitCopyFromOtherBeamtime
                                    ]
                                    [ icon { name = "terminal" }, text " Copy from prior beamtime" ]
                                ]

                        Just copyFromOtherBeamtimeModel ->
                            Html.map
                                CopyFromOtherBeamtimeMessage
                                (copyFromOtherBeamtimeView copyFromOtherBeamtimeModel)

                Just newExperimentType ->
                    form_
                        [ h5_ [ text "New experiment type" ]
                        , div [ class "mb-3" ]
                            [ label [ for "et-name", class "form-label" ] [ text "Name" ]
                            , input_ [ class "form-control", type_ "text", value newExperimentType.name, onInput ExperimentTypeNameChange ]
                            ]
                        , case model.experimentTypes of
                            Success { attributi } ->
                                let
                                    attributiCheckboxes =
                                        List.map (viewAttributoCheckbox newExperimentType.attributi << convertAttributoFromApi) attributi

                                    viewAttributoRoleRadio : Attributo AttributoType -> Html Msg
                                    viewAttributoRoleRadio a =
                                        if attributoIsChemicalId a.type_ && List.member a.id newExperimentType.attributi then
                                            tr_
                                                [ td_ [ text a.name ]
                                                , td_
                                                    [ div [ class "form-check form-check-inline" ]
                                                        [ input_
                                                            [ id (a.name ++ "-chemical-role-solution")
                                                            , class "form-check-input"
                                                            , type_ "radio"
                                                            , checked (Set.member a.id newExperimentType.solutionAttributi)
                                                            , onInput (\_ -> ExperimentTypeAttributoRoleChange a.id ChemicalTypeSolution)
                                                            ]
                                                        , label [ for (a.name ++ "-chemical-role-solution") ] [ text "Solution" ]
                                                        ]
                                                    , div [ class "form-check form-check-inline" ]
                                                        [ input_
                                                            [ id (a.name ++ "-chemical-role-crystal")
                                                            , class "form-check-input"
                                                            , type_ "radio"
                                                            , checked (not <| Set.member a.id newExperimentType.solutionAttributi)
                                                            , onInput (\_ -> ExperimentTypeAttributoRoleChange a.id ChemicalTypeCrystal)
                                                            ]
                                                        , label [ for (a.name ++ "-chemical-role-crystal") ] [ text "Crystal" ]
                                                        ]
                                                    ]
                                                ]

                                        else
                                            tr_ [ td_ [ text a.name ], td_ [] ]

                                    attributiProperties =
                                        table []
                                            [ tbody_
                                                (List.map
                                                    (viewAttributoRoleRadio << convertAttributoFromApi)
                                                    attributi
                                                )
                                            ]
                                in
                                div [ class "mb-3 row" ]
                                    [ div [ class "col" ] [ h5_ [ text "Which Attributi" ], div_ attributiCheckboxes ]
                                    , div [ class "col" ] [ h5_ [ text "Attributi properties" ], attributiProperties ]
                                    ]

                            _ ->
                                text ""
                        , button [ type_ "button", class "btn btn-primary mb-3 me-3", onClick ExperimentTypeSubmit, disabled (List.isEmpty newExperimentType.attributi) ]
                            [ icon { name = "plus-lg" }, text " Add type" ]
                        , button
                            [ type_ "button"
                            , class "btn btn-secondary mb-3"
                            , onClick CancelAddExperimentType
                            ]
                            [ icon { name = "x-lg" }, text " Cancel" ]
                        , viewRemoteDataHttp "Creation successful!" newExperimentType.createRequest
                        ]
    in
    [ h1_ [ text "Experiment Types" ]
    , newExperimentTypeForm
    , viewRemoteDataHttp "Deletion successful!" model.deleteRequest
    , case model.experimentTypes of
        Failure e ->
            makeAlert [ AlertDanger ] <|
                [ h4 [ class "alert-heading" ] [ text "Failed to retrieve experiment types" ]
                , showError e
                ]

        Success experimentTypesResponse ->
            table [ class "table table-striped" ]
                [ thead_
                    [ tr_
                        [ th_ [ text "ID" ]
                        , th_ [ text "Name" ]
                        , th_ [ text "Attributi" ]
                        , th_ [ text "Runs" ]
                        , th_ [ text "Actions" ]
                        ]
                    ]
                , tbody_ (List.map (viewRow (List.map convertAttributoFromApi experimentTypesResponse.attributi) experimentTypesResponse.experimentTypeIdToRun) experimentTypesResponse.experimentTypes)
                ]

        _ ->
            text ""
    ]
