module Amarcord.Pages.DataSets exposing (..)

import Amarcord.API.ExperimentType exposing (ExperimentType)
import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.Attributo exposing (Attributo, AttributoType, attributoMapToListOfAttributi, convertAttributoFromApi, convertAttributoMapFromApi, emptyAttributoMap)
import Amarcord.AttributoHtml exposing (AttributoFormMsg(..), AttributoNameWithValueUpdate, EditableAttributiAndOriginal, EditableAttributo, convertEditValues, createEditableAttributi, editEditableAttributi, emptyEditableAttributiAndOriginal, viewAttributoForm)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert, viewRemoteDataHttp)
import Amarcord.Chemical exposing (Chemical, chemicalIdDict, chemicalTypeFromApi, convertChemicalFromApi)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Html exposing (form_, h1_, h5_, tbody_, td_, th_, thead_, tr_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.Util exposing (HereAndNow, listContainsBy)
import Api.Data exposing (JsonCreateDataSetOutput, JsonDataSet, JsonDeleteDataSetOutput, JsonExperimentType, JsonReadDataSets)
import Api.Request.Datasets exposing (createDataSetApiDataSetsPost, deleteDataSetApiDataSetsDelete, readDataSetsApiDataSetsBeamtimeIdGet)
import Dict
import Html exposing (Html, button, div, h4, option, select, table, text)
import Html.Attributes exposing (class, disabled, selected, type_, value)
import Html.Events exposing (onClick, onInput)
import List.Extra as ListExtra exposing (find)
import Maybe.Extra exposing (isNothing)
import RemoteData exposing (RemoteData(..), fromResult)
import String
import Time exposing (Zone)


type Msg
    = DataSetCreated (Result HttpError JsonCreateDataSetOutput)
    | DataSetDeleted (Result HttpError JsonDeleteDataSetOutput)
    | DataSetsReceived (Result HttpError JsonReadDataSets)
    | DataSetDeleteSubmit Int
    | DataSetExperimentTypeChange String
    | DataSetAttributiChange AttributoNameWithValueUpdate
    | DataSetSubmit
    | AddDataSet
    | CancelAddDataSet


convertExperimentTypeFromApi : JsonExperimentType -> ExperimentType
convertExperimentTypeFromApi et =
    { id = et.id
    , name = et.name
    , attributi =
        List.map
            (\attributo_id_and_role ->
                { id = attributo_id_and_role.id
                , role = chemicalTypeFromApi attributo_id_and_role.role
                }
            )
            et.attributi
    }


type alias NewDataSet =
    { experimentType : Maybe ExperimentType
    , attributi : EditableAttributiAndOriginal
    }


type alias DataSetModel =
    { createRequest : RemoteData HttpError JsonCreateDataSetOutput
    , deleteRequest : RemoteData HttpError JsonDeleteDataSetOutput
    , newDataSet : Maybe NewDataSet
    , dataSets : RemoteData HttpError JsonReadDataSets
    , submitErrors : List String
    , zone : Zone
    , beamtimeId : BeamtimeId
    }


pageTitle : DataSetModel -> String
pageTitle _ =
    "Data Sets"


readDataSetsWrapper : Int -> Cmd Msg
readDataSetsWrapper beamtimeId =
    send DataSetsReceived (readDataSetsApiDataSetsBeamtimeIdGet beamtimeId)


initDataSet : HereAndNow -> BeamtimeId -> ( DataSetModel, Cmd Msg )
initDataSet hereAndNow beamtimeId =
    ( { createRequest = NotAsked
      , deleteRequest = NotAsked
      , dataSets = Loading
      , submitErrors = []
      , newDataSet = Nothing
      , zone = hereAndNow.zone
      , beamtimeId = beamtimeId
      }
    , readDataSetsWrapper beamtimeId
    )


updateDataSet : Msg -> DataSetModel -> ( DataSetModel, Cmd Msg )
updateDataSet msg model =
    case msg of
        DataSetCreated result ->
            ( { model | createRequest = fromResult result }, readDataSetsWrapper model.beamtimeId )

        DataSetDeleted result ->
            ( { model | deleteRequest = fromResult result }, readDataSetsWrapper model.beamtimeId )

        DataSetsReceived result ->
            ( { model | dataSets = fromResult result }, Cmd.none )

        DataSetDeleteSubmit dataSetId ->
            ( { model | deleteRequest = Loading }, send DataSetDeleted (deleteDataSetApiDataSetsDelete { id = dataSetId }) )

        AddDataSet ->
            ( { model | newDataSet = Just { experimentType = Nothing, attributi = emptyEditableAttributiAndOriginal } }, Cmd.none )

        CancelAddDataSet ->
            ( { model | newDataSet = Nothing }, Cmd.none )

        DataSetAttributiChange update ->
            case model.newDataSet of
                Nothing ->
                    ( model, Cmd.none )

                Just newDataSetForm ->
                    let
                        newDataSet =
                            { experimentType = newDataSetForm.experimentType
                            , attributi =
                                { originalAttributi = newDataSetForm.attributi.originalAttributi
                                , editableAttributi = editEditableAttributi newDataSetForm.attributi.editableAttributi update
                                }
                            }
                    in
                    ( { model | newDataSet = Just newDataSet }, Cmd.none )

        DataSetSubmit ->
            case model.newDataSet of
                Nothing ->
                    ( model, Cmd.none )

                Just newDataSet ->
                    case newDataSet.experimentType of
                        Nothing ->
                            ( model, Cmd.none )

                        Just experimentType ->
                            case convertEditValues model.zone newDataSet.attributi of
                                Err errorList ->
                                    ( { model | submitErrors = List.map (\( attributoId, errorMessage ) -> String.fromInt attributoId ++ ": " ++ errorMessage) errorList }, Cmd.none )

                                Ok editedAttributi ->
                                    ( { model | createRequest = Loading }
                                    , send DataSetCreated
                                        (createDataSetApiDataSetsPost
                                            { experimentTypeId = experimentType.id
                                            , attributi = attributoMapToListOfAttributi editedAttributi
                                            }
                                        )
                                    )

        DataSetExperimentTypeChange newExperimentTypeIdStr ->
            case model.dataSets of
                Success { attributi, experimentTypes } ->
                    let
                        convertedAttributi =
                            List.map convertAttributoFromApi attributi

                        matchingExperimentType : Maybe ExperimentType
                        matchingExperimentType =
                            String.toInt newExperimentTypeIdStr
                                |> Maybe.andThen
                                    (\newExperimentTypeId ->
                                        let
                                            convertedExperimentTypes =
                                                List.map convertExperimentTypeFromApi experimentTypes
                                        in
                                        find (\et -> et.id == newExperimentTypeId) convertedExperimentTypes
                                    )

                        attributoInExperimentType : Attributo AttributoType -> Bool
                        attributoInExperimentType attributo =
                            case matchingExperimentType of
                                Nothing ->
                                    False

                                Just et ->
                                    listContainsBy (\a -> a.id == attributo.id) et.attributi

                        newDataSet : Maybe NewDataSet
                        newDataSet =
                            Just
                                { experimentType = matchingExperimentType
                                , attributi = createEditableAttributi model.zone (List.filter attributoInExperimentType convertedAttributi) emptyAttributoMap
                                }
                    in
                    ( { model | newDataSet = newDataSet }, Cmd.none )

                _ ->
                    ( model, Cmd.none )


viewEditForm : ExperimentType -> List (Chemical Int a b) -> EditableAttributiAndOriginal -> List (Html Msg)
viewEditForm et chemicals =
    let
        attributoFormMsgToMsg : AttributoFormMsg -> Msg
        attributoFormMsgToMsg x =
            case x of
                AttributoFormValueUpdate vu ->
                    DataSetAttributiChange vu

                AttributoFormSubmit ->
                    DataSetSubmit

        viewAttributoFormWithRole : EditableAttributo -> Html AttributoFormMsg
        viewAttributoFormWithRole e =
            viewAttributoForm chemicals
                (Maybe.map .role <|
                    ListExtra.find (\awr -> awr.id == e.id) <|
                        et.attributi
                )
                e
    in
    List.map (\attributo -> Html.map attributoFormMsgToMsg (viewAttributoFormWithRole attributo)) << .editableAttributi


view : DataSetModel -> Html Msg
view model =
    div [ class "container" ] <| viewDataSet model


viewDataSet : DataSetModel -> List (Html Msg)
viewDataSet model =
    case model.dataSets of
        NotAsked ->
            List.singleton <| text ""

        Loading ->
            List.singleton <| loadingBar "Loading data set..."

        Failure e ->
            List.singleton <|
                makeAlert [ AlertDanger ] <|
                    [ h4 [ class "alert-heading" ] [ text "Failed to retrieve data sets" ]
                    , showError e
                    ]

        Success { chemicals, attributi, dataSets, experimentTypes } ->
            let
                experimentTypesById : Dict.Dict Int String
                experimentTypesById =
                    List.foldl (\et -> Dict.insert et.id et.name) Dict.empty experimentTypes

                viewRow : JsonDataSet -> Html Msg
                viewRow ds =
                    tr_
                        [ td_ [ text (String.fromInt ds.id) ]
                        , td_ [ text (Maybe.withDefault "" <| Dict.get ds.experimentTypeId experimentTypesById) ]
                        , td_
                            [ viewDataSetTable
                                (List.map convertAttributoFromApi attributi)
                                model.zone
                                (chemicalIdDict (List.map convertChemicalFromApi chemicals))
                                (convertAttributoMapFromApi ds.attributi)
                                False
                                False
                                Nothing
                            ]
                        , td_ [ button [ class "btn btn-sm btn-danger", onClick (DataSetDeleteSubmit ds.id) ] [ icon { name = "trash" } ] ]
                        ]

                viewExperimentTypeOption : Maybe ExperimentType -> ExperimentType -> Html Msg
                viewExperimentTypeOption currentEt et =
                    option
                        [ selected (Maybe.map .id currentEt == Just et.id)
                        , value (String.fromInt et.id)
                        ]
                        [ text et.name ]

                newDataSetForm =
                    case model.newDataSet of
                        Nothing ->
                            button [ type_ "button", onClick AddDataSet, class "btn btn-primary mb-3" ] [ icon { name = "plus-lg" }, text " Add Data Set" ]

                        Just newDataSet ->
                            let
                                editForm =
                                    case newDataSet.experimentType of
                                        Nothing ->
                                            []

                                        Just et ->
                                            viewEditForm et (List.map convertChemicalFromApi chemicals) newDataSet.attributi
                            in
                            form_
                                ([ h5_ [ text "New Data Set" ]
                                 , div [ class "mb-3" ]
                                    [ select [ class "form-select", onInput DataSetExperimentTypeChange ]
                                        (option
                                            [ selected (isNothing newDataSet.experimentType)
                                            ]
                                            [ text "«no value»" ]
                                            :: List.map (viewExperimentTypeOption newDataSet.experimentType) (List.map convertExperimentTypeFromApi experimentTypes)
                                        )
                                    ]
                                 ]
                                    ++ editForm
                                    ++ [ button
                                            [ type_ "button"
                                            , class "btn btn-primary mb-3 me-3"
                                            , onClick DataSetSubmit
                                            , disabled (newDataSet.experimentType == Nothing)
                                            ]
                                            [ icon { name = "plus" }, text " Add Data Set" ]
                                       , button
                                            [ type_ "button"
                                            , class "btn btn-secondary mb-3"
                                            , onClick CancelAddDataSet
                                            ]
                                            [ icon { name = "x-lg" }, text " Cancel" ]
                                       ]
                                )
            in
            [ h1_ [ text "Data Sets" ]
            , newDataSetForm
            , viewRemoteDataHttp "Deletion successful!" model.deleteRequest
            , viewRemoteDataHttp "Creation successful!" model.createRequest
            , table [ class "table table-striped" ]
                [ thead_
                    [ tr_
                        [ th_ [ text "ID" ]
                        , th_ [ text "Experiment Type" ]
                        , th_ [ text "Attributi" ]
                        , th_ [ text "Actions" ]
                        ]
                    ]
                , tbody_ (List.map viewRow dataSets)
                ]
            ]
