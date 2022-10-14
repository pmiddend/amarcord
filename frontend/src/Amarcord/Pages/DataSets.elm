module Amarcord.Pages.DataSets exposing (..)

import Amarcord.API.DataSet exposing (DataSet)
import Amarcord.API.ExperimentType exposing (ExperimentType, ExperimentTypeId)
import Amarcord.API.Requests exposing (DataSetResult, RequestError, httpCreateDataSet, httpDeleteDataSet, httpGetDataSets)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue, emptyAttributoMap)
import Amarcord.AttributoHtml exposing (AttributoFormMsg(..), AttributoNameWithValueUpdate, EditableAttributiAndOriginal, convertEditValues, createEditableAttributi, editEditableAttributi, emptyEditableAttributiAndOriginal, viewAttributoForm)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert, viewRemoteData)
import Amarcord.Chemical exposing (Chemical, chemicalIdDict)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Html exposing (form_, h1_, h5_, tbody_, td_, th_, thead_, tr_)
import Amarcord.Util exposing (HereAndNow)
import Dict
import Html exposing (Html, button, div, h4, option, select, table, text)
import Html.Attributes exposing (class, disabled, selected, type_, value)
import Html.Events exposing (onClick, onInput)
import List.Extra exposing (find)
import Maybe.Extra exposing (isNothing)
import RemoteData exposing (RemoteData(..), fromResult)
import String
import Time exposing (Zone)


type Msg
    = DataSetCreated (Result RequestError ())
    | DataSetDeleted (Result RequestError ())
    | DataSetsReceived (Result RequestError DataSetResult)
    | DataSetDeleteSubmit Int
    | DataSetExperimentTypeChange String
    | DataSetAttributiChange AttributoNameWithValueUpdate
    | DataSetSubmit
    | AddDataSet
    | CancelAddDataSet


type alias NewDataSet =
    { experimentType : Maybe ExperimentType
    , attributi : EditableAttributiAndOriginal
    }


type alias DataSetModel =
    { createRequest : RemoteData RequestError ()
    , deleteRequest : RemoteData RequestError ()
    , newDataSet : Maybe NewDataSet
    , dataSets : RemoteData RequestError DataSetResult
    , submitErrors : List String
    , zone : Zone
    }


initDataSet : HereAndNow -> ( DataSetModel, Cmd Msg )
initDataSet hereAndNow =
    ( { createRequest = NotAsked
      , deleteRequest = NotAsked
      , dataSets = Loading
      , submitErrors = []
      , newDataSet = Nothing
      , zone = hereAndNow.zone
      }
    , httpGetDataSets DataSetsReceived
    )


updateDataSet : Msg -> DataSetModel -> ( DataSetModel, Cmd Msg )
updateDataSet msg model =
    case msg of
        DataSetCreated result ->
            ( { model | createRequest = fromResult result }, httpGetDataSets DataSetsReceived )

        DataSetDeleted result ->
            ( { model | deleteRequest = fromResult result }, httpGetDataSets DataSetsReceived )

        DataSetsReceived result ->
            ( { model | dataSets = fromResult result }, Cmd.none )

        DataSetDeleteSubmit dataSetId ->
            ( { model | deleteRequest = Loading }, httpDeleteDataSet DataSetDeleted dataSetId )

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
            case (Debug.log "model" model).newDataSet of
                Nothing ->
                    ( model, Cmd.none )

                Just newDataSet ->
                    case newDataSet.experimentType of
                        Nothing ->
                            ( model, Cmd.none )

                        Just experimentType ->
                            case convertEditValues model.zone newDataSet.attributi of
                                Err errorList ->
                                    ( { model | submitErrors = List.map (\( name, errorMessage ) -> name ++ ": " ++ errorMessage) errorList }, Cmd.none )

                                Ok editedAttributi ->
                                    ( { model | createRequest = Loading }, httpCreateDataSet DataSetCreated experimentType.id editedAttributi )

        DataSetExperimentTypeChange newExperimentTypeIdStr ->
            case model.dataSets of
                Success { attributi, experimentTypes } ->
                    let
                        matchingExperimentType : Maybe ExperimentType
                        matchingExperimentType =
                            String.toInt newExperimentTypeIdStr
                                |> Maybe.andThen (\newExperimentTypeId -> find (\et -> et.id == newExperimentTypeId) experimentTypes)

                        attributoInExperimentType : Attributo AttributoType -> Bool
                        attributoInExperimentType attributo =
                            case matchingExperimentType of
                                Nothing ->
                                    False

                                Just et ->
                                    List.member attributo.name et.attributiNames

                        newDataSet : Maybe NewDataSet
                        newDataSet =
                            Just
                                { experimentType = matchingExperimentType
                                , attributi = createEditableAttributi model.zone (List.filter attributoInExperimentType attributi) emptyAttributoMap
                                }
                    in
                    ( { model | newDataSet = newDataSet }, Cmd.none )

                _ ->
                    ( model, Cmd.none )


viewEditForm : List (Chemical Int a b) -> EditableAttributiAndOriginal -> List (Html Msg)
viewEditForm chemicals =
    let
        attributoFormMsgToMsg : AttributoFormMsg -> Msg
        attributoFormMsgToMsg x =
            case x of
                AttributoFormValueUpdate vu ->
                    DataSetAttributiChange vu

                AttributoFormSubmit ->
                    DataSetSubmit
    in
    List.map (\attributo -> Html.map attributoFormMsgToMsg (viewAttributoForm chemicals attributo)) << .editableAttributi


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
            List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve data sets" ], showRequestError e ]

        Success { chemicals, attributi, dataSets, experimentTypes } ->
            let
                experimentTypesById : Dict.Dict Int String
                experimentTypesById =
                    List.foldl (\et -> Dict.insert et.id et.name) Dict.empty experimentTypes

                viewRow : DataSet -> Html Msg
                viewRow ds =
                    tr_
                        [ td_ [ text (String.fromInt ds.id) ]
                        , td_ [ text (Maybe.withDefault "" <| Dict.get ds.experimentTypeId experimentTypesById) ]
                        , td_ [ viewDataSetTable attributi model.zone (chemicalIdDict chemicals) ds False Nothing ]
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
                            form_
                                ([ h5_ [ text "New Data Set" ]
                                 , div [ class "mb-3" ]
                                    [ select [ class "form-select", onInput DataSetExperimentTypeChange ]
                                        (option [ selected (isNothing newDataSet.experimentType) ] [ text "«no value»" ] :: List.map (viewExperimentTypeOption newDataSet.experimentType) experimentTypes)
                                    ]
                                 ]
                                    ++ viewEditForm chemicals newDataSet.attributi
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
            , viewRemoteData "Deletion successful!" model.deleteRequest
            , viewRemoteData "Creation successful!" model.createRequest
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
