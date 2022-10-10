module Amarcord.Pages.DataSets exposing (..)

import Amarcord.API.Requests exposing (DataSetResult, ExperimentType, RequestError, httpCreateDataSet, httpDeleteDataSet, httpGetDataSets)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue, emptyAttributoMap)
import Amarcord.AttributoHtml exposing (AttributoFormMsg(..), AttributoNameWithValueUpdate, EditableAttributiAndOriginal, convertEditValues, createEditableAttributi, editEditableAttributi, emptyEditableAttributiAndOriginal, viewAttributoForm)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert, viewRemoteData)
import Amarcord.Chemical exposing (Chemical, chemicalIdDict)
import Amarcord.DataSet exposing (DataSet)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Html exposing (form_, h1_, h5_, tbody_, td_, th_, thead_, tr_)
import Amarcord.Util exposing (HereAndNow)
import Html exposing (Html, button, div, h4, option, select, table, text)
import Html.Attributes exposing (class, disabled, selected, type_)
import Html.Events exposing (onClick, onInput)
import List.Extra exposing (find)
import Maybe.Extra exposing (isNothing)
import RemoteData exposing (RemoteData(..), fromResult)
import String
import Time exposing (Zone)


type DataSetMsg
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
    { experimentType : Maybe String
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


initDataSet : HereAndNow -> ( DataSetModel, Cmd DataSetMsg )
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


updateDataSet : DataSetMsg -> DataSetModel -> ( DataSetModel, Cmd DataSetMsg )
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
                                    ( { model | submitErrors = List.map (\( name, errorMessage ) -> name ++ ": " ++ errorMessage) errorList }, Cmd.none )

                                Ok editedAttributi ->
                                    ( { model | createRequest = Loading }, httpCreateDataSet DataSetCreated experimentType editedAttributi )

        DataSetExperimentTypeChange newExperimentType ->
            case model.dataSets of
                Success { attributi, experimentTypes } ->
                    let
                        matchingExperimentType : Maybe ExperimentType
                        matchingExperimentType =
                            find (\et -> et.name == newExperimentType) experimentTypes

                        attributoInExperimentType : Attributo AttributoType -> Bool
                        attributoInExperimentType attributo =
                            case matchingExperimentType of
                                Nothing ->
                                    False

                                Just et ->
                                    List.member attributo.name et.attributeNames

                        newDataSet =
                            Just
                                { experimentType = Just newExperimentType
                                , attributi = createEditableAttributi model.zone (List.filter attributoInExperimentType attributi) emptyAttributoMap
                                }
                    in
                    ( { model | newDataSet = newDataSet }, Cmd.none )

                _ ->
                    ( model, Cmd.none )


viewEditForm : List (Chemical Int a b) -> EditableAttributiAndOriginal -> List (Html DataSetMsg)
viewEditForm chemicals =
    let
        attributoFormMsgToMsg : AttributoFormMsg -> DataSetMsg
        attributoFormMsgToMsg x =
            case x of
                AttributoFormValueUpdate vu ->
                    DataSetAttributiChange vu

                AttributoFormSubmit ->
                    DataSetSubmit
    in
    List.map (\attributo -> Html.map attributoFormMsgToMsg (viewAttributoForm chemicals attributo)) << .editableAttributi


view : DataSetModel -> Html DataSetMsg
view model =
    div [ class "container" ] <| viewDataSet model


viewDataSet : DataSetModel -> List (Html DataSetMsg)
viewDataSet model =
    case model.dataSets of
        NotAsked ->
            List.singleton <| text ""

        Loading ->
            List.singleton <| loadingBar "Loading data set..."

        Failure e ->
            List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve data sets" ] ] ++ [ showRequestError e ]

        Success { chemicals, attributi, dataSets, experimentTypes } ->
            let
                viewRow : DataSet -> Html DataSetMsg
                viewRow ds =
                    tr_
                        [ td_ [ text (String.fromInt ds.id) ]
                        , td_ [ text ds.experimentType ]
                        , td_ [ viewDataSetTable attributi model.zone (chemicalIdDict chemicals) ds False Nothing ]
                        , td_ [ button [ class "btn btn-sm btn-danger", onClick (DataSetDeleteSubmit ds.id) ] [ icon { name = "trash" } ] ]
                        ]

                viewExperimentTypeOption currentEt et =
                    option [ selected (currentEt == Just et.name) ] [ text et.name ]

                newDataSetForm =
                    case model.newDataSet of
                        Nothing ->
                            button [ type_ "button", onClick AddDataSet, class "btn btn-primary mb-3" ] [ icon { name = "plus-lg" }, text " Add Data Set" ]

                        Just newDataSet ->
                            form_
                                ([ h5_ [ text "New data set" ]
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
