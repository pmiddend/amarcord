module Amarcord.Pages.DataSets exposing (..)

import Amarcord.API.Requests exposing (DataSetResult, ExperimentType, RequestError, httpCreateDataSet, httpDeleteDataSet, httpGetDataSets)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue, emptyAttributoMap)
import Amarcord.AttributoHtml exposing (AttributoNameWithValueUpdate, EditableAttributiAndOriginal, convertEditValues, createEditableAttributi, editEditableAttributi, emptyEditableAttributiAndOriginal, viewAttributoForm)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert, viewRemoteData)
import Amarcord.DataSet exposing (DataSet)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Html exposing (form_, h1_, h5_, tbody_, td_, th_, thead_, tr_)
import Amarcord.Sample exposing (Sample, sampleIdDict)
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


type alias DataSetModel =
    { createRequest : RemoteData RequestError ()
    , deleteRequest : RemoteData RequestError ()
    , dataSets : RemoteData RequestError DataSetResult
    , newDataSetExperimentType : Maybe String
    , newDataSetAttributi : EditableAttributiAndOriginal
    , submitErrors : List String
    , zone : Zone
    }


initDataSet : HereAndNow -> ( DataSetModel, Cmd DataSetMsg )
initDataSet hereAndNow =
    ( { createRequest = NotAsked
      , deleteRequest = NotAsked
      , dataSets = Loading
      , newDataSetExperimentType = Nothing
      , submitErrors = []
      , newDataSetAttributi = emptyEditableAttributiAndOriginal
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

        DataSetAttributiChange update ->
            ( { model
                | newDataSetAttributi =
                    { originalAttributi = model.newDataSetAttributi.originalAttributi
                    , editableAttributi = editEditableAttributi model.newDataSetAttributi.editableAttributi update
                    }
              }
            , Cmd.none
            )

        DataSetSubmit ->
            case model.newDataSetExperimentType of
                Nothing ->
                    ( model, Cmd.none )

                Just experimentType ->
                    case convertEditValues model.zone model.newDataSetAttributi of
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
                    in
                    ( { model
                        | newDataSetExperimentType = Just newExperimentType
                        , newDataSetAttributi = createEditableAttributi model.zone (List.filter attributoInExperimentType attributi) emptyAttributoMap
                      }
                    , Cmd.none
                    )

                _ ->
                    ( model, Cmd.none )


viewEditForm : List (Sample Int a b) -> EditableAttributiAndOriginal -> List (Html DataSetMsg)
viewEditForm samples =
    List.map (\attributo -> Html.map DataSetAttributiChange (viewAttributoForm samples attributo)) << .editableAttributi


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

        Success { samples, attributi, dataSets, experimentTypes } ->
            let
                viewRow : DataSet -> Html DataSetMsg
                viewRow ds =
                    tr_
                        [ td_ [ text (String.fromInt ds.id) ]
                        , td_ [ text ds.experimentType ]
                        , td_ [ viewDataSetTable attributi model.zone (sampleIdDict samples) ds False Nothing ]
                        , td_ [ button [ class "btn btn-sm btn-danger", onClick (DataSetDeleteSubmit ds.id) ] [ icon { name = "trash" } ] ]
                        ]

                viewExperimentTypeOption et =
                    option [ selected (model.newDataSetExperimentType == Just et.name) ] [ text et.name ]
            in
            [ h1_ [ text "Data Sets" ]
            , form_
                ([ h5_ [ text "New data set" ]
                 , div [ class "mb-3" ]
                    [ select [ class "form-select", onInput DataSetExperimentTypeChange ]
                        (option [ selected (isNothing model.newDataSetExperimentType) ] [ text "«no value»" ] :: List.map viewExperimentTypeOption experimentTypes)
                    ]
                 ]
                    ++ viewEditForm samples model.newDataSetAttributi
                    ++ [ button
                            [ type_ "button"
                            , class "btn btn-primary mb-3"
                            , onClick DataSetSubmit
                            , disabled (model.newDataSetExperimentType == Nothing)
                            ]
                            [ icon { name = "plus" }, text " Add Data Set" ]
                       ]
                )
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
