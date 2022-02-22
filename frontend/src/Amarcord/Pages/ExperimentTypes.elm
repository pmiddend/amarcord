module Amarcord.Pages.ExperimentTypes exposing (..)

import Amarcord.API.Requests exposing (ExperimentType, httpCreateExperimentType, httpDeleteExperimentType, httpGetExperimentTypes)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, makeAlert, showHttpError, viewRemoteData)
import Amarcord.Html exposing (form_, h1_, h5_, input_, tbody_, td_, th_, thead_, tr_)
import Html exposing (Html, button, div, h4, label, table, text)
import Html.Attributes exposing (class, for, type_, value)
import Html.Events exposing (onClick, onInput)
import Http exposing (Error(..), Response(..))
import RemoteData exposing (RemoteData(..), fromResult)
import String exposing (join, split, trim)


type ExperimentTypeMsg
    = ExperimentTypeCreated (Result Http.Error ())
    | ExperimentTypeDeleted (Result Http.Error ())
    | ExperimentTypesReceived (Result Http.Error (List ExperimentType))
    | ExperimentTypeNameChange String
    | ExperimentTypeDeleteSubmit String
    | ExperimentTypeAttributiChange String
    | ExperimentTypeSubmit


type alias ExperimentTypeModel =
    { createRequest : RemoteData Http.Error ()
    , deleteRequest : RemoteData Http.Error ()
    , experimentTypes : RemoteData Http.Error (List ExperimentType)
    , newExperimentTypeName : String
    , newExperimentTypeAttributi : String
    }


initExperimentType : ( ExperimentTypeModel, Cmd ExperimentTypeMsg )
initExperimentType =
    ( { createRequest = NotAsked
      , deleteRequest = NotAsked
      , experimentTypes = Loading
      , newExperimentTypeName = ""
      , newExperimentTypeAttributi = ""
      }
    , httpGetExperimentTypes ExperimentTypesReceived
    )


updateExperimentType : ExperimentTypeMsg -> ExperimentTypeModel -> ( ExperimentTypeModel, Cmd ExperimentTypeMsg )
updateExperimentType msg model =
    case msg of
        ExperimentTypeCreated result ->
            ( { model | createRequest = fromResult result }, httpGetExperimentTypes ExperimentTypesReceived )

        ExperimentTypeDeleted result ->
            ( { model | deleteRequest = fromResult result }, httpGetExperimentTypes ExperimentTypesReceived )

        ExperimentTypesReceived result ->
            ( { model | experimentTypes = fromResult result }, Cmd.none )

        ExperimentTypeNameChange newName ->
            ( { model | newExperimentTypeName = newName }, Cmd.none )

        ExperimentTypeDeleteSubmit experimentTypeName ->
            ( { model | deleteRequest = Loading }, httpDeleteExperimentType ExperimentTypeDeleted experimentTypeName )

        ExperimentTypeAttributiChange newAttributi ->
            ( { model | newExperimentTypeAttributi = newAttributi }, Cmd.none )

        ExperimentTypeSubmit ->
            ( { model | createRequest = Loading }, httpCreateExperimentType ExperimentTypeCreated model.newExperimentTypeName (List.map trim <| split "," model.newExperimentTypeAttributi) )


viewExperimentType : ExperimentTypeModel -> List (Html ExperimentTypeMsg)
viewExperimentType model =
    let
        viewRow : ExperimentType -> Html ExperimentTypeMsg
        viewRow et =
            tr_
                [ td_ [ text et.name ]
                , td_ [ text (join "," et.attributeNames) ]
                , td_ [ button [ class "btn btn-danger btn-sm", onClick (ExperimentTypeDeleteSubmit et.name) ] [ icon { name = "trash" } ] ]
                ]
    in
    [ h1_ [ text "Experiment Types" ]
    , form_
        [ h5_ [ text "New experiment type" ]
        , div [ class "mb-3" ]
            [ label [ for "et-name", class "form-label" ] [ text "Name" ]
            , input_ [ class "form-control", type_ "text", value model.newExperimentTypeName, onInput ExperimentTypeNameChange ]
            ]
        , div [ class "mb-3" ]
            [ label [ for "et-attributi", class "form-label" ] [ text "Attributi" ]
            , input_ [ class "form-control", type_ "text", value model.newExperimentTypeAttributi, onInput ExperimentTypeAttributiChange ]
            , div [ class "form-text" ] [ text "Comma-separated list of attributi names" ]
            ]
        , button [ type_ "button", class "btn btn-primary mb-3", onClick ExperimentTypeSubmit ] [ text "Add type" ]
        ]
    , viewRemoteData "Deletion successful!" model.deleteRequest
    , viewRemoteData "Creation successful!" model.createRequest
    , case model.experimentTypes of
        Failure e ->
            makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve experiment types" ] ] ++ showHttpError e

        Success experimentTypes ->
            table [ class "table table-striped" ] [ thead_ [ tr_ [ th_ [ text "Name" ], th_ [ text "Attributi" ], th_ [ text "Actions" ] ] ], tbody_ (List.map viewRow experimentTypes) ]

        _ ->
            text ""
    ]
