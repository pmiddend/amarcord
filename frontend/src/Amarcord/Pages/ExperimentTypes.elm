module Amarcord.Pages.ExperimentTypes exposing (..)

import Amarcord.API.Requests exposing (ExperimentType, ExperimentTypesResponse, RequestError, httpCreateExperimentType, httpDeleteExperimentType, httpGetExperimentTypes)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, makeAlert, viewRemoteData)
import Amarcord.Html exposing (form_, h1_, h5_, input_, tbody_, td_, th_, thead_, tr_)
import Html exposing (Html, button, div, h4, input, label, table, text)
import Html.Attributes exposing (checked, class, disabled, for, id, type_, value)
import Html.Events exposing (onClick, onInput)
import RemoteData exposing (RemoteData(..), fromResult)
import String exposing (join, split, trim)


type ExperimentTypeMsg
    = ExperimentTypeCreated (Result RequestError ())
    | ExperimentTypeDeleted (Result RequestError ())
    | ExperimentTypesReceived (Result RequestError ExperimentTypesResponse)
    | ExperimentTypeNameChange String
    | ExperimentTypeDeleteSubmit String
    | ExperimentTypeSubmit
    | AddOrRemoveAttributo String Bool


type alias ExperimentTypeModel =
    { createRequest : RemoteData RequestError ()
    , deleteRequest : RemoteData RequestError ()
    , experimentTypes : RemoteData RequestError ExperimentTypesResponse
    , newExperimentTypeName : String
    , newExperimentTypeAttributi : List String
    }


initExperimentType : ( ExperimentTypeModel, Cmd ExperimentTypeMsg )
initExperimentType =
    ( { createRequest = NotAsked
      , deleteRequest = NotAsked
      , experimentTypes = Loading
      , newExperimentTypeName = ""
      , newExperimentTypeAttributi = []
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

        ExperimentTypeSubmit ->
            ( { model | createRequest = Loading }, httpCreateExperimentType ExperimentTypeCreated model.newExperimentTypeName model.newExperimentTypeAttributi )

        AddOrRemoveAttributo attributoName add ->
            ( { model
                | newExperimentTypeAttributi =
                    if add then
                        attributoName :: model.newExperimentTypeAttributi

                    else
                        List.filter (\x -> x /= attributoName) model.newExperimentTypeAttributi
              }
            , Cmd.none
            )


view : ExperimentTypeModel -> Html ExperimentTypeMsg
view model =
    div [ class "container" ] <| viewExperimentType model


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

        viewAttributoCheckbox { name } =
            div [ class "form-check" ]
                [ input
                    [ class "form-check-input"
                    , type_ "checkbox"
                    , id ("et-" ++ name)
                    , checked (List.member name model.newExperimentTypeAttributi)
                    , onInput (always <| AddOrRemoveAttributo name (not (List.member name model.newExperimentTypeAttributi)))
                    ]
                    []
                , label [ class "form-check-label", for ("et-" ++ name) ] [ text name ]
                ]
    in
    [ h1_ [ text "Experiment Types" ]
    , form_
        [ h5_ [ text "New experiment type" ]
        , div [ class "mb-3" ]
            [ label [ for "et-name", class "form-label" ] [ text "Name" ]
            , input_ [ class "form-control", type_ "text", value model.newExperimentTypeName, onInput ExperimentTypeNameChange ]
            ]
        , case model.experimentTypes of
            Success { attributi } ->
                div [ class "mb-3" ] (List.map viewAttributoCheckbox attributi)

            _ ->
                text ""
        , button [ type_ "button", class "btn btn-primary mb-3", onClick ExperimentTypeSubmit, disabled (List.isEmpty model.newExperimentTypeAttributi) ]
            [ icon { name = "plus-lg" }, text " Add type" ]
        ]
    , viewRemoteData "Deletion successful!" model.deleteRequest
    , viewRemoteData "Creation successful!" model.createRequest
    , case model.experimentTypes of
        Failure e ->
            makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve experiment types" ] ] ++ [ showRequestError e ]

        Success experimentTypesResponse ->
            table [ class "table table-striped" ] [ thead_ [ tr_ [ th_ [ text "Name" ], th_ [ text "Attributi" ], th_ [ text "Actions" ] ] ], tbody_ (List.map viewRow experimentTypesResponse.experimentTypes) ]

        _ ->
            text ""
    ]
