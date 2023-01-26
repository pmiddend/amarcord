module Amarcord.Pages.ExperimentTypes exposing (..)

import Amarcord.API.AttributoWithRole exposing (AttributoWithRole)
import Amarcord.API.ExperimentType exposing (ExperimentType, ExperimentTypeId)
import Amarcord.API.Requests exposing (ExperimentTypesResponse, RequestError, httpCreateExperimentType, httpDeleteExperimentType, httpGetExperimentTypes)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Attributo exposing (Attributo, AttributoType, attributoIsChemicalId)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, makeAlert, viewRemoteData)
import Amarcord.Chemical exposing (ChemicalType(..), chemicalTypeToString)
import Amarcord.Html exposing (div_, form_, h1_, h5_, input_, li_, tbody_, td_, th_, thead_, tr_, ul_)
import Amarcord.Util exposing (listContainsBy)
import Html exposing (Html, button, div, h4, input, label, table, text)
import Html.Attributes exposing (checked, class, disabled, for, id, type_, value)
import Html.Events exposing (onClick, onInput)
import RemoteData exposing (RemoteData(..), fromResult)
import Set exposing (Set)
import String


type ExperimentTypeMsg
    = ExperimentTypeCreated (Result RequestError ())
    | ExperimentTypeDeleted (Result RequestError ())
    | ExperimentTypesReceived (Result RequestError ExperimentTypesResponse)
    | ExperimentTypeNameChange String
    | ExperimentTypeDeleteSubmit ExperimentTypeId
    | ExperimentTypeAttributoRoleChange String ChemicalType
    | ExperimentTypeSubmit
    | AddOrRemoveAttributo String Bool
    | AddExperimentType
    | CancelAddExperimentType


type alias NewExperimentType =
    { name : String
    , attributi : List String
    , solutionAttributi : Set String
    , createRequest : RemoteData RequestError ()
    }


type alias ExperimentTypeModel =
    { deleteRequest : RemoteData RequestError ()
    , experimentTypes : RemoteData RequestError ExperimentTypesResponse
    , newExperimentType : Maybe NewExperimentType
    }


initExperimentType : ( ExperimentTypeModel, Cmd ExperimentTypeMsg )
initExperimentType =
    ( { deleteRequest = NotAsked
      , experimentTypes = Loading
      , newExperimentType = Nothing
      }
    , httpGetExperimentTypes ExperimentTypesReceived
    )


updateNewExperimentType : ExperimentTypeModel -> (NewExperimentType -> ( NewExperimentType, Cmd ExperimentTypeMsg )) -> ( ExperimentTypeModel, Cmd ExperimentTypeMsg )
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


updateExperimentType : ExperimentTypeMsg -> ExperimentTypeModel -> ( ExperimentTypeModel, Cmd ExperimentTypeMsg )
updateExperimentType msg model =
    case msg of
        ExperimentTypeCreated result ->
            updateNewExperimentType model (\newExperimentType -> ( { newExperimentType | createRequest = fromResult result }, httpGetExperimentTypes ExperimentTypesReceived ))

        ExperimentTypeDeleted result ->
            ( { model | deleteRequest = fromResult result }, httpGetExperimentTypes ExperimentTypesReceived )

        ExperimentTypesReceived result ->
            ( { model | experimentTypes = fromResult result }, Cmd.none )

        ExperimentTypeNameChange newName ->
            updateNewExperimentType model (\newExperimentType -> ( { newExperimentType | name = newName }, Cmd.none ))

        ExperimentTypeAttributoRoleChange attributoId newRole ->
            let
                newSolutionAttributi newExType =
                    if newRole == Solution then
                        Set.insert attributoId newExType.solutionAttributi

                    else
                        Set.remove attributoId newExType.solutionAttributi
            in
            updateNewExperimentType
                model
                (\newExperimentType -> ( { newExperimentType | solutionAttributi = newSolutionAttributi newExperimentType }, Cmd.none ))

        ExperimentTypeDeleteSubmit experimentTypeId ->
            ( { model | deleteRequest = Loading }, httpDeleteExperimentType ExperimentTypeDeleted experimentTypeId )

        ExperimentTypeSubmit ->
            let
                createAttributoWithRole : NewExperimentType -> String -> AttributoWithRole
                createAttributoWithRole newExp attributoName =
                    { name = attributoName
                    , role =
                        if Set.member attributoName newExp.solutionAttributi then
                            Solution

                        else
                            Crystal
                    }
            in
            updateNewExperimentType
                model
                (\newExperimentType ->
                    ( { newExperimentType | createRequest = Loading, name = "" }
                    , httpCreateExperimentType ExperimentTypeCreated newExperimentType.name (List.map (createAttributoWithRole newExperimentType) newExperimentType.attributi)
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


view : ExperimentTypeModel -> Html ExperimentTypeMsg
view model =
    div [ class "container" ] <| viewExperimentType model


viewExperimentType : ExperimentTypeModel -> List (Html ExperimentTypeMsg)
viewExperimentType model =
    let
        viewAttributoWithRole : List (Attributo AttributoType) -> AttributoWithRole -> Html msg
        viewAttributoWithRole attributi { name, role } =
            if listContainsBy (\a -> a.name == name && attributoIsChemicalId a.type_) attributi then
                li_ [ text (name ++ " (" ++ chemicalTypeToString role ++ ")") ]

            else
                li_ [ text name ]

        viewRow : List (Attributo AttributoType) -> ExperimentType -> Html ExperimentTypeMsg
        viewRow attributi et =
            tr_
                [ td_ [ text (String.fromInt et.id) ]
                , td_ [ text et.name ]
                , td_ [ ul_ (List.map (viewAttributoWithRole attributi) et.attributi) ]
                , td_ [ button [ class "btn btn-danger btn-sm", onClick (ExperimentTypeDeleteSubmit et.id) ] [ icon { name = "trash" } ] ]
                ]

        viewAttributoCheckbox attributi { name } =
            div [ class "form-check" ]
                [ input
                    [ class "form-check-input"
                    , type_ "checkbox"
                    , id ("et-" ++ name)
                    , checked (List.member name attributi)
                    , onInput (always <| AddOrRemoveAttributo name (not (List.member name attributi)))
                    ]
                    []
                , label [ class "form-check-label", for ("et-" ++ name) ] [ text name ]
                ]

        newExperimentTypeForm =
            case model.newExperimentType of
                Nothing ->
                    button [ onClick AddExperimentType, class "btn btn-primary mb-3", type_ "button" ] [ icon { name = "plus-lg" }, text " Add Experiment Type" ]

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
                                        List.map (viewAttributoCheckbox newExperimentType.attributi) attributi

                                    viewAttributoRoleRadio : Attributo AttributoType -> Html ExperimentTypeMsg
                                    viewAttributoRoleRadio a =
                                        if attributoIsChemicalId a.type_ && List.member a.name newExperimentType.attributi then
                                            tr_
                                                [ td_ [ text a.name ]
                                                , td_
                                                    [ div [ class "form-check form-check-inline" ]
                                                        [ input_
                                                            [ id (a.name ++ "-chemical-role-solution")
                                                            , class "form-check-input"
                                                            , type_ "radio"
                                                            , checked (Set.member a.name newExperimentType.solutionAttributi)
                                                            , onInput (\_ -> ExperimentTypeAttributoRoleChange a.name Solution)
                                                            ]
                                                        , label [ for (a.name ++ "-chemical-role-solution") ] [ text "Solution" ]
                                                        ]
                                                    , div [ class "form-check form-check-inline" ]
                                                        [ input_
                                                            [ id (a.name ++ "-chemical-role-crystal")
                                                            , class "form-check-input"
                                                            , type_ "radio"
                                                            , checked (not <| Set.member a.name newExperimentType.solutionAttributi)
                                                            , onInput (\_ -> ExperimentTypeAttributoRoleChange a.name Crystal)
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
                                                    viewAttributoRoleRadio
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
                        , viewRemoteData "Creation successful!" newExperimentType.createRequest
                        ]
    in
    [ h1_ [ text "Experiment Types" ]
    , newExperimentTypeForm
    , viewRemoteData "Deletion successful!" model.deleteRequest
    , case model.experimentTypes of
        Failure e ->
            makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve experiment types" ], showRequestError e ]

        Success experimentTypesResponse ->
            table [ class "table table-striped" ]
                [ thead_
                    [ tr_
                        [ th_ [ text "ID" ]
                        , th_ [ text "Name" ]
                        , th_ [ text "Attributi" ]
                        , th_ [ text "Actions" ]
                        ]
                    ]
                , tbody_ (List.map (viewRow experimentTypesResponse.attributi) experimentTypesResponse.experimentTypes)
                ]

        _ ->
            text ""
    ]
