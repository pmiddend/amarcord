module Amarcord.RunsBulkUpdate exposing (Model, Msg, init, update, view)

import Amarcord.API.Requests exposing (BeamtimeId, RunExternalId, runExternalIdFromInt, runExternalIdToInt)
import Amarcord.Attributo exposing (AttributoMap, AttributoValue, attributoMapToListOfAttributi, convertAttributoFromApi, convertAttributoValueFromApi)
import Amarcord.AttributoHtml exposing (AttributoFormMsg(..), AttributoNameWithValueUpdate, EditableAttributiAndOriginal, convertEditValues, createEditableAttributi, editEditableAttributi, viewAttributoForm)
import Amarcord.Bootstrap exposing (icon, viewRemoteData)
import Amarcord.Chemical exposing (Chemical, ChemicalId, convertChemicalFromApi)
import Amarcord.Html exposing (form_, h3_, hr_, input_, li_, onIntInput, p_, strongText)
import Amarcord.HttpError exposing (HttpError, send)
import Amarcord.Util exposing (HereAndNow)
import Api.Data exposing (ChemicalType(..), JsonExperimentType, JsonFileOutput, JsonReadRunsBulkOutput, JsonUpdateRunsBulkOutput)
import Api.Request.Runs exposing (readRunsBulkApiRunsBulkPost, updateRunsBulkApiRunsBulkPatch)
import Dict
import Html exposing (Html, button, div, label, option, select, text, ul)
import Html.Attributes exposing (class, disabled, for, id, placeholder, selected, type_, value)
import Html.Events exposing (onClick, onInput)
import Maybe.Extra as MaybeExtra
import Parser exposing ((|.), (|=))
import RemoteData exposing (RemoteData(..), fromResult, isLoading, isSuccess)


type alias EditableAttributiData =
    { actualEditableAttributi : EditableAttributiAndOriginal
    , chemicals : List (Chemical ChemicalId (AttributoMap AttributoValue) JsonFileOutput)
    , experimentTypeIds : List Int
    , experimentTypes : List JsonExperimentType
    , selectedExperimentType : Maybe Int
    }


type alias Model =
    { hereAndNow : HereAndNow
    , runsInputField : String
    , runsBulkGetRequest : RemoteData HttpError EditableAttributiData
    , runsBulkUpdateRequest : RemoteData HttpError JsonUpdateRunsBulkOutput
    , submitErrors : List String
    , beamtimeId : BeamtimeId
    }


type Msg
    = RunsInputFieldChanged String
    | SubmitRunRange
    | SubmitBulkChange
    | RunsBulkGetResponseReceived (Result HttpError JsonReadRunsBulkOutput)
    | RunsBulkUpdateResponseReceived (Result HttpError JsonUpdateRunsBulkOutput)
    | RunsBulkChangeExperimentType Int
    | AttributoChange AttributoNameWithValueUpdate


singletonElement : List a -> Maybe a
singletonElement xs =
    case xs of
        x :: [] ->
            Just x

        _ ->
            Nothing


viewBulkAttributiForm : RemoteData HttpError JsonUpdateRunsBulkOutput -> List String -> EditableAttributiData -> Html Msg
viewBulkAttributiForm editRequest submitErrorsList { chemicals, actualEditableAttributi, experimentTypeIds, experimentTypes, selectedExperimentType } =
    let
        submitErrors =
            case submitErrorsList of
                [] ->
                    [ text "" ]

                errors ->
                    [ p_ [ strongText "There were submission errors:" ]
                    , ul [ class "text-danger" ] <| List.map (\e -> li_ [ text e ]) errors
                    ]

        submitSuccess =
            if isSuccess editRequest then
                [ div [ class "badge text-bg-success" ] [ text "Saved!" ] ]

            else
                []

        okButton =
            [ button
                [ type_ "button"
                , class "btn btn-primary"
                , onClick SubmitBulkChange
                , disabled (isLoading editRequest)
                ]
                [ icon { name = "save" }, text " Update all runs" ]
            ]

        attributoFormMsgToMsg : AttributoFormMsg -> Msg
        attributoFormMsgToMsg x =
            case x of
                AttributoFormValueUpdate vu ->
                    AttributoChange vu

                AttributoFormSubmit ->
                    SubmitBulkChange

        experimentTypeOption : JsonExperimentType -> Html Msg
        experimentTypeOption { id, name } =
            option
                [ value (String.fromInt id)
                , selected (Just id == singletonElement experimentTypeIds)
                ]
                [ text name ]

        variousOptions =
            case experimentTypeIds of
                -- no experiment type IDs => can't be, every run must have one
                [] ->
                    []

                -- a single experiment ID - then a "various" option doesn't make sense
                _ :: [] ->
                    []

                _ ->
                    [ option [ disabled True, value "", selected (MaybeExtra.isNothing selectedExperimentType) ] [ text "«various»" ] ]

        experimentTypeSelect : Html Msg
        experimentTypeSelect =
            div [ class "form-floating" ]
                [ select
                    [ class "form-select", id "bulk-experiment-type", onIntInput RunsBulkChangeExperimentType ]
                    (variousOptions ++ List.map experimentTypeOption experimentTypes)
                , label [ for "bulk-experiment-type" ] [ text "Experiment Type" ]
                ]
    in
    form_
        (h3_ [ text "Attributo values" ]
            :: div [ class "lead" ] [ text "The following input fields might be left empty, in which case multiple runs have different values for this attributo. Write something in the input field value, and all runs will have the new attributo value set." ]
            :: hr_
            :: experimentTypeSelect
            :: List.map (Html.map attributoFormMsgToMsg << viewAttributoForm chemicals ChemicalTypeCrystal) actualEditableAttributi.editableAttributi
            ++ submitErrors
            ++ submitSuccess
            ++ okButton
        )


view : Model -> Html Msg
view model =
    div []
        [ form_
            [ div [ class "form-floating mb-3" ]
                [ input_
                    [ type_ "text"
                    , value model.runsInputField
                    , class "form-control"
                    , id "run-ids"
                    , placeholder "1-3, 45, 60-120"
                    , onInput RunsInputFieldChanged
                    ]
                , label [ for "run-ids" ] [ text "Run IDs" ]
                , div [ class "form-text" ] [ text "Can be single run IDs like \"1, 5, 8\" or ranges of runs like \"50-65\". Or a mix: \"1, 4, 50-65\"" ]
                ]
            , button
                [ type_ "button"
                , class "btn btn-primary mb-3"
                , onClick SubmitRunRange
                , disabled (model.runsInputField == "" || MaybeExtra.isNothing (parseRunIds model.runsInputField))
                ]
                [ icon { name = "arrow-clockwise" }, text " Retrieve run attributi" ]
            ]
        , case model.runsBulkGetRequest of
            Success editableAttributi ->
                viewBulkAttributiForm model.runsBulkUpdateRequest model.submitErrors editableAttributi

            _ ->
                viewRemoteData "Bulk request" model.runsBulkGetRequest
        ]


init : HereAndNow -> BeamtimeId -> Model
init hereAndNow beamtimeId =
    { hereAndNow = hereAndNow
    , runsInputField = ""
    , runsBulkGetRequest = NotAsked
    , runsBulkUpdateRequest = NotAsked
    , submitErrors = []
    , beamtimeId = beamtimeId
    }


type alias IntRange =
    { from : Int
    , to : Int
    }


parseIntOrRange : Parser.Parser IntRange
parseIntOrRange =
    Parser.andThen
        (\beginning ->
            Parser.map
                (IntRange beginning)
            <|
                Parser.oneOf [ Parser.succeed identity |. Parser.symbol "-" |= Parser.int, Parser.succeed beginning ]
        )
        Parser.int


parseRunIdsRaw : String -> Result (List Parser.DeadEnd) (List IntRange)
parseRunIdsRaw =
    Parser.run
        (Parser.sequence
            { start = ""
            , end = ""
            , separator = ","
            , spaces = Parser.spaces
            , item = parseIntOrRange
            , trailing = Parser.Optional
            }
        )


parseRunIds : String -> Maybe (List RunExternalId)
parseRunIds x =
    case parseRunIdsRaw x of
        Err _ ->
            Nothing

        Ok intRanges ->
            Just <| List.concatMap (\ir -> List.map runExternalIdFromInt <| List.range ir.from ir.to) intRanges


buildAttributoMap : AttributoMap (List AttributoValue) -> AttributoMap AttributoValue
buildAttributoMap =
    let
        transducer attributoId values prior =
            case values of
                [] ->
                    prior

                [ x ] ->
                    Dict.insert attributoId x prior

                _ ->
                    prior
    in
    Dict.foldl transducer Dict.empty


update : Model -> Msg -> ( Model, Cmd Msg )
update model msg =
    case msg of
        RunsBulkChangeExperimentType newExperimentTypeId ->
            case model.runsBulkGetRequest of
                Success successfulRequest ->
                    ( { model | runsBulkGetRequest = Success { successfulRequest | selectedExperimentType = Just newExperimentTypeId } }, Cmd.none )

                _ ->
                    ( model, Cmd.none )

        RunsInputFieldChanged string ->
            ( { model | runsInputField = string }, Cmd.none )

        SubmitBulkChange ->
            case ( parseRunIds model.runsInputField, model.runsBulkGetRequest ) of
                ( Just runIds, Success { selectedExperimentType, actualEditableAttributi } ) ->
                    case convertEditValues model.hereAndNow.zone actualEditableAttributi of
                        Err errorList ->
                            ( { model | submitErrors = List.map (\( attributoId, errorMessage ) -> String.fromInt attributoId ++ ": " ++ errorMessage) errorList }, Cmd.none )

                        Ok editedAttributi ->
                            ( { model | runsBulkUpdateRequest = Loading }
                            , send RunsBulkUpdateResponseReceived
                                (updateRunsBulkApiRunsBulkPatch
                                    { beamtimeId = model.beamtimeId
                                    , externalRunIds = List.map runExternalIdToInt runIds
                                    , attributi = attributoMapToListOfAttributi editedAttributi
                                    , newExperimentTypeId = selectedExperimentType
                                    }
                                )
                            )

                _ ->
                    ( model, Cmd.none )

        SubmitRunRange ->
            case parseRunIds model.runsInputField of
                Nothing ->
                    ( model, Cmd.none )

                Just runIds ->
                    ( { model | runsBulkGetRequest = Loading }
                    , send RunsBulkGetResponseReceived (readRunsBulkApiRunsBulkPost { beamtimeId = model.beamtimeId, externalRunIds = List.map runExternalIdToInt runIds })
                    )

        RunsBulkGetResponseReceived response ->
            case response of
                Ok bulkResponse ->
                    let
                        editableAttributi =
                            createEditableAttributi
                                model.hereAndNow.zone
                                (List.map convertAttributoFromApi bulkResponse.attributi)
                                (buildAttributoMap <| Dict.fromList <| List.map (\{ attributoId, values } -> ( attributoId, List.map convertAttributoValueFromApi values )) <| bulkResponse.attributiValues)
                    in
                    ( { model
                        | runsBulkGetRequest =
                            Success
                                { actualEditableAttributi = editableAttributi
                                , chemicals = List.map convertChemicalFromApi bulkResponse.chemicals
                                , experimentTypeIds = bulkResponse.experimentTypeIds
                                , experimentTypes = bulkResponse.experimentTypes

                                -- if we have exactly one experiment type, select that, otherwise make the selection "Nothing"
                                , selectedExperimentType = singletonElement bulkResponse.experimentTypeIds
                                }
                      }
                    , Cmd.none
                    )

                Err error ->
                    ( { model | runsBulkGetRequest = Failure error }, Cmd.none )

        AttributoChange v ->
            case model.runsBulkGetRequest of
                Success { actualEditableAttributi, chemicals, experimentTypes, experimentTypeIds, selectedExperimentType } ->
                    let
                        newEditable =
                            editEditableAttributi actualEditableAttributi.editableAttributi v

                        newRunsBulkGetRequest : EditableAttributiData
                        newRunsBulkGetRequest =
                            { actualEditableAttributi = { actualEditableAttributi | editableAttributi = newEditable }
                            , chemicals = chemicals
                            , experimentTypes = experimentTypes
                            , experimentTypeIds = experimentTypeIds
                            , selectedExperimentType = selectedExperimentType
                            }
                    in
                    ( { model | runsBulkGetRequest = Success newRunsBulkGetRequest }, Cmd.none )

                _ ->
                    ( model, Cmd.none )

        RunsBulkUpdateResponseReceived result ->
            ( { model | runsBulkUpdateRequest = fromResult result }, Cmd.none )
