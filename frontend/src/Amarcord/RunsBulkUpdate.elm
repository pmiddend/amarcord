module Amarcord.RunsBulkUpdate exposing (Model, Msg, init, update, view)

import Amarcord.API.Requests exposing (RequestError, RunsBulkGetResponse, httpGetRunsBulk, httpUpdateRunsBulk)
import Amarcord.Attributo exposing (AttributoMap, AttributoValue)
import Amarcord.AttributoHtml exposing (AttributoFormMsg(..), AttributoNameWithValueUpdate, EditableAttributiAndOriginal, convertEditValues, createEditableAttributi, editEditableAttributi, viewAttributoForm)
import Amarcord.Bootstrap exposing (icon, viewRemoteData)
import Amarcord.Chemical exposing (Chemical, ChemicalId, ChemicalType(..))
import Amarcord.File exposing (File)
import Amarcord.Html exposing (form_, input_, li_, p_, strongText)
import Amarcord.Util exposing (HereAndNow)
import Dict
import Html exposing (Html, button, div, label, p, text, ul)
import Html.Attributes exposing (class, disabled, for, id, placeholder, type_, value)
import Html.Events exposing (onClick, onInput)
import Maybe.Extra as MaybeExtra
import Parser exposing ((|.), (|=))
import RemoteData exposing (RemoteData(..), fromResult, isLoading, isSuccess)


type alias EditableAttributiData =
    { actualEditableAttributi : EditableAttributiAndOriginal
    , chemicals : List (Chemical ChemicalId (AttributoMap AttributoValue) File)
    }


type alias Model =
    { hereAndNow : HereAndNow
    , runsInputField : String
    , runsBulkGetRequest : RemoteData RequestError EditableAttributiData
    , runsBulkUpdateRequest : RemoteData RequestError ()
    , submitErrors : List String
    }


type Msg
    = RunsInputFieldChanged String
    | SubmitRunRange
    | SubmitBulkChange
    | RunsBulkGetResponseReceived (Result RequestError RunsBulkGetResponse)
    | RunsBulkUpdateResponseReceived (Result RequestError ())
    | AttributoChange AttributoNameWithValueUpdate


viewBulkAttributiForm : RemoteData RequestError () -> List String -> EditableAttributiData -> Html Msg
viewBulkAttributiForm editRequest submitErrorsList { chemicals, actualEditableAttributi } =
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
                [ p [ class "text-success" ] [ text "Saved!" ] ]

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
    in
    form_ (List.map (Html.map attributoFormMsgToMsg << viewAttributoForm chemicals Crystal) actualEditableAttributi.editableAttributi ++ submitErrors ++ submitSuccess ++ okButton)


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
                , class "btn btn-primary"
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


init : HereAndNow -> Model
init hereAndNow =
    { hereAndNow = hereAndNow
    , runsInputField = ""
    , runsBulkGetRequest = NotAsked
    , runsBulkUpdateRequest = NotAsked
    , submitErrors = []
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


parseRunIds : String -> Maybe (List Int)
parseRunIds x =
    case parseRunIdsRaw x of
        Err _ ->
            Nothing

        Ok intRanges ->
            Just <| List.concatMap (\ir -> List.range ir.from ir.to) intRanges


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
        RunsInputFieldChanged string ->
            ( { model | runsInputField = string }, Cmd.none )

        SubmitBulkChange ->
            case ( parseRunIds model.runsInputField, model.runsBulkGetRequest ) of
                ( Just runIds, Success { actualEditableAttributi } ) ->
                    case convertEditValues model.hereAndNow.zone actualEditableAttributi of
                        Err errorList ->
                            ( { model | submitErrors = List.map (\( name, errorMessage ) -> name ++ ": " ++ errorMessage) errorList }, Cmd.none )

                        Ok editedAttributi ->
                            ( { model | runsBulkUpdateRequest = Loading }
                            , httpUpdateRunsBulk RunsBulkUpdateResponseReceived { runIds = runIds, attributi = editedAttributi }
                            )

                _ ->
                    ( model, Cmd.none )

        SubmitRunRange ->
            case parseRunIds model.runsInputField of
                Nothing ->
                    ( model, Cmd.none )

                Just runIds ->
                    ( { model | runsBulkGetRequest = Loading }
                    , httpGetRunsBulk RunsBulkGetResponseReceived { runIds = runIds }
                    )

        RunsBulkGetResponseReceived response ->
            case response of
                Ok bulkResponse ->
                    let
                        editableAttributi =
                            createEditableAttributi model.hereAndNow.zone bulkResponse.attributi (buildAttributoMap bulkResponse.attributiMap)
                    in
                    ( { model | runsBulkGetRequest = Success { actualEditableAttributi = editableAttributi, chemicals = bulkResponse.chemicals } }, Cmd.none )

                Err error ->
                    ( { model | runsBulkGetRequest = Failure error }, Cmd.none )

        AttributoChange v ->
            case model.runsBulkGetRequest of
                Success { actualEditableAttributi, chemicals } ->
                    let
                        newEditable =
                            editEditableAttributi actualEditableAttributi.editableAttributi v

                        newRunsBulkGetRequest =
                            { actualEditableAttributi = { editableAttributi = newEditable, originalAttributi = actualEditableAttributi.originalAttributi }
                            , chemicals = chemicals
                            }
                    in
                    ( { model | runsBulkGetRequest = Success newRunsBulkGetRequest }, Cmd.none )

                _ ->
                    ( model, Cmd.none )

        RunsBulkUpdateResponseReceived result ->
            ( { model | runsBulkUpdateRequest = fromResult result }, Cmd.none )
