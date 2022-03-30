module Amarcord.Pages.Samples exposing (Model, Msg, init, update, view)

import Amarcord.API.Requests exposing (RequestError, SamplesResponse, httpCreateFile, httpCreateSample, httpDeleteSample, httpGetSamples, httpUpdateSample)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Attributo
    exposing
        ( Attributo
        , AttributoMap
        , AttributoType(..)
        , AttributoValue(..)
        , emptyAttributoMap
        )
import Amarcord.AttributoHtml exposing (AttributoEditValue(..), AttributoNameWithValueUpdate, EditStatus(..), EditableAttributiAndOriginal, convertEditValues, createEditableAttributi, editEditableAttributi, makeAttributoHeader, mutedSubheader, viewAttributoCell, viewAttributoForm)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert, viewRemoteData)
import Amarcord.Dialog as Dialog
import Amarcord.File exposing (File)
import Amarcord.Html exposing (br_, form_, h4_, h5_, input_, li_, p_, span_, strongText, sup_, tbody_, td_, th_, thead_, tr_)
import Amarcord.Route exposing (makeFilesLink)
import Amarcord.Sample exposing (Sample, SampleId, sampleMapAttributi, sampleMapId)
import Amarcord.Util exposing (HereAndNow)
import Dict exposing (Dict)
import File as ElmFile
import File.Select
import Html exposing (..)
import Html.Attributes exposing (attribute, class, disabled, for, href, id, type_, value)
import Html.Events exposing (onClick, onInput)
import List exposing (length, singleton)
import Maybe
import Maybe.Extra as Maybe exposing (isJust, isNothing)
import RemoteData exposing (RemoteData(..), fromResult)
import String exposing (fromInt, split)
import Time exposing (Month(..), Posix, Zone)


type alias SamplesAndAttributi =
    { samples : List (Sample SampleId (AttributoMap AttributoValue) File)
    , attributi : List (Attributo AttributoType)
    }


type alias NewFileUpload =
    { file : Maybe ElmFile.File
    , description : String
    }


type alias Model =
    { samples : RemoteData RequestError SamplesAndAttributi
    , deleteModalOpen : Maybe ( String, SampleId )
    , sampleDeleteRequest : RemoteData RequestError ()
    , fileUploadRequest : RemoteData RequestError ()
    , editSample : Maybe (Sample (Maybe Int) EditableAttributiAndOriginal File)
    , modifyRequest : RemoteData RequestError ()
    , myTimeZone : Zone
    , submitErrors : List String
    , newFileUpload : NewFileUpload
    }


type Msg
    = SamplesReceived SamplesResponse
    | CancelDelete
    | AskDelete String SampleId
    | InitiateEdit (Sample Int (AttributoMap AttributoValue) File)
    | ConfirmDelete SampleId
    | AddSample
    | EditSampleName String
    | SampleDeleteFinished (Result RequestError ())
    | EditSampleSubmit
    | EditSampleCancel
    | EditSampleFinished (Result RequestError ())
    | EditSampleAttributo AttributoNameWithValueUpdate
    | EditNewFileDescription String
    | EditNewFileFile ElmFile.File
    | EditFileUpload
    | EditFileUploadFinished (Result RequestError File)
    | EditFileDelete Int
    | EditResetNewFileUpload
    | EditNewFileOpenSelector


init : HereAndNow -> ( Model, Cmd Msg )
init { zone } =
    ( { samples = Loading
      , deleteModalOpen = Nothing
      , sampleDeleteRequest = NotAsked
      , modifyRequest = NotAsked
      , fileUploadRequest = NotAsked
      , editSample = Nothing
      , myTimeZone = zone
      , submitErrors = []
      , newFileUpload = { file = Nothing, description = "" }
      }
    , httpGetSamples SamplesReceived
    )


viewFiles : RemoteData RequestError () -> NewFileUpload -> List File -> List (Html Msg)
viewFiles fileUploadError newFile files =
    let
        viewFileRow : File -> Html Msg
        viewFileRow file =
            tr [ class "align-middle" ]
                [ td_ [ text (String.fromInt file.id) ]
                , td_ [ text file.fileName ]
                , td_ [ text file.type_ ]
                , td_ [ text file.description ]
                , td_ [ button [ class "btn btn-danger btn-sm", type_ "button", onClick (EditFileDelete file.id) ] [ icon { name = "trash" } ] ]
                ]

        filesTable =
            [ h5_ [ text "Attached files" ]
            , table [ class "table table-striped table-sm" ]
                [ thead_
                    [ tr_
                        [ th_ [ text "ID" ]
                        , th_ [ text "File name" ]
                        , th_ [ text "Type" ]
                        , th_ [ text "Description" ]
                        , th_ [ text "Actions" ]
                        ]
                    ]
                , tbody_ (List.map viewFileRow files)
                ]
            ]

        uploadForm =
            [ div [ class "card mb-3" ]
                [ div [ class "card-header" ]
                    [ text "File upload" ]
                , div [ class "card-body" ]
                    [ viewRemoteData "Upload successful!" fileUploadError
                    , div [ class "input-group mb-3" ]
                        [ button [ type_ "button", class "btn btn-outline-secondary", onClick EditNewFileOpenSelector ] [ text "Choose file..." ]
                        , input [ type_ "text", disabled True, value (Maybe.unwrap "No file selected" ElmFile.name newFile.file), class "form-control" ] []
                        ]
                    , div [ class "mb-3" ]
                        [ label [ for "file-description", class "form-label" ] [ text "File Description", sup_ [ text "*" ] ]
                        , input_
                            [ type_ "text"
                            , class
                                ("form-control"
                                    ++ (if String.isEmpty newFile.description && isJust newFile.file then
                                            " is-invalid"

                                        else
                                            ""
                                       )
                                )
                            , id "name"
                            , value newFile.description
                            , onInput EditNewFileDescription
                            ]
                        ]
                    , button
                        [ class "btn btn-dark me-3"
                        , type_ "button"
                        , disabled (newFile.description == "" || isNothing newFile.file)
                        , onClick EditFileUpload
                        ]
                        [ icon { name = "upload" }, text " Upload" ]
                    , button
                        [ class "btn btn-light me-3"
                        , type_ "button"
                        , onClick EditResetNewFileUpload
                        ]
                        [ icon { name = "arrow-counterclockwise" }, text " Reset" ]
                    ]
                ]
            ]
    in
    if length files == 0 then
        uploadForm

    else
        filesTable ++ uploadForm


viewEditForm : RemoteData RequestError () -> List String -> NewFileUpload -> Sample (Maybe Int) EditableAttributiAndOriginal File -> Html Msg
viewEditForm fileUploadRequest submitErrorsList newFileUpload sample =
    let
        attributiFormEntries =
            List.map (\attributo -> Html.map EditSampleAttributo (viewAttributoForm [] attributo)) sample.attributi.editableAttributi

        submitErrors =
            case submitErrorsList of
                [] ->
                    [ text "" ]

                errors ->
                    [ p_ [ strongText "There were submission errors:" ]
                    , ul [ class "text-danger" ] <| List.map (\e -> li_ [ text e ]) errors
                    ]
    in
    form_ <|
        [ h4_
            [ text
                (if isNothing sample.id then
                    "Add new sample"

                 else
                    "Edit sample"
                )
            ]
        , div [ class "mb-3" ]
            [ label [ for "name", class "form-label" ] [ text "Name", sup_ [ text "*" ] ]
            , input_
                [ type_ "text"
                , class
                    ("form-control"
                        ++ (if sample.name == "" then
                                " is-invalid"

                            else
                                ""
                           )
                    )
                , id "name"
                , value sample.name
                , onInput EditSampleName
                ]
            , if sample.name /= "" then
                text ""

              else
                div [ class "invalid-feedback" ] [ text "Name is mandatory" ]
            ]
        ]
            ++ attributiFormEntries
            ++ viewFiles fileUploadRequest newFileUpload sample.files
            ++ submitErrors
            ++ [ button
                    [ class "btn btn-primary me-3 mb-3"
                    , onClick EditSampleSubmit
                    , type_ "button"
                    ]
                    [ icon { name = "plus-lg" }
                    , text
                        (if isNothing sample.id then
                            " Add new sample"

                         else
                            " Confirm edit"
                        )
                    ]
               , button
                    [ class "btn btn-secondary me-3 mb-3"
                    , onClick EditSampleCancel
                    , type_ "button"
                    ]
                    [ icon { name = "x-lg" }, text " Cancel" ]
               ]


typeToIcon : String -> Html Msg
typeToIcon type_ =
    icon
        { name =
            case split "/" type_ of
                "text" :: "x-shellscript" :: [] ->
                    "file-code"

                "application" :: "pdf" :: [] ->
                    "file-pdf"

                prefix :: _ ->
                    case prefix of
                        "image" ->
                            "file-image"

                        "text" ->
                            "file-text"

                        _ ->
                            "question-diamond"

                _ ->
                    "question-diamond"
        }


viewSampleRow : Zone -> List (Attributo AttributoType) -> Sample SampleId (AttributoMap AttributoValue) File -> Html Msg
viewSampleRow zone attributi sample =
    let
        viewFile { id, type_, fileName, description } =
            li [ class "list-group-item" ]
                [ typeToIcon type_
                , text " "
                , span [ attribute "data-tooltip" description ] [ a [ href (makeFilesLink id), class "stretched-link" ] [ text fileName ] ]
                ]

        files =
            case sample.files of
                [] ->
                    text ""

                _ ->
                    ul [ class "list-group list-group-flush" ] (List.map viewFile sample.files)
    in
    tr_ <|
        [ td_ [ text (fromInt sample.id) ]
        , td_ [ text sample.name ]
        ]
            ++ List.map (viewAttributoCell { shortDateTime = False } zone Dict.empty sample.attributi) attributi
            ++ [ td_ [ files ]
               ]
            ++ [ td_
                    [ button [ class "btn btn-lnk btn-sm me-1", onClick (AskDelete sample.name sample.id) ] [ icon { name = "trash" } ]
                    , button [ class "btn btn-link btn-sm", onClick (InitiateEdit sample) ] [ icon { name = "pencil-square" } ]
                    ]
               ]


viewSampleTable : Zone -> List (Sample SampleId (AttributoMap AttributoValue) File) -> List (Attributo AttributoType) -> Html Msg
viewSampleTable zone samples attributi =
    let
        attributiColumns : List (Html msg)
        attributiColumns =
            List.map (th_ << makeAttributoHeader) attributi
    in
    table [ class "table" ]
        [ thead_
            [ tr [ class "align-top" ] <|
                [ th_ [ text "ID" ]
                , th_ [ text "Name" ]
                ]
                    ++ attributiColumns
                    ++ [ th_ [ text "Files", br_, mutedSubheader "Hover to see description, click to download" ] ]
                    ++ [ th_ [ text "Actions" ]
                       ]
            ]
        , tbody_
            (List.map (viewSampleRow zone attributi) samples)
        ]


viewInner : Model -> List (Html Msg)
viewInner model =
    case model.samples of
        NotAsked ->
            singleton <| text ""

        Loading ->
            singleton <| loadingBar "Loading samples..."

        Failure e ->
            singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve samples" ] ] ++ [ showRequestError e ]

        Success { samples, attributi } ->
            let
                prefix =
                    case model.editSample of
                        Nothing ->
                            button [ class "btn btn-primary", onClick AddSample ] [ icon { name = "plus-lg" }, text " Add sample" ]

                        Just ea ->
                            viewEditForm model.fileUploadRequest model.submitErrors model.newFileUpload ea

                modifyRequestResult =
                    case model.modifyRequest of
                        NotAsked ->
                            text ""

                        Loading ->
                            p [] [ text "Request in progress..." ]

                        Failure e ->
                            div [] [ makeAlert [ AlertDanger ] [ showRequestError e ] ]

                        Success _ ->
                            div [ class "mt-3" ]
                                [ makeAlert [ AlertSuccess ] [ text "Request successful!" ]
                                ]

                deleteRequestResult =
                    case model.sampleDeleteRequest of
                        NotAsked ->
                            text ""

                        Loading ->
                            p [] [ text "Request in progress..." ]

                        Failure e ->
                            div [] [ makeAlert [ AlertDanger ] [ showRequestError e ] ]

                        Success _ ->
                            div [ class "mt-3" ]
                                [ makeAlert [ AlertSuccess ] [ text "Deletion successful!" ]
                                ]
            in
            prefix
                :: modifyRequestResult
                :: deleteRequestResult
                :: viewSampleTable model.myTimeZone samples attributi
                :: []


view : Model -> Html Msg
view model =
    let
        maybeDeleteModal =
            case model.deleteModalOpen of
                Nothing ->
                    []

                Just ( sampleName, sampleId ) ->
                    [ Dialog.view
                        (Just
                            { header = Nothing
                            , body = Just (span_ [ text "Really delete sample ", strongText sampleName, text "?" ])
                            , closeMessage = Just CancelDelete
                            , containerClass = Nothing
                            , footer = Just (button [ class "btn btn-danger", onClick (ConfirmDelete sampleId) ] [ text "Really delete!" ])
                            }
                        )
                    ]
    in
    div [ class "container" ] (maybeDeleteModal ++ viewInner model)


editSampleFromAttributiAndValues : Zone -> List (Attributo AttributoType) -> Sample (Maybe Int) (AttributoMap AttributoValue) a -> Sample (Maybe Int) EditableAttributiAndOriginal a
editSampleFromAttributiAndValues zone attributi =
    sampleMapAttributi (createEditableAttributi zone attributi)


emptySample : Sample (Maybe Int) (AttributoMap a) b
emptySample =
    { id = Nothing, name = "", attributi = emptyAttributoMap, files = [] }


emptyNewFileUpload =
    { description = "", file = Nothing }


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        SamplesReceived response ->
            ( { model | samples = fromResult <| Result.map (\( samples, attributi ) -> { samples = samples, attributi = attributi }) <| response }, Cmd.none )

        -- The user pressed "yes, really delete!" in the modal
        ConfirmDelete sampleId ->
            ( { model | sampleDeleteRequest = Loading, deleteModalOpen = Nothing }, httpDeleteSample SampleDeleteFinished sampleId )

        -- The user closed the "Really delete?" modal
        CancelDelete ->
            ( { model | deleteModalOpen = Nothing }, Cmd.none )

        -- The deletion request for an object finished
        SampleDeleteFinished result ->
            case result of
                Err e ->
                    ( { model | sampleDeleteRequest = Failure e }, Cmd.none )

                Ok _ ->
                    ( { model | sampleDeleteRequest = Success () }, httpGetSamples SamplesReceived )

        -- The user pressed the "Add new object" button
        AddSample ->
            case model.samples of
                Success { attributi } ->
                    ( { model | editSample = Just (editSampleFromAttributiAndValues model.myTimeZone attributi emptySample) }, Cmd.none )

                _ ->
                    ( model, Cmd.none )

        -- The name of the object changed
        EditSampleName newName ->
            case model.editSample of
                Nothing ->
                    ( model, Cmd.none )

                Just editSample ->
                    ( { model | editSample = Just { editSample | name = newName } }, Cmd.none )

        -- The user pressed the submit change button (either creating or editing an object)
        EditSampleSubmit ->
            case model.editSample of
                Nothing ->
                    ( model, Cmd.none )

                Just editSample ->
                    let
                        -- either edit or create, depending on the ID
                        operation =
                            Maybe.unwrap httpCreateSample (always httpUpdateSample) editSample.id
                    in
                    if model.newFileUpload.description /= "" || isJust model.newFileUpload.file then
                        ( { model | submitErrors = [ "There is still a file in the upload form. Submit or clear the form!" ] }, Cmd.none )

                    else
                        case convertEditValues editSample.attributi of
                            Err errorList ->
                                ( { model | submitErrors = List.map (\( name, errorMessage ) -> name ++ ": " ++ errorMessage) errorList }, Cmd.none )

                            Ok editedAttributi ->
                                let
                                    sampleToSend =
                                        { id = editSample.id
                                        , name = editSample.name
                                        , attributi = editedAttributi
                                        , files = List.map .id editSample.files
                                        }
                                in
                                ( { model | modifyRequest = Loading }, operation EditSampleFinished sampleToSend )

        EditSampleCancel ->
            ( { model | editSample = Nothing, newFileUpload = emptyNewFileUpload, fileUploadRequest = NotAsked, modifyRequest = NotAsked, sampleDeleteRequest = NotAsked }, Cmd.none )

        EditSampleFinished result ->
            case result of
                Err e ->
                    ( { model | modifyRequest = Failure e }, Cmd.none )

                Ok _ ->
                    ( { model | modifyRequest = Success (), editSample = Nothing, fileUploadRequest = NotAsked, sampleDeleteRequest = NotAsked }, httpGetSamples SamplesReceived )

        EditSampleAttributo v ->
            case model.editSample of
                -- This is the unlikely case that we have an "attributo was edited" message, but sample is edited
                Nothing ->
                    ( model, Cmd.none )

                Just editSample ->
                    let
                        newEditable =
                            editEditableAttributi editSample.attributi.editableAttributi v

                        newSample =
                            { editSample | attributi = { originalAttributi = editSample.attributi.originalAttributi, editableAttributi = newEditable } }
                    in
                    ( { model | editSample = Just newSample }, Cmd.none )

        AskDelete sampleName sampleId ->
            ( { model | deleteModalOpen = Just ( sampleName, sampleId ) }, Cmd.none )

        InitiateEdit sample ->
            case model.samples of
                Success { attributi } ->
                    ( { model | editSample = Just (editSampleFromAttributiAndValues model.myTimeZone attributi (sampleMapId Just sample)) }, Cmd.none )

                _ ->
                    ( model, Cmd.none )

        EditNewFileDescription newDescription ->
            let
                oldFileUpload =
                    model.newFileUpload
            in
            ( { model | newFileUpload = { oldFileUpload | description = newDescription } }, Cmd.none )

        EditNewFileFile newFile ->
            let
                oldFileUpload =
                    model.newFileUpload
            in
            ( { model | newFileUpload = { oldFileUpload | file = Just newFile } }, Cmd.none )

        EditFileUpload ->
            case model.newFileUpload.file of
                Nothing ->
                    ( model, Cmd.none )

                Just fileToUpload ->
                    ( { model | fileUploadRequest = Loading }, httpCreateFile EditFileUploadFinished model.newFileUpload.description fileToUpload )

        EditFileUploadFinished result ->
            case result of
                Err e ->
                    ( { model | fileUploadRequest = Failure e }, Cmd.none )

                Ok file ->
                    let
                        newEditSample =
                            case model.editSample of
                                Nothing ->
                                    Nothing

                                Just editSample ->
                                    Just { editSample | files = file :: editSample.files }
                    in
                    ( { model | fileUploadRequest = Success (), newFileUpload = emptyNewFileUpload, editSample = newEditSample }, Cmd.none )

        EditFileDelete toDeleteId ->
            case model.editSample of
                Nothing ->
                    ( model, Cmd.none )

                Just editSample ->
                    let
                        newEditSample =
                            Just { editSample | files = List.filter (\x -> x.id /= toDeleteId) editSample.files }
                    in
                    ( { model | editSample = newEditSample, fileUploadRequest = NotAsked }, Cmd.none )

        EditResetNewFileUpload ->
            ( { model | newFileUpload = emptyNewFileUpload }, Cmd.none )

        EditNewFileOpenSelector ->
            ( model, File.Select.file [] EditNewFileFile )
