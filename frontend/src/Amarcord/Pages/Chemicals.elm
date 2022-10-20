module Amarcord.Pages.Chemicals exposing (Model, Msg, init, update, view)

import Amarcord.API.Requests exposing (ChemicalsResponse, RequestError, httpCreateChemical, httpCreateFile, httpDeleteChemical, httpGetChemicals, httpUpdateChemical)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Attributo
    exposing
        ( Attributo
        , AttributoMap
        , AttributoType
        , AttributoValue
        , emptyAttributoMap
        )
import Amarcord.AttributoHtml exposing (AttributoFormMsg(..), AttributoNameWithValueUpdate, EditableAttributiAndOriginal, convertEditValues, createEditableAttributi, editEditableAttributi, extractStringAttributo, findEditableAttributo, makeAttributoHeader, mutedSubheader, viewAttributoCell, viewAttributoForm)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert, mimeTypeToIcon, viewRemoteData)
import Amarcord.Chemical exposing (Chemical, ChemicalId, chemicalMapAttributi, chemicalMapId)
import Amarcord.Crystallography exposing (validateCellDescription, validatePointGroup)
import Amarcord.Dialog as Dialog
import Amarcord.File exposing (File)
import Amarcord.Html exposing (br_, form_, h4_, h5_, img_, input_, li_, p_, span_, strongText, sup_, tbody_, td_, th_, thead_, tr_)
import Amarcord.MarkdownUtil exposing (markupWithoutErrors)
import Amarcord.Route exposing (makeFilesLink)
import Amarcord.Util exposing (HereAndNow, scrollToTop)
import Dict
import File as ElmFile
import File.Select
import Html exposing (..)
import Html.Attributes exposing (attribute, class, disabled, for, href, id, src, style, type_, value)
import Html.Events exposing (onClick, onInput)
import List exposing (length, singleton)
import Maybe.Extra as Maybe exposing (isJust, isNothing)
import RemoteData exposing (RemoteData(..), fromResult)
import String
import Time exposing (Zone)


type alias ChemicalsAndAttributi =
    { chemicals : List (Chemical ChemicalId (AttributoMap AttributoValue) File)
    , attributi : List (Attributo AttributoType)
    }


type alias NewFileUpload =
    { file : Maybe ElmFile.File
    , description : String
    }


type alias Model =
    { chemicals : RemoteData RequestError ChemicalsAndAttributi
    , deleteModalOpen : Maybe ( String, ChemicalId )
    , chemicalDeleteRequest : RemoteData RequestError ()
    , fileUploadRequest : RemoteData RequestError ()
    , editChemical : Maybe (Chemical (Maybe Int) EditableAttributiAndOriginal File)
    , modifyRequest : RemoteData RequestError ()
    , myTimeZone : Zone
    , submitErrors : List (Html Msg)
    , newFileUpload : NewFileUpload
    }


type Msg
    = ChemicalsReceived ChemicalsResponse
    | CancelDelete
    | AskDelete String ChemicalId
    | InitiateEdit (Chemical Int (AttributoMap AttributoValue) File)
    | ConfirmDelete ChemicalId
    | AddChemical
    | EditChemicalName String
    | ChemicalDeleteFinished (Result RequestError ())
    | EditChemicalSubmit
    | EditChemicalCancel
    | EditChemicalFinished (Result RequestError ())
    | EditChemicalAttributo AttributoNameWithValueUpdate
    | EditNewFileDescription String
    | EditNewFileFile ElmFile.File
    | EditFileUpload
    | EditFileUploadFinished (Result RequestError File)
    | EditFileDelete Int
    | EditResetNewFileUpload
    | EditNewFileOpenSelector
    | Nop


init : HereAndNow -> ( Model, Cmd Msg )
init { zone } =
    ( { chemicals = Loading
      , deleteModalOpen = Nothing
      , chemicalDeleteRequest = NotAsked
      , modifyRequest = NotAsked
      , fileUploadRequest = NotAsked
      , editChemical = Nothing
      , myTimeZone = zone
      , submitErrors = []
      , newFileUpload = { file = Nothing, description = "" }
      }
    , httpGetChemicals ChemicalsReceived
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
                , td_ [ markupWithoutErrors file.description ]
                , td_ [ button [ class "btn btn-danger btn-sm", type_ "button", onClick (EditFileDelete file.id) ] [ icon { name = "trash" } ] ]
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
        let
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
        in
        filesTable ++ uploadForm


viewEditForm : List (Chemical ChemicalId (AttributoMap AttributoValue) File) -> RemoteData RequestError () -> List (Html Msg) -> NewFileUpload -> Chemical (Maybe Int) EditableAttributiAndOriginal File -> Html Msg
viewEditForm chemicals fileUploadRequest submitErrorsList newFileUpload editingChemical =
    let
        attributoFormMsgToMsg : AttributoFormMsg -> Msg
        attributoFormMsgToMsg x =
            case x of
                AttributoFormValueUpdate vu ->
                    EditChemicalAttributo vu

                -- With chemicals, pressing return and submitting would close the form.
                -- That's very unexpected!
                AttributoFormSubmit ->
                    Nop

        attributiFormEntries =
            List.map (\attributo -> Html.map attributoFormMsgToMsg (viewAttributoForm [] attributo)) editingChemical.attributi.editableAttributi

        otherChemicalsNames =
            List.map .name <|
                case editingChemical.id of
                    Nothing ->
                        chemicals

                    Just editingId ->
                        List.filter (\s -> not <| .id s == editingId) chemicals

        submitErrors =
            case submitErrorsList of
                [] ->
                    [ text "" ]

                errors ->
                    [ p_ [ strongText "There were submission errors:" ]
                    , ul [ class "text-danger" ] <| List.map (\e -> li_ [ e ]) errors
                    ]

        isDuplicateName =
            otherChemicalsNames
                |> List.map String.trim
                |> List.member (String.trim editingChemical.name)

        isValidChemicalName =
            not isDuplicateName
                && (String.trim editingChemical.name /= "")

        isInvalidChemicalNameStyle =
            if not isValidChemicalName then
                " is-invalid"

            else
                ""
    in
    form_ <|
        [ h4_
            [ icon { name = "plus-square" }
            , text
                (if isNothing editingChemical.id then
                    " Add new chemical"

                 else
                    " Edit chemical"
                )
            ]
        , p [ class "lead text-muted" ] [ text "Note: If you prepared your crystals in multiple batches, please create one chemical per batch. This helps during analysis." ]
        , p [ class "lead text-muted" ]
            [ text "For the details on the "
            , strong [ style "font-weight" "bold" ] [ text "cell description" ]
            , text " please refer to the "
            , a [ href "https://www.desy.de/~twhite/crystfel/tutorial-0.9.1.html#index" ] [ text "CrystFEL tutorial, section 10" ]
            , text ". For the "
            , strong [ style "font-weight" "bold" ] [ text "point group" ]
            , text " please refer to the "
            , a [ href "https://www.desy.de/~twhite/crystfel/tutorial-0.9.1.html#merge" ] [ text "CrystFEL tutorial, section 13" ]
            , text "."
            ]
        , div [ class "mb-3" ]
            [ label [ for "name", class "form-label" ] [ text "Name", sup_ [ text "*" ] ]
            , input_
                [ type_ "text"
                , class
                    ("form-control"
                        ++ isInvalidChemicalNameStyle
                    )
                , id "name"
                , value editingChemical.name
                , onInput EditChemicalName
                ]
            , if String.trim editingChemical.name /= "" then
                if isDuplicateName then
                    div [ class "invalid-feedback" ] [ text "Name already used" ]

                else
                    text ""

              else
                div [ class "invalid-feedback" ] [ text "Name is mandatory" ]
            ]
        ]
            ++ attributiFormEntries
            ++ viewFiles fileUploadRequest newFileUpload editingChemical.files
            ++ submitErrors
            ++ [ button
                    [ class "btn btn-primary me-3 mb-3"
                    , onClick EditChemicalSubmit
                    , disabled (not isValidChemicalName)
                    , type_ "button"
                    ]
                    [ icon { name = "plus-lg" }
                    , text
                        (if isNothing editingChemical.id then
                            " Add new chemical"

                         else
                            " Confirm edit"
                        )
                    ]
               , button
                    [ class "btn btn-secondary me-3 mb-3"
                    , onClick EditChemicalCancel
                    , type_ "button"
                    ]
                    [ icon { name = "x-lg" }, text " Cancel" ]
               ]


viewChemicalRow : Zone -> List (Attributo AttributoType) -> Chemical ChemicalId (AttributoMap AttributoValue) File -> Html Msg
viewChemicalRow zone attributi chemical =
    let
        viewFile { id, type_, fileName, description } =
            li [ class "list-group-item" ] <|
                if String.startsWith "image/" type_ then
                    [ figure [ class "figure" ] [ img_ [ src (makeFilesLink id), style "width" "20em" ], figcaption [ class "figure-caption" ] [ a [ href (makeFilesLink id), class "stretched-link" ] [ text description ] ] ] ]

                else
                    [ mimeTypeToIcon type_
                    , text " "
                    , span [ attribute "data-tooltip" description, class "align-top" ] [ a [ href (makeFilesLink id), class "stretched-link" ] [ text fileName ] ]
                    ]

        files =
            case chemical.files of
                [] ->
                    text ""

                _ ->
                    ul [ class "list-group list-group-flush" ] (List.map viewFile chemical.files)
    in
    tr_ <|
        td_ [ text <| String.fromInt chemical.id ]
            :: td_ [ text chemical.name ]
            :: List.map (viewAttributoCell { shortDateTime = False, colorize = False } zone Dict.empty chemical.attributi) attributi
            ++ [ td_ [ files ]
               , td [ class "text-nowrap" ]
                    [ button [ class "btn btn-sm btn-danger me-3", onClick (AskDelete chemical.name chemical.id) ] [ icon { name = "trash" } ]
                    , button [ class "btn btn-sm btn-info", onClick (InitiateEdit chemical) ] [ icon { name = "pencil-square" } ]
                    ]
               ]


viewChemicalTable : Zone -> List (Chemical ChemicalId (AttributoMap AttributoValue) File) -> List (Attributo AttributoType) -> Html Msg
viewChemicalTable zone chemicals attributi =
    let
        attributiColumns : List (Html msg)
        attributiColumns =
            List.map (th_ << makeAttributoHeader) attributi
    in
    table [ class "table" ]
        [ thead_
            [ tr [ class "align-top" ] <|
                th_ [ text "ID" ]
                    :: th_ [ text "Name" ]
                    :: attributiColumns
                    ++ [ th_ [ text "Files", br_, mutedSubheader "Hover to see description, click to download" ]
                       , th_ [ text "Actions" ]
                       ]
            ]
        , tbody_
            (List.map (viewChemicalRow zone attributi) chemicals)
        ]


viewInner : Model -> List (Html Msg)
viewInner model =
    case model.chemicals of
        NotAsked ->
            singleton <| text ""

        Loading ->
            singleton <| loadingBar "Loading chemicals..."

        Failure e ->
            singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve chemicals" ], showRequestError e ]

        Success { chemicals, attributi } ->
            let
                prefix =
                    case model.editChemical of
                        Nothing ->
                            button [ class "btn btn-primary", onClick AddChemical ] [ icon { name = "plus-lg" }, text " Add chemical" ]

                        Just editChemical ->
                            viewEditForm chemicals model.fileUploadRequest model.submitErrors model.newFileUpload editChemical

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
                    case model.chemicalDeleteRequest of
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
            [ prefix, modifyRequestResult, deleteRequestResult, viewChemicalTable model.myTimeZone (List.sortBy .id chemicals) attributi ]


view : Model -> Html Msg
view model =
    let
        maybeDeleteModal =
            case model.deleteModalOpen of
                Nothing ->
                    []

                Just ( chemicalName, chemicalId ) ->
                    [ Dialog.view
                        (Just
                            { header = Nothing
                            , body = Just (span_ [ text "Really delete chemical ", strongText chemicalName, text "?" ])
                            , closeMessage = Just CancelDelete
                            , containerClass = Nothing
                            , footer = Just (button [ class "btn btn-danger", onClick (ConfirmDelete chemicalId) ] [ text "Really delete!" ])
                            }
                        )
                    ]
    in
    div [ class "container" ] (maybeDeleteModal ++ viewInner model)


editChemicalFromAttributiAndValues : Zone -> List (Attributo AttributoType) -> Chemical (Maybe Int) (AttributoMap AttributoValue) a -> Chemical (Maybe Int) EditableAttributiAndOriginal a
editChemicalFromAttributiAndValues zone attributi =
    chemicalMapAttributi (createEditableAttributi zone attributi)


emptyChemical : Chemical (Maybe Int) (AttributoMap a) b
emptyChemical =
    { id = Nothing, name = "", attributi = emptyAttributoMap, files = [] }


emptyNewFileUpload : { description : String, file : Maybe a }
emptyNewFileUpload =
    { description = "", file = Nothing }


validatePointGroupAndCellDescription : Chemical a EditableAttributiAndOriginal b -> Result (Html msg) ()
validatePointGroupAndCellDescription { attributi } =
    findEditableAttributo attributi.editableAttributi "point group"
        |> Result.andThen extractStringAttributo
        |> Result.andThen validatePointGroup
        |> Result.andThen (\_ -> findEditableAttributo attributi.editableAttributi "cell description")
        |> Result.andThen extractStringAttributo
        |> Result.andThen validateCellDescription


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        ChemicalsReceived response ->
            ( { model | chemicals = fromResult <| Result.map (\( chemicals, attributi ) -> { chemicals = chemicals, attributi = attributi }) <| response }, Cmd.none )

        -- The user pressed "yes, really delete!" in the modal
        ConfirmDelete chemicalId ->
            ( { model | chemicalDeleteRequest = Loading, deleteModalOpen = Nothing }, httpDeleteChemical ChemicalDeleteFinished chemicalId )

        -- The user closed the "Really delete?" modal
        CancelDelete ->
            ( { model | deleteModalOpen = Nothing }, Cmd.none )

        -- The deletion request for an object finished
        ChemicalDeleteFinished result ->
            case result of
                Err e ->
                    ( { model | chemicalDeleteRequest = Failure e }, Cmd.none )

                Ok _ ->
                    ( { model | chemicalDeleteRequest = Success () }, httpGetChemicals ChemicalsReceived )

        -- The user pressed the "Add new object" button
        AddChemical ->
            case model.chemicals of
                Success { attributi } ->
                    ( { model | editChemical = Just (editChemicalFromAttributiAndValues model.myTimeZone attributi emptyChemical) }, Cmd.none )

                _ ->
                    ( model, Cmd.none )

        -- The name of the object changed
        EditChemicalName newName ->
            case model.editChemical of
                Nothing ->
                    ( model, Cmd.none )

                Just editChemical ->
                    ( { model | editChemical = Just { editChemical | name = newName } }, Cmd.none )

        -- The user pressed the submit change button (either creating or editing an object)
        EditChemicalSubmit ->
            case model.editChemical of
                Nothing ->
                    ( model, Cmd.none )

                Just editChemical ->
                    if model.newFileUpload.description /= "" || isJust model.newFileUpload.file then
                        ( { model | submitErrors = [ text "There is still a file in the upload form. Submit or clear the form!" ] }, Cmd.none )

                    else
                        case validatePointGroupAndCellDescription editChemical of
                            Err errorMessage ->
                                ( { model | submitErrors = [ errorMessage ] }, Cmd.none )

                            Ok _ ->
                                case convertEditValues model.myTimeZone editChemical.attributi of
                                    Err errorList ->
                                        ( { model | submitErrors = List.map (\( name, errorMessage ) -> text <| name ++ ": " ++ errorMessage) errorList }, Cmd.none )

                                    Ok editedAttributi ->
                                        let
                                            operation =
                                                Maybe.unwrap httpCreateChemical (always httpUpdateChemical) editChemical.id

                                            chemicalToSend =
                                                { id = editChemical.id
                                                , name = editChemical.name
                                                , attributi = editedAttributi
                                                , files = List.map .id editChemical.files
                                                }
                                        in
                                        ( { model | modifyRequest = Loading }, operation EditChemicalFinished chemicalToSend )

        EditChemicalCancel ->
            ( { model | editChemical = Nothing, newFileUpload = emptyNewFileUpload, fileUploadRequest = NotAsked, modifyRequest = NotAsked, chemicalDeleteRequest = NotAsked, submitErrors = [] }, Cmd.none )

        EditChemicalFinished result ->
            case result of
                Err e ->
                    ( { model | modifyRequest = Failure e }, Cmd.none )

                Ok _ ->
                    ( { model | modifyRequest = Success (), editChemical = Nothing, fileUploadRequest = NotAsked, chemicalDeleteRequest = NotAsked, submitErrors = [] }, httpGetChemicals ChemicalsReceived )

        EditChemicalAttributo v ->
            case model.editChemical of
                -- This is the unlikely case that we have an "attributo was edited" message, but chemical is edited
                Nothing ->
                    ( model, Cmd.none )

                Just editChemical ->
                    let
                        newEditable =
                            editEditableAttributi editChemical.attributi.editableAttributi v

                        newChemical =
                            { editChemical | attributi = { originalAttributi = editChemical.attributi.originalAttributi, editableAttributi = newEditable } }
                    in
                    ( { model | editChemical = Just newChemical }, Cmd.none )

        AskDelete chemicalName chemicalId ->
            ( { model | deleteModalOpen = Just ( chemicalName, chemicalId ) }, Cmd.none )

        InitiateEdit chemical ->
            case model.chemicals of
                Success { attributi } ->
                    ( { model | editChemical = Just (editChemicalFromAttributiAndValues model.myTimeZone attributi (chemicalMapId Just chemical)) }, scrollToTop (always Nop) )

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
                        newEditChemical =
                            case model.editChemical of
                                Nothing ->
                                    Nothing

                                Just editChemical ->
                                    Just { editChemical | files = file :: editChemical.files }
                    in
                    ( { model | fileUploadRequest = Success (), newFileUpload = emptyNewFileUpload, editChemical = newEditChemical }, Cmd.none )

        EditFileDelete toDeleteId ->
            case model.editChemical of
                Nothing ->
                    ( model, Cmd.none )

                Just editChemical ->
                    let
                        newEditChemical =
                            Just { editChemical | files = List.filter (\x -> x.id /= toDeleteId) editChemical.files }
                    in
                    ( { model | editChemical = newEditChemical, fileUploadRequest = NotAsked }, Cmd.none )

        EditResetNewFileUpload ->
            ( { model | newFileUpload = emptyNewFileUpload }, Cmd.none )

        EditNewFileOpenSelector ->
            ( model, File.Select.file [] EditNewFileFile )

        Nop ->
            ( model, Cmd.none )
