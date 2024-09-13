module Amarcord.EventForm exposing (Model, Msg(..), init, update, updateLiveStreamAndShiftUser, view)

import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, makeAlert, viewMarkdownSupportText)
import Amarcord.Html exposing (form_, h5_, input_, tbody_, td_, th_, thead_, tr_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Api.Data exposing (JsonCreateFileOutput, JsonEventTopLevelOutput, JsonFileOutput)
import Api.Request.Events exposing (createEventApiEventsPost)
import Api.Request.Files exposing (createFileApiFilesPost)
import File as ElmFile
import File.Select
import Html exposing (Html, button, div, h4, input, label, p, table, text, textarea, tr)
import Html.Attributes exposing (checked, class, disabled, for, id, placeholder, style, type_, value)
import Html.Events exposing (onClick, onInput)
import List
import RemoteData exposing (RemoteData(..), isLoading)
import String


type alias Model =
    { currentUserNameInput : String
    , currentShiftUserName : Maybe String
    , message : String
    , files : List JsonFileOutput
    , fileUploadRequest : RemoteData HttpError JsonCreateFileOutput
    , eventRequest : RemoteData HttpError JsonEventTopLevelOutput
    , hasLiveStream : Bool
    , postWithLiveStream : Bool
    , beamtimeId : BeamtimeId
    }


type Msg
    = NewModel Model
    | Submit
    | SubmitFinished (Result HttpError JsonEventTopLevelOutput)
    | OpenSelector (List String)
    | NewFile ElmFile.File
    | FileUploadFinished (Result HttpError JsonCreateFileOutput)
    | FileDelete Int
    | TogglePostWithLiveStream


modelValid : Model -> Bool
modelValid { currentUserNameInput, message } =
    currentUserNameInput /= "" && message /= ""


updateLiveStreamAndShiftUser : Model -> Bool -> Maybe String -> Model
updateLiveStreamAndShiftUser m b currentShiftUserName =
    let
        newUser =
            if currentShiftUserName /= m.currentShiftUserName then
                case currentShiftUserName of
                    Nothing ->
                        m.currentUserNameInput

                    Just newShiftUser ->
                        newShiftUser

            else
                m.currentUserNameInput
    in
    { m | hasLiveStream = b, currentUserNameInput = newUser, currentShiftUserName = currentShiftUserName }


init : BeamtimeId -> String -> Model
init beamtimeId currentUserNameInput =
    { currentUserNameInput = currentUserNameInput
    , currentShiftUserName = Nothing
    , message = ""
    , files = []
    , fileUploadRequest = NotAsked
    , eventRequest = NotAsked
    , hasLiveStream = False
    , beamtimeId = beamtimeId
    , postWithLiveStream = False
    }


view : Model -> Html Msg
view { eventRequest, currentShiftUserName, currentUserNameInput, message, files, fileUploadRequest, hasLiveStream, beamtimeId, postWithLiveStream } =
    let
        eventError =
            case eventRequest of
                Success _ ->
                    p [ class "text-success" ] [ text "Message added!" ]

                Failure e ->
                    makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to add message!" ], showError e ]

                _ ->
                    text ""

        fileUploadError =
            case fileUploadRequest of
                NotAsked ->
                    text ""

                Loading ->
                    text "Uploading"

                Failure e ->
                    showError e

                Success _ ->
                    p [ class "text-success" ] [ text "File added!" ]

        viewFileRow : JsonFileOutput -> Html Msg
        viewFileRow file =
            tr [ class "align-middle" ]
                [ td_ [ text (String.fromInt file.id) ]
                , td_ [ text file.fileName ]
                , td_ [ text file.type__ ]
                , td_ [ text file.description ]
                , td_ [ button [ class "btn btn-danger btn-sm", type_ "button", onClick (FileDelete file.id) ] [ icon { name = "trash" } ] ]
                ]

        viewUserNameInput =
            div [ class "form-floating" ]
                [ input_
                    [ value currentUserNameInput
                    , type_ "text"
                    , class "form-control form-control-sm"
                    , placeholder "User name"
                    , id "user-name"
                    , onInput
                        (\e ->
                            NewModel
                                { eventRequest = eventRequest
                                , currentShiftUserName = currentShiftUserName
                                , currentUserNameInput = e
                                , message = message
                                , files = files
                                , fileUploadRequest = fileUploadRequest
                                , hasLiveStream = hasLiveStream
                                , postWithLiveStream = postWithLiveStream
                                , beamtimeId = beamtimeId
                                }
                        )
                    ]
                , label [ for "user-name" ] [ text "User name" ]
                , div [ class "form-text" ] [ text "If present, this name is filled in from the current schedule entry." ]
                ]

        viewTextInput =
            div [ class "form-floating" ]
                [ textarea
                    [ id "event-text"
                    , class "form-control"
                    , style "height" "8em"
                    , value message
                    , onInput
                        (\e ->
                            NewModel
                                { eventRequest = eventRequest
                                , currentShiftUserName = currentShiftUserName
                                , currentUserNameInput = currentUserNameInput
                                , message = e
                                , files = files
                                , fileUploadRequest = fileUploadRequest
                                , hasLiveStream = hasLiveStream
                                , beamtimeId = beamtimeId
                                , postWithLiveStream = postWithLiveStream
                                }
                        )
                    ]
                    []
                , viewMarkdownSupportText
                , label [ for "event-text" ] [ text "Logbook message" ]
                ]
    in
    form_
        [ div [ class "row" ]
            [ div [ class "col" ]
                [ h5_ [ icon { name = "plus-lg" }, text " Add logbook entry" ]
                , div [ class "mb-3" ] [ viewUserNameInput ]
                , div [ class "mb-3" ] [ viewTextInput ]
                , div [ class "hstack gap-1 mb-3" ]
                    [ button
                        [ type_ "button"
                        , class "btn btn-outline-secondary"
                        , onClick (OpenSelector [ "image/*" ])
                        , style "white-space" "nowrap"
                        ]
                        [ icon { name = "camera" }, text " Upload image" ]
                    , button
                        [ type_ "button"
                        , class "btn btn-outline-secondary"
                        , onClick (OpenSelector [])
                        , style "white-space" "nowrap"
                        ]
                        [ icon { name = "upload" }, text " Upload file" ]
                    ]
                , div [ class "hstack gap-3" ]
                    [ button
                        [ onClick Submit
                        , disabled (isLoading eventRequest || currentUserNameInput == "" || message == "")
                        , type_ "button"
                        , class "btn btn-primary"
                        , style "white-space" "nowrap"
                        ]
                        [ icon { name = "send" }, text " Add log entry" ]
                    , if hasLiveStream then
                        div [ class "form-check form-switch" ]
                            [ input
                                [ class "form-check-input"
                                , type_ "checkbox"
                                , id "post-with-live-stream"
                                , checked postWithLiveStream
                                , onClick TogglePostWithLiveStream
                                ]
                                []
                            , label [ class "form-check-label", for "post-with-live-stream" ] [ text "Attach live stream image" ]
                            ]

                      else
                        text ""
                    ]
                , eventError
                , fileUploadError
                ]
            ]
        , div [ class "row" ]
            [ if List.isEmpty files then
                text ""

              else
                table [ class "table table-striped table-sm" ]
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
        ]


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        TogglePostWithLiveStream ->
            ( { model | postWithLiveStream = not model.postWithLiveStream }, Cmd.none )

        NewModel newModel ->
            ( newModel, Cmd.none )

        Submit ->
            if modelValid model then
                ( { model | eventRequest = Loading }
                , send SubmitFinished
                    (createEventApiEventsPost
                        { beamtimeId = model.beamtimeId
                        , withLiveStream = model.postWithLiveStream
                        , event = { source = model.currentUserNameInput, text = model.message, fileIds = List.map .id model.files, level = "user" }
                        }
                    )
                )

            else
                ( model, Cmd.none )

        SubmitFinished result ->
            case result of
                Err e ->
                    ( { model | eventRequest = Failure e }, Cmd.none )

                Ok v ->
                    ( { model
                        | eventRequest = Success v
                        , message = ""
                        , files = []
                        , fileUploadRequest = NotAsked
                      }
                    , Cmd.none
                    )

        NewFile newFile ->
            ( { model | fileUploadRequest = Loading }, send FileUploadFinished (createFileApiFilesPost newFile (ElmFile.name newFile) "False") )

        OpenSelector mimes ->
            ( model, File.Select.file mimes NewFile )

        FileUploadFinished result ->
            case result of
                Err e ->
                    ( { model | fileUploadRequest = Failure e }, Cmd.none )

                Ok file ->
                    ( { model
                        | fileUploadRequest = Success file
                        , files =
                            { id = file.id
                            , description = file.description
                            , type__ = file.type__
                            , originalPath = file.originalPath
                            , fileName = file.fileName
                            , sizeInBytes = file.sizeInBytes
                            }
                                :: model.files
                      }
                    , Cmd.none
                    )

        FileDelete fileId ->
            ( { model | files = List.filter (\f -> f.id /= fileId) model.files }, Cmd.none )
