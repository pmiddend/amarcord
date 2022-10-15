module Amarcord.EventForm exposing (Model, Msg(..), init, update, updateLiveStream, view)

import Amarcord.API.Requests exposing (IncludeLiveStream(..), RequestError, httpCreateEvent, httpCreateFile)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, makeAlert)
import Amarcord.File exposing (File)
import Amarcord.Html exposing (form_, h5_, input_, tbody_, td_, th_, thead_, tr_)
import File as ElmFile
import File.Select
import Hotkeys exposing (onEnter)
import Html exposing (Html, button, div, h4, label, p, table, text, tr)
import Html.Attributes exposing (class, disabled, for, id, placeholder, style, type_, value)
import Html.Events exposing (onClick, onInput)
import List
import RemoteData exposing (RemoteData(..), isLoading)
import String


type alias Model =
    { userName : String
    , message : String
    , files : List File
    , fileUploadRequest : RemoteData RequestError ()
    , eventRequest : RemoteData RequestError ()
    , hasLiveStream : Bool
    }


type Msg
    = NewModel Model
    | Submit IncludeLiveStream
    | SubmitFinished (Result RequestError ())
    | OpenSelector (List String)
    | NewFile ElmFile.File
    | FileUploadFinished (Result RequestError File)
    | FileDelete Int


modelValid : Model -> Bool
modelValid { userName, message } =
    userName /= "" && message /= ""


updateLiveStream : Model -> Bool -> Model
updateLiveStream m b =
    { m | hasLiveStream = b }


init : Model
init =
    { userName = "User"
    , message = ""
    , files = []
    , fileUploadRequest = NotAsked
    , eventRequest = NotAsked
    , hasLiveStream = False
    }


view : Model -> Html Msg
view { eventRequest, userName, message, files, fileUploadRequest, hasLiveStream } =
    let
        eventError =
            case eventRequest of
                Success _ ->
                    p [ class "text-success" ] [ text "Message added!" ]

                Failure e ->
                    makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to add message!" ], showRequestError e ]

                _ ->
                    text ""

        fileUploadError =
            case fileUploadRequest of
                NotAsked ->
                    text ""

                Loading ->
                    text "Uploading"

                Failure e ->
                    showRequestError e

                Success _ ->
                    p [ class "text-success" ] [ text "File added!" ]

        viewFileRow file =
            tr [ class "align-middle" ]
                [ td_ [ text (String.fromInt file.id) ]
                , td_ [ text file.fileName ]
                , td_ [ text file.type_ ]
                , td_ [ text file.description ]
                , td_ [ button [ class "btn btn-danger btn-sm", type_ "button", onClick (FileDelete file.id) ] [ icon { name = "trash" } ] ]
                ]
    in
    form_
        [ div [ class "row" ]
            [ div [ class "col" ]
                [ h5_ [ text "Did something happen just now? Tell us!" ]
                , div [ class "row g-2" ]
                    [ div [ class "col-3" ]
                        [ div [ class "form-floating" ]
                            [ input_
                                [ value userName
                                , type_ "text"
                                , class "form-control form-control-sm"
                                , placeholder "User name"
                                , id "user-name"
                                , onInput (\e -> NewModel { eventRequest = eventRequest, userName = e, message = message, files = files, fileUploadRequest = fileUploadRequest, hasLiveStream = hasLiveStream })
                                ]
                            , label [ for "user-name" ] [ text "User name" ]
                            ]
                        ]
                    , div [ class "col-9" ]
                        [ div
                            [ class "input-group mb-3" ]
                            [ div [ class "form-floating flex-grow-1" ]
                                [ input_
                                    [ value message
                                    , type_ "text"
                                    , class "form-control"
                                    , id "event-text"
                                    , onEnter (Submit NoLiveStream)
                                    , onInput (\e -> NewModel { eventRequest = eventRequest, userName = userName, message = e, files = files, fileUploadRequest = fileUploadRequest, hasLiveStream = hasLiveStream })
                                    ]
                                , label [ for "event-text" ] [ text "What happened?" ]
                                ]
                            , button
                                [ type_ "button"
                                , class "btn btn-outline-secondary"
                                , onClick (OpenSelector [ "image/*" ])
                                , style "white-space" "nowrap"
                                ]
                                [ icon { name = "camera" }, text " Image" ]
                            , button
                                [ type_ "button"
                                , class "btn btn-outline-secondary"
                                , onClick (OpenSelector [])
                                , style "white-space" "nowrap"
                                ]
                                [ icon { name = "upload" }, text " File" ]
                            , button
                                [ onClick (Submit NoLiveStream)
                                , disabled (isLoading eventRequest || userName == "" || message == "")
                                , type_ "button"
                                , class "btn btn-primary"
                                , style "white-space" "nowrap"
                                ]
                                [ icon { name = "send" }, text " Post" ]
                            , if hasLiveStream then
                                button
                                    [ onClick (Submit WithLiveStream)
                                    , disabled (isLoading eventRequest || userName == "" || message == "")
                                    , type_ "button"
                                    , class "btn btn-primary"
                                    , style "white-space" "nowrap"
                                    ]
                                    [ icon { name = "camera-video-fill" }, text " Post with live stream" ]

                              else
                                text ""
                            ]
                        ]
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
        NewModel newModel ->
            ( newModel, Cmd.none )

        Submit includeStream ->
            if modelValid model then
                ( { model | eventRequest = Loading }
                , httpCreateEvent SubmitFinished includeStream model.userName model.message (List.map .id model.files)
                )

            else
                ( model, Cmd.none )

        SubmitFinished result ->
            case result of
                Err e ->
                    ( { model | eventRequest = Failure e }, Cmd.none )

                Ok _ ->
                    ( { model
                        | eventRequest = Success ()
                        , message = ""
                        , files = []
                        , fileUploadRequest = NotAsked
                      }
                    , Cmd.none
                    )

        NewFile newFile ->
            ( { model | fileUploadRequest = Loading }, httpCreateFile FileUploadFinished (ElmFile.name newFile) newFile )

        OpenSelector mimes ->
            ( model, File.Select.file mimes NewFile )

        FileUploadFinished result ->
            case result of
                Err e ->
                    ( { model | fileUploadRequest = Failure e }, Cmd.none )

                Ok file ->
                    ( { model | fileUploadRequest = Success (), files = file :: model.files }, Cmd.none )

        FileDelete fileId ->
            ( { model | files = List.filter (\f -> f.id /= fileId) model.files }, Cmd.none )
