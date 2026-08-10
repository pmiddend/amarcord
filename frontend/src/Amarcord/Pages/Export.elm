module Amarcord.Pages.Export exposing (Model, Msg, init, pageTitle, subscriptions, update, view)

import Amarcord.API.Requests
    exposing
        ( BeamtimeId
        , beamtimeIdToString
        )
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, makeAlert, viewAlert)
import Amarcord.Html exposing (code_, div_, h5_, input_, p_, tbody_, td_, th_, thead_, tr_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.Util exposing (HereAndNow, formatPosixHumanFriendly)
import Api.Data exposing (JsonExportJobOutput, JsonReadExportJobs)
import Api.Request.Exports exposing (createExportJobApiExportsBeamtimeIdPost, readExportsApiExportsBeamtimeIdGet)
import Html exposing (Html, a, br, button, div, em, h3, label, p, table, td, text)
import Html.Attributes exposing (checked, class, disabled, for, href, id, type_, value)
import Html.Events exposing (onClick, onInput)
import RemoteData exposing (RemoteData(..), fromResult, isLoading)
import Time exposing (Posix, Zone, millisToPosix)


type Msg
    = CreateNewExport
    | Refresh Posix
    | CreateNewExportDone (Result HttpError JsonExportJobOutput)
    | ExportsReceived (Result HttpError JsonReadExportJobs)
    | ToggleExportStreamFiles


type alias Model =
    { beamtimeId : BeamtimeId
    , zone : Zone
    , now : Posix
    , refreshRequest : RemoteData HttpError ()
    , exports : RemoteData HttpError JsonReadExportJobs
    , createNewExportRequest : RemoteData HttpError JsonExportJobOutput
    , exportStreamFiles : Bool
    }


pageTitle : String
pageTitle =
    "Export"


init : HereAndNow -> BeamtimeId -> ( Model, Cmd Msg )
init { zone, now } beamtimeId =
    ( { beamtimeId = beamtimeId
      , zone = zone
      , now = now
      , refreshRequest = NotAsked
      , exports = Loading
      , createNewExportRequest = NotAsked
      , exportStreamFiles = True
      }
    , send ExportsReceived (readExportsApiExportsBeamtimeIdGet beamtimeId)
    )


subscriptions : List (Sub Msg)
subscriptions =
    [ Time.every 10000 Refresh ]


view : Model -> Html Msg
view model =
    div [ class "container" ]
        [ h3 [ class "mt-3" ] [ icon { name = "file-earmark-zip" }, text " ZIP export" ]
        , p [ class "text-muted" ] [ em [] [ text "Note" ], text ": To import the exported .zip files into another instance of AMARCORD, go to the beamline overview page and use the form there." ]
        , case model.exports of
            Success { exportBasePath, exportJobs } ->
                case exportBasePath of
                    Nothing ->
                        viewAlert [ AlertInfo ]
                            [ text "ZIP file exports are disabled for this instance. Check the documentation on how to enable them. It boils down to setting a directory that has enough space and then restarting the web server."
                            ]

                    Just exportDirectory ->
                        div_
                            [ case exportJobs of
                                [] ->
                                    text ""

                                _ ->
                                    let
                                        viewExportRow { id, startedLocal, stoppedLocal, statusMessage, outputPath, sizeInMebibytes } =
                                            tr_
                                                [ td_ [ text (String.fromInt id) ]
                                                , td_
                                                    [ case startedLocal of
                                                        Nothing ->
                                                            text "Queued"

                                                        Just _ ->
                                                            text statusMessage
                                                    ]
                                                , td_ [ text (String.fromInt sizeInMebibytes ++ "MiB") ]
                                                , td_
                                                    [ case startedLocal of
                                                        Nothing ->
                                                            text ""

                                                        Just startedLocalReal ->
                                                            text (formatPosixHumanFriendly model.zone (millisToPosix startedLocalReal))
                                                    ]
                                                , td_
                                                    [ case stoppedLocal of
                                                        Nothing ->
                                                            text ""

                                                        Just stoppedLocalReal ->
                                                            text (formatPosixHumanFriendly model.zone (millisToPosix stoppedLocalReal))
                                                    ]
                                                , td_ [ code_ [ text outputPath ] ]
                                                , td [ class "text-nowrap" ]
                                                    [ a
                                                        [ href
                                                            ("api/exports/"
                                                                ++ String.fromInt model.beamtimeId
                                                                ++ "/"
                                                                ++ String.fromInt id
                                                                ++ ".zip"
                                                            )
                                                        ]
                                                        [ icon { name = "download" }, text " Download" ]
                                                    ]
                                                ]
                                    in
                                    div_
                                        [ h5_ [ text "Existing exports" ]
                                        , table [ class "table table-striped" ]
                                            [ thead_
                                                [ tr_
                                                    [ th_ [ text "ID" ]
                                                    , th_ [ text "Status" ]
                                                    , th_ [ text "Size" ]
                                                    , th_ [ text "Started" ]
                                                    , th_ [ text "Stopped" ]
                                                    , th_ [ text "Path" ]
                                                    , th_ [ text "Link" ]
                                                    ]
                                                ]
                                            , tbody_ (List.map viewExportRow exportJobs)
                                            ]
                                        ]
                            , div [ class "hstack gap-3" ]
                                [ button
                                    ([ class "btn btn-primary", onClick CreateNewExport ]
                                        ++ (if isLoading model.createNewExportRequest then
                                                [ disabled True ]

                                            else
                                                []
                                           )
                                    )
                                    [ icon { name = "send" }, text " Create new export" ]
                                , div [ class "form-check form-switch" ]
                                    [ input_
                                        [ class "form-check-input"
                                        , type_ "checkbox"
                                        , value ""
                                        , checked model.exportStreamFiles
                                        , id "export-stream-files"
                                        , onInput (always ToggleExportStreamFiles)
                                        ]
                                    , label
                                        [ class "form-check-label"
                                        , for "export-stream-files"
                                        ]
                                        [ text "Export stream files" ]
                                    ]
                                ]
                            , p [ class "text-muted" ]
                                [ text "This will queue a new export to be done in the background. You can check the status in this tab, and once it is finished, you can download the .zip file. "
                                , br [] []
                                , em [] [ text "Note" ]
                                , text ": Exporting stream files can take a very long time and produce a huge .zip file."
                                ]
                            , p_ [ text "Export directory: ", code_ [ text exportDirectory ], text ". This must have enough space available to contain the export data." ]
                            , case model.createNewExportRequest of
                                Success { exportJobId } ->
                                    div [ class "badge text-bg-success" ] [ text ("Export with ID " ++ String.fromInt exportJobId ++ " created! This page will auto-refresh regularly.") ]

                                Failure e ->
                                    makeAlert [ AlertDanger ] [ text "Creation of export job failed!", showError e ]

                                _ ->
                                    text ""
                            ]

            _ ->
                text "Waiting for current exports"
        , h3 [ class "mt-3" ] [ icon { name = "file-earmark-spreadsheet" }, text " Spreadsheet export" ]
        , p [] [ text "Done with the experiment? Ready for more analyses? Just download the whole database with a single click!" ]
        , a [ href ("api/" ++ beamtimeIdToString model.beamtimeId ++ "/spreadsheet.zip"), class "btn btn-secondary" ] [ icon { name = "file-earmark-spreadsheet" }, text " Download spreadsheet" ]
        , p [ class "text-muted" ] [ text "Right-click and choose \"Save as\". The result will be a .zip file containing an Excel file and a list of attached files, if you have any." ]
        ]


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        Refresh now ->
            case model.refreshRequest of
                Loading ->
                    ( { model | now = now }, Cmd.none )

                _ ->
                    ( { model | refreshRequest = Loading, now = now }
                    , send ExportsReceived (readExportsApiExportsBeamtimeIdGet model.beamtimeId)
                    )

        CreateNewExportDone result ->
            ( { model | createNewExportRequest = RemoteData.fromResult result }, Cmd.none )

        ToggleExportStreamFiles ->
            ( { model | exportStreamFiles = not model.exportStreamFiles }, Cmd.none )

        CreateNewExport ->
            ( { model | createNewExportRequest = Loading }
            , send CreateNewExportDone (createExportJobApiExportsBeamtimeIdPost model.beamtimeId { withStreamFiles = model.exportStreamFiles })
            )

        ExportsReceived response ->
            ( { model | exports = fromResult response, refreshRequest = NotAsked }, Cmd.none )
