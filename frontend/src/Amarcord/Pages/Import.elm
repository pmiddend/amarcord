module Amarcord.Pages.Import exposing (..)

import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.Attributo exposing (convertAttributoFromApi, convertAttributoMapFromApi)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, viewAlert)
import Amarcord.Chemical exposing (chemicalIdDict, convertChemicalFromApi)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Html exposing (div_, em_, h4_, h5_, li_, p_, span_, strongText, tbody_, td_, th_, thead_, tr_, ul_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.Route as Route exposing (ImportStep(..))
import Amarcord.Util exposing (HereAndNow)
import Api.Data exposing (JsonRunsBulkImportInfo, JsonRunsBulkImportOutput)
import Api.Request.Runs exposing (bulkImportApiRunBulkImportBeamtimeIdPost, bulkImportInfoApiRunBulkImportBeamtimeIdGet)
import Bytes
import File as ElmFile
import File.Select
import Html exposing (Html, a, button, dd, div, dl, dt, em, h1, h3, h4, input, label, p, small, span, strong, sup, table, td, text)
import Html.Attributes exposing (checked, class, colspan, disabled, for, href, id, type_, value)
import Html.Events exposing (onClick)
import Maybe.Extra
import RemoteData exposing (RemoteData(..))
import Task


type alias FileWithBytes =
    { fileMetadata : ElmFile.File
    , fileBytes : Bytes.Bytes
    }


type Msg
    = OpenFileSelector
    | FileSelected ElmFile.File
    | FileSelectedAndBytesRead FileWithBytes
    | FileUploadFinished (Result HttpError JsonRunsBulkImportOutput)
    | ToggleSimulate
    | ToggleCreateDataSets
    | SubmitUpload
    | ImportInfoFinished (Result HttpError JsonRunsBulkImportInfo)


type alias Model =
    { beamtimeId : BeamtimeId
    , hereAndNow : HereAndNow
    , importInfoRequest : RemoteData HttpError JsonRunsBulkImportInfo
    , uploadRequest : RemoteData HttpError JsonRunsBulkImportOutput
    , upload : Maybe FileWithBytes
    , simulate : Bool
    , createDataSets : Bool
    , step : ImportStep
    }


pageTitle : Model -> String
pageTitle _ =
    "Import"


init : HereAndNow -> BeamtimeId -> ImportStep -> ( Model, Cmd Msg )
init hereAndNow beamtimeId step =
    ( { beamtimeId = beamtimeId
      , hereAndNow = hereAndNow
      , importInfoRequest = Loading
      , uploadRequest = NotAsked
      , upload = Nothing
      , simulate = True
      , createDataSets = True
      , step = step
      }
    , send ImportInfoFinished (bulkImportInfoApiRunBulkImportBeamtimeIdGet beamtimeId)
    )


view : Model -> Html Msg
view model =
    case model.importInfoRequest of
        Success infoRequest ->
            viewInner model infoRequest

        _ ->
            text ""


viewCheck : Bool -> Html msg
viewCheck isOff =
    if isOff then
        text ""

    else
        sup []
            [ span [ class "text-bg-success rounded-pill ms-1 rounded-circle" ] [ small [] [ icon { name = "check" } ] ]
            ]


viewImportExperimentTypes : Model -> JsonRunsBulkImportInfo -> Html Msg
viewImportExperimentTypes model infoRequest =
    div_
        [ p []
            [ text "Every run needs to have exactly one experiment type associated. Thus, in order to import runs, you need to create experiment types. Currently, you have "
            , strong []
                [ text (String.fromInt (List.length infoRequest.experimentTypes))
                ]
            , text " experiment type(s):"
            ]
        , ul_ (List.map (\etName -> li_ [ text etName ]) infoRequest.experimentTypes)
        , p []
            [ text "Click "
            , a
                [ href (Route.makeLink (Route.ExperimentTypes model.beamtimeId))
                ]
                [ text "→ here" ]
            , text " to go to the experiment type overview and add more."
            ]
        ]


viewImportAttributi : Model -> JsonRunsBulkImportInfo -> Html Msg
viewImportAttributi model infoRequest =
    div_
        [ p [] [ text "In order to proceed to create experiment types, you need run attributi first. By default, runs have the attributes …" ]
        , dl []
            [ dt [] [ text "start time" ]
            , dd [] [ text "When the run was started" ]
            , dt [] [ text "stop time" ]
            , dd [] [ text "When the run was stopped (this is optional, if the run is ongoing)" ]
            , dt [] [ text "run ID" ]
            , dd [] [ text "Unique, numerical (per beamtime) ID of this run (usually starts at 1)" ]
            , dt [] [ text "experiment type" ]
            , dd [] [ text "Non-optional experiment type for this run" ]
            ]
        , p []
            [ text "However, this isn't enough to tell runs apart. You need to create more attributi yourself. Currently, you have "
            , strong []
                [ text (String.fromInt (List.length infoRequest.runAttributi))
                ]
            , text " run attributi:"
            ]
        , ul_ (List.map (\attributo -> li_ [ text attributo.name ]) infoRequest.runAttributi)
        , p []
            [ text "Click "
            , a
                [ href (Route.makeLink (Route.Attributi model.beamtimeId))
                ]
                [ text "→ here" ]
            , text " to go to the attributi overview and add more (be sure that they are "
            , em [] [ text "run" ]
            , text " attributi, not chemical attributi)."
            ]
        ]


validateFile : Maybe FileWithBytes -> Html msg
validateFile file =
    case file of
        Nothing ->
            text ""

        Just { fileMetadata } ->
            if ElmFile.mime fileMetadata /= "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" then
                div [ class "text-danger" ] [ text "The file does not appear to be an Excel spreadsheet file." ]

            else
                text ""


viewUploadResult : Model -> JsonRunsBulkImportInfo -> RemoteData HttpError JsonRunsBulkImportOutput -> Html Msg
viewUploadResult model { runAttributi, chemicals } uploadResult =
    case uploadResult of
        NotAsked ->
            text ""

        Loading ->
            text ""

        Failure e ->
            div [ class "mt-3" ]
                [ viewAlert [ AlertWarning ] <|
                    [ h4 [ class "alert-heading" ] [ text "There were errors in the spreadsheet!" ]
                    , showError e
                    ]
                ]

        Success { errors, numberOfRuns, dataSets, simulated, createDataSets } ->
            if List.isEmpty errors then
                div [ class "mt-3" ]
                    [ h5_
                        [ text
                            (if simulated then
                                "Upload will succeed!"

                             else
                                "Upload succesful!"
                            )
                        ]
                    , p_
                        [ text
                            (if simulated then
                                "We will import "

                             else
                                "We have succesfully imported "
                            )
                        , strongText (String.fromInt numberOfRuns)
                        , text " run(s)!"
                        , if simulated then
                            text ""

                          else
                            span_
                                [ text "Check "
                                , a [ href (Route.makeLink (Route.Runs model.beamtimeId [])) ] [ text "→ the runs table" ]
                                , text " to view these runs."
                                ]
                        ]
                    , if List.isEmpty dataSets then
                        if createDataSets then
                            p_ [ em [] [ text "No data sets were found to be created, although you requested to do so." ] ]

                        else
                            text ""

                      else
                        div_ <|
                            p_ [ text "The following data sets were found to be created:" ]
                                :: List.map
                                    (\ds ->
                                        viewDataSetTable
                                            (List.map convertAttributoFromApi runAttributi)
                                            model.hereAndNow.zone
                                            (chemicalIdDict (List.map convertChemicalFromApi chemicals))
                                            (convertAttributoMapFromApi ds.attributi)
                                            True
                                            True
                                            Nothing
                                    )
                                    dataSets
                    ]

            else
                div [ class "mt-3" ]
                    [ viewAlert [ AlertWarning ] <|
                        [ h4 [ class "alert-heading" ] [ text "There were errors in the spreadsheet!" ]
                        , ul_ (List.map (\e -> li_ [ text e ]) errors)
                        ]
                    ]


viewImportRuns : Model -> JsonRunsBulkImportInfo -> Html Msg
viewImportRuns model infoRequest =
    div_
        [ h4_ [ text "Description" ]
        , p []
            [ text "In order to import runs, you need to fill in a spreadsheet file. You can download a template for this "
            , a [ href (Route.makeImportSpreadsheetLink model.beamtimeId) ] [ text "here" ]
            , text " with all the columns filled in."
            ]
        , p [] [ text "The table looks like this:" ]
        , table [ class "table table-sm" ]
            [ thead_
                [ tr_
                    [ th_ [ text "run id" ]
                    , th_ [ text "experiment type" ]
                    , th_ [ text "started" ]
                    , th_ [ text "stopped" ]
                    , th_ [ text "files" ]
                    , th_ [ text "attributo 1" ]
                    , th_ [ text "..." ]
                    ]
                ]
            , tbody_
                [ tr_
                    [ td_ [ text "1" ]
                    , td_ [ text "simple ssx" ]
                    , td_ [ text "2024-12-16 15:00" ]
                    , td_ [ text "2024-12-16 15:10" ]
                    , td_ [ text "/gpfs/beamline/run1*.h5" ]
                    , td_ [ text "13" ]
                    , td_ [ text "..." ]
                    ]
                , tr_
                    [ td_ [ text "2" ]
                    , td_ [ text "simple ssx" ]
                    , td_ [ text "2024-12-16 15:20" ]
                    , td_ [ text "2024-12-16 15:35" ]
                    , td_ [ text "/gpfs/beamline/run2*.h5" ]
                    , td_ [ text "18" ]
                    , td_ [ text "..." ]
                    ]
                , tr_ [ td [ class "text-center", colspan 7 ] [ text "..." ] ]
                ]
            ]
        , p [] [ text "Let's talk about the columns you see here:" ]
        , dl []
            [ dt [] [ text "run id" ]
            , dd []
                [ text "As we talked about above, every run has a run ID. This is an "
                , em_ [ text "integer" ]
                , text " so it must be 1, 2, 1337, but "
                , em_ [ text "not" ]
                , text " “1-3” or “run1”."
                ]
            , dt [] [ text "experiment type" ]
            , dd [] [ text "Specify the run's experiment type by its name." ]
            , dt [] [ text "started/stopped" ]
            , dd [] [ text "These are start and stop timestamps for the run. You have to specify both a date and a time. If you don't really care about the run durations and timestamps, you're free to just enter a random date and time here. But it must be filled in. Sorry." ]
            , dt [] [ text "files" ]
            , dd []
                [ p_ [ text "In order to start processing your data in AMARCORD, you need to tell it where the raw images are stored. You specify this for every run separately. You specify a path to the file system where the processing jobs are run. To specify multiple files, separate them with a comma:" ]
                , p [ class "font-monospace" ] [ text "/gpfs/beamline/run1-00001.h5,/gpfs/beamline/run1-00002.h5" ]
                , p_ [ text "You can also use the ", span [ class "font-monospace" ] [ text "*" ], text " character (also called “", a [ href "https://en.wikipedia.org/wiki/Glob_(programming)" ] [ text "globbing" ], text "”) to mean arbitrary characters. So the two files above can also be specified with:" ]
                , p [ class "font-monospace" ] [ text "/gpfs/beamline/run1-*.h5" ]
                , p_ [ text "Note that this would also match a file called “run1-.h5” and of course “run1-00003.h5”." ]
                ]
            , dt [] [ text "other attributi" ]
            , dd [] [ text "The format of the other cells depends on the type of the attributo (number, string, ...)." ]
            ]
        , h4_ [ text "Upload" ]
        , div [ class "input-group mb-3" ]
            [ button [ type_ "button", class "btn btn-outline-secondary", onClick OpenFileSelector ] [ text "Choose file..." ]
            , input
                [ type_ "text"
                , disabled True
                , value (Maybe.Extra.unwrap "No file selected" (ElmFile.name << .fileMetadata) model.upload)
                , class "form-control"
                ]
                []
            ]
        , validateFile model.upload
        , div [ class "form-check form-switch" ]
            [ input
                [ class "form-check-input"
                , type_ "checkbox"
                , id "simulate"
                , checked model.simulate
                , onClick ToggleSimulate
                ]
                []
            , label [ class "form-check-label", for "simulate" ] [ text "Simulate import, don't change database" ]
            ]
        , div [ class "form-check form-switch mb-3" ]
            [ input
                [ class "form-check-input"
                , type_ "checkbox"
                , id "create-data-sets"
                , checked model.createDataSets
                , onClick ToggleCreateDataSets
                ]
                []
            , label [ class "form-check-label", for "create-data-sets" ] [ text "Create data sets automatically" ]
            ]
        , button [ class "btn btn-primary", onClick SubmitUpload, disabled (RemoteData.isLoading model.uploadRequest) ]
            [ if RemoteData.isLoading model.uploadRequest then
                span [ class "spinner-border spinner-border-sm" ] []

              else
                icon { name = "send" }
            , text " Submit"
            ]
        , viewUploadResult model infoRequest model.uploadRequest
        ]


viewInner : Model -> JsonRunsBulkImportInfo -> Html Msg
viewInner model infoRequest =
    div [ class "container" ]
        [ h1 [] [ text "Import" ]
        , p [] [ text "You can upload runs into AMARCORD from a beam time you had before. There are a few steps to take in order to achieve this:" ]
        , case model.step of
            ImportAttributi ->
                div []
                    [ h3 []
                        [ strong [] [ text "Step 1" ]
                        , text ": Add run attributi"
                        , viewCheck (List.isEmpty infoRequest.runAttributi)
                        ]
                    , viewImportAttributi model infoRequest
                    , div [ class "hstack gap-3" ]
                        [ a
                            [ class
                                ("btn btn-primary"
                                    ++ (if List.isEmpty infoRequest.runAttributi then
                                            " disabled"

                                        else
                                            ""
                                       )
                                )
                            , href (Route.makeLink (Route.Import model.beamtimeId ImportExperimentTypes))
                            ]
                            [ text "→ Next: Experiment Types" ]
                        ]
                    ]

            ImportExperimentTypes ->
                div []
                    [ h3 []
                        [ strong [] [ text "Step 2" ]
                        , text ": Add experiment types"
                        , viewCheck (List.isEmpty infoRequest.experimentTypes)
                        ]
                    , viewImportExperimentTypes model infoRequest
                    , div [ class "hstack gap-3" ]
                        [ a [ class "btn btn-secondary", href (Route.makeLink (Route.Import model.beamtimeId ImportAttributi)) ] [ text "← Previous: Attributi" ]
                        , a
                            [ class
                                ("btn btn-primary"
                                    ++ (if List.isEmpty infoRequest.experimentTypes then
                                            " disabled"

                                        else
                                            ""
                                       )
                                )
                            , href (Route.makeLink (Route.Import model.beamtimeId ImportRuns))
                            ]
                            [ text "→ Next: Runs" ]
                        ]
                    ]

            ImportRuns ->
                div []
                    [ h3 []
                        [ strong [] [ text "Step 3" ]
                        , text ": Import runs from spreadsheet"
                        ]
                    , viewImportRuns model infoRequest
                    ]
        ]


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        ToggleSimulate ->
            ( { model | simulate = not model.simulate }, Cmd.none )

        ToggleCreateDataSets ->
            ( { model | createDataSets = not model.createDataSets }, Cmd.none )

        OpenFileSelector ->
            ( model, File.Select.file [] FileSelected )

        FileSelected newFile ->
            ( model, Task.perform (FileSelectedAndBytesRead << FileWithBytes newFile) (ElmFile.toBytes newFile) )

        FileSelectedAndBytesRead fwb ->
            ( { model | upload = Just fwb }
            , Cmd.none
            )

        FileUploadFinished result ->
            ( { model | uploadRequest = RemoteData.fromResult result }, Cmd.none )

        SubmitUpload ->
            case model.upload of
                Nothing ->
                    ( model, Cmd.none )

                Just { fileMetadata } ->
                    ( { model | uploadRequest = Loading }
                    , send FileUploadFinished
                        (bulkImportApiRunBulkImportBeamtimeIdPost
                            model.beamtimeId
                            model.simulate
                            model.createDataSets
                            fileMetadata
                        )
                    )

        ImportInfoFinished result ->
            ( { model | importInfoRequest = RemoteData.fromResult result }, Cmd.none )
