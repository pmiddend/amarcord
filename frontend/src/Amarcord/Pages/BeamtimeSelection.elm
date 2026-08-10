module Amarcord.Pages.BeamtimeSelection exposing (Model, Msg, init, pageTitle, update, view)

import Amarcord.API.Requests exposing (JsonImportJobOutput, createImportJobApiExportsImportPost, invalidBeamtimeId)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, makeAlert, spinner, viewMarkdownSupportText)
import Amarcord.Html exposing (br_, code_, div_, form_, h2_, h4_, input_, p_, span_, strongText)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.MarkdownUtil exposing (markupWithoutErrors)
import Amarcord.Route exposing (Route(..), makeLink)
import Amarcord.Util exposing (HereAndNow, formatPosixDateTimeCompatible, formatPosixHumanFriendly, localDateTimeStringToPosix, scrollToTop)
import Api.Data exposing (JsonBeamtimeInput, JsonBeamtimeOutput, JsonReadBeamtime)
import Api.Request.Beamtimes exposing (createBeamtimeApiBeamtimesPost, readBeamtimesApiBeamtimesGet, updateBeamtimeApiBeamtimesPatch)
import File as ElmFile
import File.Select
import Html exposing (Html, a, button, div, h5, hr, input, label, p, span, table, tbody, td, text, textarea, th, thead, tr)
import Html.Attributes exposing (checked, class, colspan, disabled, for, href, id, placeholder, style, type_, value)
import Html.Events exposing (onClick, onInput)
import List exposing (sort)
import Maybe exposing (withDefault)
import Maybe.Extra exposing (isJust, isNothing)
import RemoteData exposing (RemoteData(..), fromResult, isLoading)
import Result.Extra as ResultExtra
import Time exposing (millisToPosix, posixToMillis, utc)


type alias ImportModel =
    { importFile : Maybe ElmFile.File
    , importFileUploadRequest : RemoteData HttpError JsonImportJobOutput
    , importChangeTitle : Maybe String
    , importChangeOutputPath : Maybe String
    }


importInit : ImportModel
importInit =
    { importFile = Nothing
    , importFileUploadRequest = NotAsked
    , importChangeTitle = Nothing
    , importChangeOutputPath = Nothing
    }


importUpdate : ImportMsg -> ImportModel -> ( ImportModel, Cmd ImportMsg )
importUpdate subMsg importModel =
    case subMsg of
        ImportChangeTitle newTitle ->
            ( { importModel | importChangeTitle = Just newTitle }, Cmd.none )

        ImportToggleChangeTitle ->
            ( { importModel
                | importChangeTitle =
                    case importModel.importChangeTitle of
                        Nothing ->
                            Just ""

                        _ ->
                            Nothing
              }
            , Cmd.none
            )

        ImportChangeOutputPath newOutputPath ->
            ( { importModel | importChangeOutputPath = Just newOutputPath }, Cmd.none )

        ImportToggleChangeOutputPath ->
            ( { importModel
                | importChangeOutputPath =
                    case importModel.importChangeOutputPath of
                        Nothing ->
                            Just ""

                        _ ->
                            Nothing
              }
            , Cmd.none
            )

        ImportFileOpenSelector ->
            ( importModel, File.Select.file [ "application/zip" ] ImportFileNewFileSelected )

        ImportFileNewFileSelected newFile ->
            ( { importModel | importFile = Just newFile }, Cmd.none )

        ImportFileUpload ->
            case importModel.importFile of
                Nothing ->
                    ( importModel, Cmd.none )

                Just importFile ->
                    ( { importModel | importFileUploadRequest = Loading }
                    , send ImportFileUploadDone (createImportJobApiExportsImportPost importFile (Maybe.withDefault "" importModel.importChangeTitle) (Maybe.withDefault "" importModel.importChangeOutputPath))
                    )

        ImportFileUploadDone result ->
            ( { importModel | importFileUploadRequest = RemoteData.fromResult result }, Cmd.none )

        ImportFileCancel ->
            ( importModel, Cmd.none )


type ImportMsg
    = ImportFileOpenSelector
    | ImportFileNewFileSelected ElmFile.File
    | ImportFileUpload
    | ImportFileUploadDone (Result HttpError JsonImportJobOutput)
    | ImportFileCancel
    | ImportToggleChangeTitle
    | ImportChangeTitle String
    | ImportToggleChangeOutputPath
    | ImportChangeOutputPath String


type BeamtimeForm
    = NoBeamtimeForm
    | BeamtimeEdit JsonBeamtimeOutput
    | BeamtimeImport ImportModel


type alias Model =
    { beamtimeResult : RemoteData HttpError (List JsonBeamtimeOutput)
    , beamtimeForm : BeamtimeForm
    , modifyRequest : RemoteData HttpError ()
    , hereAndNow : HereAndNow
    }


pageTitle : String
pageTitle =
    "Beamtime Selection"


type Msg
    = BeamtimesReceived (Result HttpError JsonReadBeamtime)
    | AddBeamtime
    | ImportBeamtime
    | Nop
    | EditBeamtimeStart JsonBeamtimeOutput
    | EditBeamtimeSubmit
    | ChangeEditBeamtime (JsonBeamtimeOutput -> JsonBeamtimeOutput)
    | EditBeamtimeCancel
    | EditBeamtimeFinished (Result HttpError {})
    | ImportSubMsg ImportMsg


init : HereAndNow -> ( Model, Cmd Msg )
init hereAndNow =
    ( { beamtimeResult = Loading
      , beamtimeForm = NoBeamtimeForm
      , modifyRequest = NotAsked
      , hereAndNow = hereAndNow
      }
    , send BeamtimesReceived readBeamtimesApiBeamtimesGet
    )


emptyBeamtime : HereAndNow -> JsonBeamtimeOutput
emptyBeamtime hereAndNow =
    { beamline = ""
    , comment = ""
    , start = posixToMillis hereAndNow.now
    , startLocal = posixToMillis hereAndNow.now
    , end = posixToMillis hereAndNow.now
    , endLocal = posixToMillis hereAndNow.now
    , externalId = ""
    , id = invalidBeamtimeId
    , proposal = ""
    , title = ""
    , chemicalNames = []
    , analysisOutputPath = "/asap3/petra3/gpfs/{beamtime.beamline_lowercase}/{beamtime.year}/data/{beamtime.external_id}/processed"
    }


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        Nop ->
            ( model, Cmd.none )

        ImportBeamtime ->
            ( { model | beamtimeForm = BeamtimeImport importInit }, Cmd.none )

        ImportSubMsg subMsg ->
            case model.beamtimeForm of
                BeamtimeImport importModel ->
                    case subMsg of
                        ImportFileCancel ->
                            ( { model | beamtimeForm = NoBeamtimeForm }, Cmd.none )

                        ImportFileUploadDone (Ok _) ->
                            let
                                ( newImportModel, importCmds ) =
                                    importUpdate subMsg importModel

                                updateBeamtimeListCmds =
                                    send BeamtimesReceived readBeamtimesApiBeamtimesGet
                            in
                            ( { model | beamtimeForm = BeamtimeImport newImportModel }
                            , Cmd.batch [ Cmd.map ImportSubMsg importCmds, updateBeamtimeListCmds ]
                            )

                        _ ->
                            let
                                ( newImportModel, importCmds ) =
                                    importUpdate subMsg importModel
                            in
                            ( { model | beamtimeForm = BeamtimeImport newImportModel }, Cmd.map ImportSubMsg importCmds )

                _ ->
                    ( model, Cmd.none )

        ChangeEditBeamtime btModifier ->
            case model.beamtimeForm of
                BeamtimeEdit bt ->
                    ( { model | beamtimeForm = BeamtimeEdit (btModifier bt) }, Cmd.none )

                _ ->
                    ( model, Cmd.none )

        BeamtimesReceived response ->
            ( { model | beamtimeResult = fromResult (Result.map .beamtimes response) }, Cmd.none )

        AddBeamtime ->
            ( { model | beamtimeForm = BeamtimeEdit (emptyBeamtime model.hereAndNow), modifyRequest = NotAsked }, Cmd.none )

        EditBeamtimeStart bt ->
            ( { model | beamtimeForm = BeamtimeEdit bt, modifyRequest = NotAsked }, scrollToTop (always Nop) )

        EditBeamtimeCancel ->
            ( { model | beamtimeForm = NoBeamtimeForm, modifyRequest = NotAsked }, Cmd.none )

        EditBeamtimeFinished result ->
            case result of
                Err e ->
                    ( { model | modifyRequest = Failure e }, Cmd.none )

                Ok _ ->
                    ( { model
                        | modifyRequest = Success ()
                        , beamtimeForm = NoBeamtimeForm
                      }
                    , send BeamtimesReceived readBeamtimesApiBeamtimesGet
                    )

        EditBeamtimeSubmit ->
            case model.beamtimeForm of
                BeamtimeEdit bt ->
                    let
                        body : JsonBeamtimeInput
                        body =
                            { beamline = bt.beamline
                            , comment = bt.comment
                            , endLocal = bt.endLocal
                            , externalId = bt.externalId
                            , id = bt.id
                            , proposal = bt.proposal
                            , startLocal = bt.startLocal
                            , title = bt.title
                            , analysisOutputPath = bt.analysisOutputPath
                            }
                    in
                    ( { model | modifyRequest = Loading }
                    , if bt.id <= 0 then
                        send (EditBeamtimeFinished << Result.map (always {})) (createBeamtimeApiBeamtimesPost body)

                      else
                        send (EditBeamtimeFinished << Result.map (always {})) (updateBeamtimeApiBeamtimesPatch body)
                    )

                _ ->
                    ( model, Cmd.none )


viewBeamtimeTableRow : JsonBeamtimeOutput -> List (Html Msg)
viewBeamtimeTableRow ({ beamline, comment, startLocal, endLocal, externalId, id, proposal, title, chemicalNames } as bt) =
    let
        firstRow =
            tr []
                [ td []
                    [ button
                        [ class "btn btn-link amarcord-small-link-button", class "amarcord-edit-button", onClick (EditBeamtimeStart bt) ]
                        [ icon { name = "pencil-square" } ]
                    ]
                , td [] [ strongText externalId ]
                , td [] [ text (String.fromInt id) ]
                , td [] [ a [ href (makeLink (RunOverview id)) ] [ text title ] ]
                , td [] [ text beamline ]
                , td [ class "text-nowrap" ] [ text proposal ]
                , td [] [ text (formatPosixHumanFriendly utc (millisToPosix startLocal)) ]
                , td [] [ text (formatPosixHumanFriendly utc (millisToPosix endLocal)) ]
                ]

        secondRow =
            [ tr []
                [ td [ colspan 8 ]
                    [ p []
                        ((if comment /= "" then
                            [ p [] [ strongText "Comment: ", markupWithoutErrors comment ] ]

                          else
                            []
                         )
                            ++ [ strongText "Chemicals: "
                               , span_
                                    (List.map
                                        (\chemicalName ->
                                            span [ class "badge text-bg-light me-2" ] [ text chemicalName ]
                                        )
                                        (sort chemicalNames)
                                    )
                               ]
                        )
                    ]
                ]
            ]
    in
    firstRow :: secondRow


viewBeamtimes : List JsonBeamtimeOutput -> Html Msg
viewBeamtimes beamtimes =
    table [ class "table table-striped amarcord-table-fix-head", id "beamtime-table" ]
        [ thead []
            [ tr []
                [ th [] [ text "Actions" ]
                , th [ class "text-nowrap" ] [ text "External ID" ]
                , th [] [ text "ID" ]
                , th [] [ text "Title" ]
                , th [] [ text "Beamline" ]
                , th [] [ text "Proposal" ]
                , th [] [ text "Start" ]
                , th [] [ text "End" ]
                ]
            ]
        , tbody [] (List.concatMap viewBeamtimeTableRow beamtimes)
        ]


viewEditForm : JsonBeamtimeOutput -> Html Msg
viewEditForm bt =
    let
        addOrEditHeadline =
            h4_
                [ icon { name = "plus-lg" }
                , text
                    (if bt.id == invalidBeamtimeId then
                        " Add new beamtime"

                     else
                        " Edit “" ++ bt.title ++ "”"
                    )
                ]
    in
    form_
        [ hr [] []
        , addOrEditHeadline
        , div [ class "form-floating mb-3" ]
            [ input [ id "beamtime-edit-title", type_ "text", class "form-control", value bt.title, onInput (\newValue -> ChangeEditBeamtime (\bt2 -> { bt2 | title = newValue })) ] []
            , label [ for "beamtime-edit-title" ] [ text "Title" ]
            , div [ class "form-text" ] [ text "This will appear in the title bar of the beamtime." ]
            ]
        , div [ class "form-floating mb-3" ]
            [ input [ id "beamtime-edit-external-id", type_ "text", class "form-control", value bt.externalId, onInput (\newValue -> ChangeEditBeamtime (\bt2 -> { bt2 | externalId = newValue })) ] []
            , label [ for "beamtime-edit-external-id" ] [ text "External ID" ]
            , div [ class "form-text" ] [ text "For Petra beamlines, this is an identifier typically looking like \"1101xxxx\" for some digits in \"xxxx\"." ]
            ]
        , div [ class "form-floating mb-3" ]
            [ input [ id "beamtime-edit-proposal", type_ "text", class "form-control", value bt.proposal, onInput (\newValue -> ChangeEditBeamtime (\bt2 -> { bt2 | proposal = newValue })) ] []
            , label [ for "beamtime-edit-proposal" ] [ text "Proposal" ]
            , div [ class "form-text" ] [ text "This will be facility specific, could be \"BAG\" or a proposal ID." ]
            ]
        , div [ class "form-floating mb-3" ]
            [ input [ id "beamtime-edit-beamline", type_ "text", class "form-control", value bt.beamline, onInput (\newValue -> ChangeEditBeamtime (\bt2 -> { bt2 | beamline = newValue })) ] []
            , label [ for "beamtime-edit-beamline" ] [ text "Beamline" ]
            , div [ class "form-text" ] [ text "The beamline used. For example P11 or P09." ]
            ]
        , div [ class "form-floating mb-3" ]
            [ input
                [ id "beamtime-edit-start"
                , type_ "datetime-local"
                , class "form-control"

                -- note here and below: local time zone!
                , value (formatPosixDateTimeCompatible utc (millisToPosix bt.startLocal))
                , onInput
                    (\newValue ->
                        ChangeEditBeamtime
                            (\bt2 ->
                                ResultExtra.unwrap
                                    bt2
                                    (\newParsed -> { bt2 | startLocal = posixToMillis newParsed })
                                    (localDateTimeStringToPosix utc newValue)
                            )
                    )
                ]
                []
            , label [ for "beamtime-edit-start" ] [ text "Start" ]
            ]
        , div [ class "form-floating mb-3" ]
            [ input
                [ id "beamtime-edit-end"
                , type_ "datetime-local"
                , class "form-control"

                -- note here and below: local time zone!
                , value (formatPosixDateTimeCompatible utc (millisToPosix bt.endLocal))
                , onInput
                    (\newValue ->
                        ChangeEditBeamtime
                            (\bt2 ->
                                ResultExtra.unwrap
                                    bt2
                                    (\newParsed -> { bt2 | endLocal = posixToMillis newParsed })
                                    (localDateTimeStringToPosix utc newValue)
                            )
                    )
                ]
                []
            , label [ for "beamtime-edit-end" ] [ text "End" ]
            ]
        , div [ class "form-floating mb-3" ]
            [ input
                [ id "beamtime-edit-analysis-output-path"
                , type_ "text"
                , class "form-control"
                , value bt.analysisOutputPath
                , onInput (\newValue -> ChangeEditBeamtime (\bt2 -> { bt2 | analysisOutputPath = newValue }))
                ]
                []
            , label [ for "beamtime-edit-analysis-output-path" ] [ text "Analysis output path" ]
            , div [ class "form-text" ]
                [ p_ [ text "This will be facility specific and can include placeholders. For Petra III, choose " ]
                , p_ [ text "/asap3/petra3/gpfs/{beamtime.beamline_lowercase}/{beamtime.year}/data/{beamtime.external_id}/processed" ]
                , p_ [ text "For external beam times copied to DESY's GPFS, choose:" ]
                , p_ [ text "/asap3/petra3/gpfs/external/{beamtime.year}/data/{beamtime.external_id}/processed" ]
                , p_ [ text "And choose the external beamtime ID accordingly (or don't use the placeholder for the external ID and simply write the actual number, as you wish)." ]
                ]
            ]
        , div [ class "form-floating mb-3" ]
            [ textarea
                [ id "beamtime-edit-comment"
                , class "form-control"
                , value bt.comment
                , onInput (\newValue -> ChangeEditBeamtime (\bt2 -> { bt2 | comment = newValue }))
                , style "height" "5em"
                ]
                []
            , label [ for "beamtime-edit-comment" ] [ text "Comment" ]
            , viewMarkdownSupportText
            ]
        , button
            [ class "btn btn-primary me-3 mb-3"
            , onClick EditBeamtimeSubmit
            , type_ "button"
            ]
            [ icon { name = "plus-lg" }
            , text
                (if bt.id == invalidBeamtimeId then
                    " Save"

                 else
                    " Confirm edit"
                )
            ]
        , button
            [ class "btn btn-secondary me-3 mb-3"
            , onClick EditBeamtimeCancel
            , type_ "button"
            ]
            [ icon { name = "x-lg" }, text " Cancel" ]
        ]


viewImport : ImportModel -> Html ImportMsg
viewImport model =
    form_
        [ hr [] []
        , h4_ [ icon { name = "upload" }, text " Import beamtime" ]
        , p [ class "text-muted" ] [ text "Specify a .zip file here that you previously downloaded from an AMARCORD export." ]
        , div [ class "input-group mb-3" ]
            [ div [ class "input-group-text" ]
                [ input_
                    [ class "form-check-input me-1"
                    , id "import-change-name"
                    , type_ "checkbox"
                    , value ""
                    , checked (isJust model.importChangeTitle)
                    , onInput (always ImportToggleChangeTitle)
                    ]
                , label [ for "import-change-name" ] [ text " Use alternative title for beamtime" ]
                ]
            , input_
                [ class "form-control"
                , type_ "text"
                , placeholder "New beamtime title"
                , value (withDefault "" model.importChangeTitle)
                , disabled (isNothing model.importChangeTitle)
                , onInput ImportChangeTitle
                ]
            ]
        , p [ class "form-text" ]
            [ text "If you want to import a beamtime and later trigger analysis jobs here, you usually do not want to use the output path from the original (exported) beamtime. It might not even exist, e.g. if you take data at ESRF, which might be stored under "
            , code_ [ text "/data/..." ]
            , text " and then import them to a DESY file system."
            , br_
            , text "The checkbox below gives you the ability to import the beamtime and change the analysis path directly to something you like. If you leave the checkbox in the checked state, the directory specified for imports will be used. This is the same directory where the imported "
            , code_ [ text ".stream" ]
            , text " files will also be placed in."
            ]
        , div [ class "input-group mb-3" ]
            [ div [ class "input-group-text" ]
                [ input_
                    [ class "form-check-input me-1"
                    , id "import-change-output-path"
                    , type_ "checkbox"
                    , value ""
                    , checked (isNothing model.importChangeOutputPath)
                    , onInput (always ImportToggleChangeOutputPath)
                    ]
                , label [ for "import-change-output-path" ] [ text " Use import path as analysis output path" ]
                ]
            , input_
                [ class "form-control"
                , type_ "text"
                , placeholder "New analysis output path"
                , value (withDefault "" model.importChangeOutputPath)
                , disabled (isNothing model.importChangeOutputPath)
                , onInput ImportChangeOutputPath
                ]
            ]
        , div [ class "input-group mb-3" ]
            [ button
                [ type_ "button"
                , class "btn btn-outline-secondary"
                , onClick ImportFileOpenSelector
                ]
                [ text "Choose zip file..." ]
            , case model.importFile of
                Nothing ->
                    span [ class "input-group-text" ] [ text "No file selected yet." ]

                Just importFileJust ->
                    span [ class "input-group-text" ] [ code_ [ text (ElmFile.name importFileJust) ] ]
            ]
        , div [ class "hstack gap-3" ]
            [ button
                ([ type_ "button"
                 , class "btn btn-primary"
                 , onClick ImportFileUpload
                 ]
                    ++ (if isLoading model.importFileUploadRequest then
                            [ disabled True ]

                        else
                            []
                       )
                )
                (if isLoading model.importFileUploadRequest then
                    [ spinner True, text " Importing (be patient!)" ]

                 else
                    [ icon { name = "send" }, text " Start import" ]
                )
            , button
                ([ class "btn btn-secondary"
                 , onClick ImportFileCancel
                 , type_ "button"
                 ]
                    ++ (if isLoading model.importFileUploadRequest then
                            [ disabled True ]

                        else
                            []
                       )
                )
                [ icon { name = "x-lg" }, text " Cancel" ]
            ]
        , case model.importFileUploadRequest of
            Success _ ->
                div [ class "badge text-bg-success mb-3" ] [ text "Import successful!" ]

            Failure e ->
                div [ class "mt-3" ] [ makeAlert [ AlertDanger ] [ h5 [] [ text "Import failed!" ], showError e ] ]

            _ ->
                text ""
        ]


view : Model -> Html Msg
view model =
    div [ class "container" ]
        [ h2_ [ icon { name = "arrow-left-right" }, text " Beamtimes" ]
        , let
            viewButtons =
                div [ class "hstack gap-3" ]
                    [ button
                        [ class "btn btn-primary", onClick AddBeamtime, id "add-beamtime-button" ]
                        [ icon { name = "plus-lg" }, text " Add Beamtime" ]
                    , button
                        [ class "btn btn-primary", onClick ImportBeamtime, id "import-beamtime-button" ]
                        [ icon { name = "upload" }, text " Import Beamtime" ]
                    ]
          in
          case model.beamtimeForm of
            BeamtimeEdit beamtime ->
                div_
                    [ viewEditForm beamtime
                    ]

            BeamtimeImport importData ->
                case importData.importFileUploadRequest of
                    Success _ ->
                        div [ class "mb-3" ]
                            [ div [ class "badge text-bg-success mb-3" ] [ text "Import successful!" ]
                            , viewButtons
                            ]

                    _ ->
                        Html.map ImportSubMsg (viewImport importData)

            NoBeamtimeForm ->
                viewButtons
        , hr [] []
        , case model.modifyRequest of
            NotAsked ->
                text ""

            Loading ->
                p [] [ text "Request in progress..." ]

            Failure e ->
                div [] [ makeAlert [ AlertDanger ] [ showError e ] ]

            Success _ ->
                div [ class "mt-3", id "beamtime-edit-success-alert" ]
                    [ makeAlert [ AlertSuccess ] [ text "Beam time edited successfully!" ]
                    ]
        , case model.beamtimeResult of
            Success beamtimes ->
                viewBeamtimes beamtimes

            Failure e ->
                div [] [ makeAlert [ AlertDanger ] [ showError e ] ]

            _ ->
                text "Loading..."
        ]
