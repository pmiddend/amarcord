module Amarcord.Pages.RunAnalysis exposing (Model, Msg, init, pageTitle, update, view)

import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue, convertAttributoFromApi, convertAttributoMapFromApi)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert)
import Amarcord.Chemical exposing (Chemical, ChemicalId, chemicalIdDict, convertChemicalFromApi)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Html exposing (div_, h1_, h5_, h6_, input_, p_, strongText, tbody_, td_, th_, thead_, tr_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.Route exposing (Route(..), makeLink)
import Amarcord.RunStatistics exposing (viewHitRateAndIndexingGraphs)
import Amarcord.Util exposing (lineBreakFilePath, listMinMax)
import Api.Data exposing (JsonAnalysisRun, JsonFileOutput, JsonIndexingFom, JsonIndexingStatistic, JsonReadRunAnalysis, JsonRunAnalysisIndexingResult, JsonRunFile, JsonRunId)
import Api.Request.Analysis exposing (readRunAnalysisApiRunAnalysisBeamtimeIdGet)
import Html exposing (Html, a, button, div, h4, span, table, tbody, td, text, th, thead, tr)
import Html.Attributes exposing (class, href, style, type_, value)
import Html.Events exposing (onClick, onInput)
import Html.Events.Extra exposing (onEnter)
import List.Extra
import RemoteData exposing (RemoteData(..), fromResult)


type Msg
    = RunAnalysisResultsReceived (Result HttpError JsonReadRunAnalysis)
    | ChangeRunId (Int -> Int)
    | ChangeRunIdText String
    | ConfirmRunIdText


type alias Model =
    { runIdInput : String
    , runAnalysisRequest : RemoteData HttpError JsonReadRunAnalysis
    , beamtimeId : BeamtimeId
    , runDoesntExist : Maybe Int
    }


pageTitle : String
pageTitle =
    "Run Analysis"


init : BeamtimeId -> ( Model, Cmd Msg )
init beamtimeId =
    ( { runIdInput = ""
      , runAnalysisRequest = Loading
      , beamtimeId = beamtimeId
      , runDoesntExist = Nothing
      }
    , send RunAnalysisResultsReceived (readRunAnalysisApiRunAnalysisBeamtimeIdGet beamtimeId Nothing)
    )


viewRunStatistics : JsonIndexingFom -> List JsonIndexingStatistic -> Html msg
viewRunStatistics fom originalStats =
    viewHitRateAndIndexingGraphs (Just fom) originalStats


viewRunFiles : List JsonRunFile -> Html msg
viewRunFiles files =
    case files of
        [] ->
            text ""

        _ ->
            div_
                [ h6_ [ strongText "Files" ]
                , table [ class "table table-sm table-striped" ]
                    [ thead_ [ tr_ [ th_ [ text "Source" ], th_ [ text "Glob" ] ] ]
                    , tbody_ (List.map (\{ glob, source } -> tr_ [ td_ [ text source ], td_ [ lineBreakFilePath glob ] ]) files)
                    ]
                ]


viewRunTableRow :
    BeamtimeId
    -> List (Attributo AttributoType)
    -> List (Chemical ChemicalId (AttributoMap AttributoValue) JsonFileOutput)
    -> JsonAnalysisRun
    -> Maybe JsonRunAnalysisIndexingResult
    -> Html msg
viewRunTableRow beamtimeId attributi chemicals run rarMaybe =
    let
        viewRun r =
            div_
                [ viewDataSetTable
                    attributi
                    (chemicalIdDict chemicals)
                    (convertAttributoMapFromApi r.attributi)
                    False
                    False
                    Nothing
                , viewRunFiles run.filePaths
                ]
    in
    tr_
        [ td [ style "width" "50%" ]
            [ viewRun run
            ]
        , td_
            [ case rarMaybe of
                Nothing ->
                    strongText "No indexing results."

                Just rar ->
                    h5_ [ text ("Indexing Result " ++ String.fromInt rar.indexingResultId) ]
            , case run.dataSetId of
                Nothing ->
                    text ""

                Just dataSetIdReal ->
                    a
                        [ href (makeLink (AnalysisDataSet beamtimeId dataSetIdReal))
                        ]
                        [ text ("→ Data Set " ++ String.fromInt dataSetIdReal) ]
            , case rarMaybe of
                Nothing ->
                    text ""

                Just rar ->
                    viewRunStatistics rar.foms rar.indexingStatistics
            ]
        ]


viewRunGraphs :
    BeamtimeId
    -> String
    -> List (Attributo AttributoType)
    -> List (Chemical ChemicalId (AttributoMap AttributoValue) JsonFileOutput)
    -> List JsonRunId
    -> Maybe JsonAnalysisRun
    -> List JsonRunAnalysisIndexingResult
    -> Html Msg
viewRunGraphs beamtimeId runIdInput attributi chemicals runIds run rars =
    div_
        [ case listMinMax (List.map .externalRunId runIds) of
            Nothing ->
                p_ [ strongText "No runs yet!" ]

            Just ( min, max ) ->
                p_
                    [ text "Lowest run ID: "
                    , strongText (String.fromInt min)
                    , text ", Highest run ID: "
                    , strongText (String.fromInt max)
                    ]
        , div [ class "hstack gap-3" ]
            [ button [ class "btn btn-outline-secondary", onClick (ChangeRunId (\r -> r - 1)) ] [ icon { name = "arrow-left" } ]
            , span [ class "form-text text-nowrap" ] [ text "Run ID" ]
            , input_
                [ type_ "text"
                , value runIdInput
                , class "form-control"
                , onInput ChangeRunIdText
                , onEnter ConfirmRunIdText
                ]
            , button [ class "btn btn-outline-secondary", onClick (ChangeRunId (\r -> r + 1)) ] [ icon { name = "arrow-right" } ]
            ]
        , case run of
            Nothing ->
                text ""

            Just runReal ->
                let
                    tableBody =
                        if List.isEmpty rars then
                            [ viewRunTableRow beamtimeId attributi chemicals runReal Nothing ]

                        else
                            List.map (viewRunTableRow beamtimeId attributi chemicals runReal << Just) rars
                in
                table [ class "table" ]
                    [ thead []
                        [ tr []
                            [ th [] [ text "Run Information" ]
                            , th [ style "width" "100%" ] [ text "Statistics" ]
                            ]
                        ]
                    , tbody [] tableBody
                    ]
        ]


viewInner : Model -> List (Html Msg)
viewInner model =
    [ h1_ [ text "Indexing Details" ]
    , p_ [ text "Enter a run ID to see details. Press Return to update." ]
    , case model.runDoesntExist of
        Nothing ->
            text ""

        Just _ ->
            div [ class "alert alert-warning" ] [ text "This run ID does not exist." ]
    , case model.runAnalysisRequest of
        NotAsked ->
            text ""

        Loading ->
            loadingBar "Loading analysis results..."

        Failure e ->
            makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve run analysis" ], showError e ]

        Success { attributi, chemicals, run, runIds, indexingResults } ->
            viewRunGraphs
                model.beamtimeId
                model.runIdInput
                (List.map convertAttributoFromApi attributi)
                (List.map convertChemicalFromApi chemicals)
                runIds
                run
                indexingResults
    ]


view : Model -> Html Msg
view model =
    div [ class "container" ] (viewInner model)


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        RunAnalysisResultsReceived analysisResults ->
            ( { model | runAnalysisRequest = fromResult analysisResults }, Cmd.none )

        ChangeRunId changer ->
            case model.runAnalysisRequest of
                Success { runIds } ->
                    case String.toInt model.runIdInput of
                        Nothing ->
                            ( model, Cmd.none )

                        Just realRunId ->
                            let
                                newRunId =
                                    changer realRunId
                            in
                            case List.Extra.find (\runIdIntExt -> runIdIntExt.externalRunId == newRunId) runIds of
                                Just { internalRunId } ->
                                    ( { model | runIdInput = String.fromInt newRunId }, send RunAnalysisResultsReceived (readRunAnalysisApiRunAnalysisBeamtimeIdGet model.beamtimeId (Just internalRunId)) )

                                Nothing ->
                                    ( model, Cmd.none )

                _ ->
                    ( model, Cmd.none )

        ChangeRunIdText newText ->
            ( { model | runIdInput = newText }, Cmd.none )

        ConfirmRunIdText ->
            case model.runAnalysisRequest of
                Success { runIds } ->
                    case String.toInt model.runIdInput of
                        Nothing ->
                            ( model, Cmd.none )

                        Just realRunId ->
                            case List.Extra.find (\runIdIntExt -> runIdIntExt.externalRunId == realRunId) runIds of
                                Nothing ->
                                    ( { model | runDoesntExist = Just realRunId }, Cmd.none )

                                Just { internalRunId } ->
                                    ( { model | runDoesntExist = Nothing }
                                    , send RunAnalysisResultsReceived
                                        (readRunAnalysisApiRunAnalysisBeamtimeIdGet model.beamtimeId (Just internalRunId))
                                    )

                _ ->
                    ( model, Cmd.none )
