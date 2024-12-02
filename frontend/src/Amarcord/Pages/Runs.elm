module Amarcord.Pages.Runs exposing (Model, Msg(..), init, pageTitle, subscriptions, update, view)

import Amarcord.API.ExperimentType exposing (experimentTypeIdDict)
import Amarcord.API.Requests exposing (BeamtimeId, RunEventDate(..), RunEventDateFilter, RunFilter, RunInternalId(..), emptyRunEventDateFilter, emptyRunFilter, runEventDateFilter, runEventDateToString, runFilterToString, specificRunEventDateFilter)
import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo exposing (Attributo, AttributoType(..), ChemicalNameDict, convertAttributoFromApi, convertAttributoMapFromApi)
import Amarcord.AttributoHtml exposing (makeAttributoHeader, viewAttributoCell, viewRunExperimentTypeCell)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert, mimeTypeToIcon)
import Amarcord.Chemical exposing (chemicalIdDict, convertChemicalFromApi)
import Amarcord.ColumnChooser as ColumnChooser
import Amarcord.Html exposing (input_, li_, p_, strongText, tbody_, td_, th_, thead_, tr_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.LocalStorage exposing (LocalStorage)
import Amarcord.MarkdownUtil exposing (markupWithoutErrors)
import Amarcord.Route exposing (makeFilesLink)
import Amarcord.RunAttributiForm as RunAttributiForm
import Amarcord.Util exposing (HereAndNow, formatPosixTimeOfDayHumanFriendly, posixBefore)
import Api.Data exposing (JsonCreateDataSetFromRunOutput, JsonDeleteEventOutput, JsonEvent, JsonFileOutput, JsonReadRuns, JsonRun, JsonUpdateRunOutput)
import Api.Request.Events exposing (deleteEventApiEventsDelete)
import Api.Request.Runs exposing (readRunsApiRunsBeamtimeIdGet)
import Date
import Dict exposing (Dict)
import Html exposing (Html, a, button, div, h4, label, span, table, td, text, tr, ul)
import Html.Attributes exposing (checked, class, colspan, for, href, id, style, type_)
import Html.Events exposing (onClick)
import List exposing (head)
import Maybe
import RemoteData exposing (RemoteData(..), fromResult, isSuccess)
import String
import Time exposing (Posix, Zone, millisToPosix)


type Msg
    = RunsReceived (Result HttpError JsonReadRuns)
    | Refresh Posix
    | EventDelete Int
    | EventDeleteFinished (Result HttpError JsonDeleteEventOutput)
    | RunInitiateEdit JsonRun
    | ColumnChooserMessage ColumnChooser.Msg
    | RunAttributiFormMsg RunAttributiForm.Msg
    | ResetDate
    | SetRunDateFilter RunEventDate


pageTitle : Model -> String
pageTitle _ =
    "Run Table"


type alias RunFilterInfo =
    { runFilter : RunFilter
    , nextRunFilter : String
    , runFilterRequest : RemoteData HttpError JsonReadRuns
    }


type alias RunDateFilterInfo =
    { runDateFilter : RunEventDateFilter }


initRunFilter : RunFilterInfo
initRunFilter =
    { runFilter = emptyRunFilter
    , nextRunFilter = ""
    , runFilterRequest = NotAsked
    }


initRunDateFilter : RunDateFilterInfo
initRunDateFilter =
    { runDateFilter = emptyRunEventDateFilter
    }



-- viewRunFilter : RunFilterInfo -> Html RunFilterMsg
-- viewRunFilter model =
--     form_
--         [ h5_ [ text "Run filter" ]
--         , div [ class "input-group mb-3" ]
--             [ input_
--                 [ type_ "text"
--                 , class "form-control"
--                 , value model.nextRunFilter
--                 , onInput RunFilterEdit
--                 , disabled (isLoading model.runFilterRequest)
--                 , onEnter RunFilterSubmit
--                 ]
--             , button
--                 [ class "btn btn-outline-secondary"
--                 , onClick RunFilterReset
--                 , type_ "button"
--                 ]
--                 [ text "Reset" ]
--             , button
--                 [ class "btn btn-secondary"
--                 , disabled (isLoading model.runFilterRequest)
--                 , type_ "button"
--                 , onClick RunFilterSubmit
--                 ]
--                 [ icon { name = "save" }, text " Update" ]
--             ]
--         , case model.runFilterRequest of
--             Failure e ->
--                 div [ class "mb-3" ] [ div_ [ makeAlert [ AlertDanger ] [ showError e ] ] ]
--             _ ->
--                 text ""
--         ]


type alias Model =
    { runs : RemoteData HttpError JsonReadRuns
    , zone : Zone
    , refreshRequest : RemoteData HttpError ()
    , now : Posix
    , runDateFilter : RunDateFilterInfo
    , runDates : List RunEventDate
    , runEditInfo : Maybe RunAttributiForm.Model
    , runEditRequest : RemoteData HttpError JsonUpdateRunOutput
    , runFilter : RunFilterInfo
    , submitErrors : List String
    , columnChooser : ColumnChooser.Model
    , localStorage : Maybe LocalStorage
    , dataSetFromRunRequest : RemoteData HttpError JsonCreateDataSetFromRunOutput
    , beamtimeId : BeamtimeId
    }


init : HereAndNow -> Maybe LocalStorage -> BeamtimeId -> ( Model, Cmd Msg )
init { zone, now } localStorage beamtimeId =
    ( { runs = Loading
      , zone = zone
      , refreshRequest = NotAsked
      , now = now
      , runDates = []
      , runDateFilter = initRunDateFilter
      , runEditInfo = Nothing
      , runEditRequest = NotAsked
      , runFilter = initRunFilter
      , submitErrors = []
      , columnChooser = ColumnChooser.init localStorage []
      , localStorage = localStorage
      , dataSetFromRunRequest = NotAsked
      , beamtimeId = beamtimeId
      }
    , send RunsReceived
        (readRunsApiRunsBeamtimeIdGet
            beamtimeId
            (Maybe.map runEventDateToString <| runEventDateFilter emptyRunEventDateFilter)
            (Just <| runFilterToString emptyRunFilter)
        )
    )


subscriptions : Model -> List (Sub Msg)
subscriptions model =
    [ ColumnChooser.subscriptions model.columnChooser ColumnChooserMessage, Time.every 10000 Refresh ]


attributiColumnHeaders : List (Attributo AttributoType) -> List (Html msg)
attributiColumnHeaders =
    List.map (th_ << makeAttributoHeader)


attributiColumns : Zone -> ChemicalNameDict -> Dict Int String -> List (Attributo AttributoType) -> JsonRun -> List (Html Msg)
attributiColumns zone chemicalIds experimentTypeIds attributi run =
    let
        viewCell : Attributo AttributoType -> Maybe (Html Msg)
        viewCell a =
            if a.associatedTable == AssociatedTable.Run then
                Just <|
                    if a.name == virtualExperimentTypeAttributoName then
                        viewRunExperimentTypeCell (Maybe.withDefault "unknown experiment type" <| Dict.get run.experimentTypeId experimentTypeIds)

                    else
                        viewAttributoCell
                            { shortDateTime = True
                            , colorize = True
                            , withUnit = False
                            , withTolerance = False
                            }
                            zone
                            chemicalIds
                            (convertAttributoMapFromApi run.attributi)
                            a

            else
                Nothing
    in
    List.filterMap viewCell attributi


viewRunRow :
    Zone
    -> ChemicalNameDict
    -> Dict Int String
    -> List (Attributo AttributoType)
    -> Maybe RunAttributiForm.Model
    -> Int
    -> JsonRun
    -> List (Html Msg)
viewRunRow zone chemicalIds experimentTypeIds attributi runEditInfo attributoColumnCount r =
    tr [ style "white-space" "nowrap" ]
        (td_
            [ button
                [ class "btn btn-link amarcord-small-link-button"
                , onClick (RunInitiateEdit r)
                ]
                [ icon { name = "pencil-square" } ]
            ]
            :: td_ [ strongText (String.fromInt r.externalId) ]
            :: td_ [ text (String.fromInt r.id) ]
            :: td_ [ text <| formatPosixTimeOfDayHumanFriendly zone (millisToPosix r.started) ]
            :: td_ [ text <| Maybe.withDefault "" <| Maybe.map (formatPosixTimeOfDayHumanFriendly zone) (Maybe.map millisToPosix r.stopped) ]
            :: attributiColumns zone chemicalIds experimentTypeIds attributi r
        )
        :: (case runEditInfo of
                Nothing ->
                    []

                Just rei ->
                    if rei.runId == RunInternalId r.id then
                        [ tr_
                            [ td
                                [ colspan attributoColumnCount ]
                                [ Html.map RunAttributiFormMsg <| RunAttributiForm.view rei ]
                            ]
                        ]

                    else
                        []
           )


viewEventRow : Zone -> Int -> JsonEvent -> Html Msg
viewEventRow zone attributoColumnCount e =
    let
        viewFile : JsonFileOutput -> Html Msg
        viewFile { type__, fileName, id } =
            li_
                [ mimeTypeToIcon type__
                , text " "
                , a [ href (makeFilesLink id Nothing) ] [ text fileName ]
                ]

        maybeFiles =
            if List.isEmpty e.files then
                [ text "" ]

            else
                [ ul [ class "me-0" ] (List.map viewFile e.files) ]

        mainContent =
            [ button [ class "btn btn-sm btn-link amarcord-small-link-button", type_ "button", onClick (EventDelete e.id) ] [ icon { name = "trash" } ]
            , strongText <| " " ++ e.source ++ " "
            , markupWithoutErrors e.text
            ]
                ++ maybeFiles
    in
    tr [ class "bg-light" ]
        [ td_ []
        , td_ [ text <| formatPosixTimeOfDayHumanFriendly zone (millisToPosix e.created) ]
        , td [ colspan attributoColumnCount ] mainContent
        ]


viewRunAndEventRows :
    Zone
    -> ChemicalNameDict
    -> Dict Int String
    -> List (Attributo AttributoType)
    -> Int
    -> Maybe RunAttributiForm.Model
    -> List JsonRun
    -> List JsonEvent
    -> List (Html Msg)
viewRunAndEventRows zone chemicalIds experimentTypeIds attributi attributoColumnCount runEditInfo runs events =
    case ( head runs, head events ) of
        -- No elements, neither runs nor events, left anymore
        ( Nothing, Nothing ) ->
            []

        -- Only runs left
        ( Just _, Nothing ) ->
            List.concatMap (viewRunRow zone chemicalIds experimentTypeIds attributi runEditInfo attributoColumnCount) runs

        -- Only events left
        ( Nothing, Just _ ) ->
            List.map (viewEventRow zone (List.length attributi)) events

        -- We have events and runs and have to compare the dates
        ( Just run, Just event ) ->
            if posixBefore (millisToPosix event.created) (millisToPosix run.started) then
                viewRunRow
                    zone
                    chemicalIds
                    experimentTypeIds
                    attributi
                    runEditInfo
                    attributoColumnCount
                    run
                    ++ viewRunAndEventRows zone
                        chemicalIds
                        experimentTypeIds
                        attributi
                        attributoColumnCount
                        runEditInfo
                        (List.drop 1 runs)
                        events

            else
                viewEventRow zone (List.length attributi) event
                    :: viewRunAndEventRows zone
                        chemicalIds
                        experimentTypeIds
                        attributi
                        attributoColumnCount
                        runEditInfo
                        runs
                        (List.drop 1 events)


viewRunsTable :
    Zone
    -> List (Attributo AttributoType)
    -> Maybe RunAttributiForm.Model
    -> JsonReadRuns
    -> Html Msg
viewRunsTable zone chosenColumns runEditInfo { runs, events, chemicals, experimentTypes } =
    let
        runRows : List (Html Msg)
        runRows =
            viewRunAndEventRows zone
                (chemicalIdDict <| List.map convertChemicalFromApi chemicals)
                (experimentTypeIdDict experimentTypes)
                chosenColumns
                -- Why + 5? It's the chosen columns plus ID, Internal ID, started, stopped and actions, as you can see below
                (List.length chosenColumns + 5)
                runEditInfo
                runs
                events
    in
    table [ class "table amarcord-table-fix-head table-bordered table-hover" ]
        [ thead_
            [ tr [ class "align-top" ] <|
                th_ [ text "Actions" ]
                    :: th_ [ text "ID" ]
                    :: th_ [ text "Internal ID" ]
                    :: th_ [ text "Started" ]
                    :: th_ [ text "Stopped" ]
                    :: attributiColumnHeaders chosenColumns
            ]
        , tbody_ runRows
        ]


viewInner : Model -> JsonReadRuns -> List (Html Msg)
viewInner model rrc =
    [ div [ class "container" ]
        [ div [ class "row" ]
            [ div [ class "col-6" ] (runDatesGroup model)
            , div [ class "col-6" ] [ Html.map ColumnChooserMessage (ColumnChooser.view model.columnChooser) ]
            ]
        ]
    , div [ class "container-fluid" ]
        [ p_ [ span [ class "text-info" ] [ text "Colored columns" ], text " belong to manually entered attributi." ]
        , viewRunsTable
            model.zone
            (ColumnChooser.resolveChosen model.columnChooser)
            model.runEditInfo
            rrc
        ]
    ]


runDatesGroup : Model -> List (Html Msg)
runDatesGroup model =
    if List.isEmpty model.runDates then
        []

    else
        [ div [ class "btn-group" ] (dateFilterButtons model) ]


runDateFilterIsNothing : RunEventDateFilter -> Bool
runDateFilterIsNothing rdf =
    case runEventDateFilter rdf of
        Nothing ->
            True

        Maybe.Just _ ->
            False


dateEqualsDateInFilter : RunEventDate -> RunEventDateFilter -> Bool
dateEqualsDateInFilter rd rdf =
    case runEventDateFilter rdf of
        Nothing ->
            False

        Maybe.Just a ->
            a == rd


dateFilterButtons : Model -> List (Html Msg)
dateFilterButtons model =
    List.concat
        [ [ input_
                [ type_ "radio"
                , class "btn-check"
                , id "all_dates"
                , checked
                    (runDateFilterIsNothing model.runDateFilter.runDateFilter)
                , onClick ResetDate
                ]
          , label [ class "btn btn-outline-primary", for "all_dates" ] [ text "All dates" ]
          ]
        , List.concatMap (dateRadioOption model) model.runDates
        ]


dateRadioOption : Model -> RunEventDate -> List (Html Msg)
dateRadioOption model runEventDate =
    [ input_ [ type_ "radio", class "btn-check", id ("filter" ++ runEventDateToString runEventDate), checked (dateEqualsDateInFilter runEventDate model.runDateFilter.runDateFilter), onClick (SetRunDateFilter runEventDate) ]
    , label [ class "btn btn-outline-primary", for ("filter" ++ runEventDateToString runEventDate) ] [ text (formattedOrEmptyDate runEventDate) ]
    ]


formattedOrEmptyDate : RunEventDate -> String
formattedOrEmptyDate runEventDate =
    let
        date =
            runEventDateToString runEventDate
    in
    case Date.fromIsoString date of
        Ok v ->
            Date.format "EE, d MMM y" v

        Err _ ->
            "Unreadable date <" ++ date ++ ">"


view : Model -> Html Msg
view model =
    div [ class "container-fluid" ] <|
        case model.runs of
            NotAsked ->
                List.singleton <| text "Impossible state reached: time zone, but no runs in progress?"

            Loading ->
                List.singleton <| loadingBar "Loading runs..."

            Failure e ->
                List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve runs" ], showError e ]

            Success a ->
                viewInner model a


virtualExperimentTypeAttributoName : String
virtualExperimentTypeAttributoName =
    "experiment_type"


updateColumnChooser : Maybe LocalStorage -> ColumnChooser.Model -> RemoteData HttpError JsonReadRuns -> Result HttpError JsonReadRuns -> ColumnChooser.Model
updateColumnChooser localStorage ccm currentRuns runsResponse =
    case ( currentRuns, runsResponse ) of
        ( _, Ok { attributi } ) ->
            -- Inject "virtual" attributo experiment type here
            let
                experimentTypeAttributo =
                    { id = -1
                    , name = virtualExperimentTypeAttributoName
                    , description = "Experiment Type"
                    , group = "manual"
                    , associatedTable = AssociatedTable.Run
                    , type_ = String
                    }
            in
            ColumnChooser.updateAttributi ccm (List.map convertAttributoFromApi attributi ++ [ experimentTypeAttributo ])

        ( _, Err _ ) ->
            ColumnChooser.init localStorage []


extractRunDates : Result HttpError JsonReadRuns -> List RunEventDate
extractRunDates runDates =
    case runDates of
        Err _ ->
            []

        Ok { filterDates } ->
            List.map RunEventDate filterDates


updateRunDateFilter : RunDateFilterInfo -> RunEventDate -> RunDateFilterInfo
updateRunDateFilter runDateFilterInfo runDate =
    { runDateFilterInfo | runDateFilter = specificRunEventDateFilter runDate }


retrieveRuns : Model -> Cmd Msg
retrieveRuns model =
    send RunsReceived
        (readRunsApiRunsBeamtimeIdGet model.beamtimeId
            (Maybe.map runEventDateToString <| runEventDateFilter <| model.runDateFilter.runDateFilter)
            (Just <| runFilterToString model.runFilter.runFilter)
        )


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        Refresh now ->
            case ( model.refreshRequest, model.runs ) of
                ( Loading, _ ) ->
                    ( model, Cmd.none )

                ( _, Loading ) ->
                    ( model, Cmd.none )

                _ ->
                    ( { model | refreshRequest = Loading, now = now }
                    , retrieveRuns model
                    )

        ResetDate ->
            let
                newModel =
                    { model
                        | runDateFilter = initRunDateFilter
                    }
            in
            ( newModel
            , retrieveRuns newModel
            )

        SetRunDateFilter runDate ->
            let
                newRunDateFilter =
                    updateRunDateFilter model.runDateFilter runDate

                newModel =
                    { model | runDateFilter = newRunDateFilter }
            in
            ( newModel
            , retrieveRuns newModel
            )

        RunsReceived response ->
            ( { model
                | runs = fromResult response
                , runDates = extractRunDates response
                , columnChooser = updateColumnChooser model.localStorage model.columnChooser model.runs response
                , refreshRequest =
                    if isSuccess model.runs then
                        Success ()

                    else
                        model.refreshRequest
              }
            , Cmd.none
            )

        EventDelete eventId ->
            ( model, send EventDeleteFinished (deleteEventApiEventsDelete { id = eventId }) )

        EventDeleteFinished result ->
            case result of
                Ok _ ->
                    ( model, retrieveRuns model )

                _ ->
                    ( model, Cmd.none )

        RunInitiateEdit run ->
            case model.runs of
                Success { attributi, chemicals, experimentTypes } ->
                    let
                        ( editInfo, editInfoCmd ) =
                            RunAttributiForm.init
                                { zone = model.zone
                                , attributi = attributi
                                , chemicals = List.map convertChemicalFromApi chemicals
                                , experimentTypes = experimentTypes
                                }
                                run
                    in
                    ( { model | runEditInfo = Just editInfo }
                    , Cmd.map RunAttributiFormMsg editInfoCmd
                    )

                _ ->
                    ( model, Cmd.none )

        ColumnChooserMessage columnChooserMessage ->
            let
                ( newColumnChooser, cmds ) =
                    ColumnChooser.update model.columnChooser columnChooserMessage
            in
            ( { model | columnChooser = newColumnChooser }, Cmd.map ColumnChooserMessage cmds )

        RunAttributiFormMsg subMsg ->
            case model.runEditInfo of
                Nothing ->
                    ( model, Cmd.none )

                Just runEditInfo ->
                    case subMsg of
                        RunAttributiForm.Cancel ->
                            ( { model | runEditInfo = Nothing }, Cmd.none )

                        _ ->
                            let
                                ( newEditInfo, subCmd ) =
                                    RunAttributiForm.update subMsg runEditInfo

                                cmd =
                                    case subMsg of
                                        RunAttributiForm.SubmitFinished _ ->
                                            Cmd.batch [ Cmd.map RunAttributiFormMsg subCmd, retrieveRuns model ]

                                        _ ->
                                            Cmd.map RunAttributiFormMsg subCmd
                            in
                            ( { model | runEditInfo = Just newEditInfo }
                            , cmd
                            )
