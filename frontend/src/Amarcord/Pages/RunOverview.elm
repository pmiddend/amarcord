module Amarcord.Pages.RunOverview exposing (Model, Msg(..), init, update, view)

import Amarcord.API.Requests exposing (Event, RequestError, Run, RunsResponse, RunsResponseContent, httpCreateEvent, httpDeleteEvent, httpGetRuns, httpUpdateRun)
import Amarcord.API.RequestsHtml exposing (showRequestError)
import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue, attributoFrames, attributoHits, attributoStarted, attributoStopped, extractDateTime, retrieveAttributoValue, retrieveDateTimeAttributoValue, retrieveIntAttributoValue)
import Amarcord.AttributoHtml exposing (AttributoNameWithValueUpdate, EditableAttributiAndOriginal, convertEditValues, createEditableAttributi, editEditableAttributi, formatFloatHumanFriendly, makeAttributoHeader, resetEditableAttributo, unsavedAttributoChanges, viewAttributoCell, viewAttributoForm)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert, spinner)
import Amarcord.Constants exposing (manualAttributiGroup)
import Amarcord.DataSet exposing (DataSet, DataSetSummary)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Html exposing (br_, em_, form_, h1_, h2_, h3_, h5_, hr_, input_, li_, p_, span_, strongText, tbody_, td_, th_, thead_, tr_)
import Amarcord.Sample exposing (Sample, sampleIdDict)
import Amarcord.Util exposing (HereAndNow, formatPosixTimeOfDayHumanFriendly, millisDiffHumanFriendly, posixBefore, posixDiffHumanFriendly, posixDiffMillis, scrollToTop)
import Dict exposing (Dict)
import Hotkeys exposing (onEnter)
import Html exposing (Html, a, button, div, form, h2, h4, label, option, p, select, span, table, td, text, tfoot, tr, ul)
import Html.Attributes exposing (autocomplete, checked, class, colspan, disabled, for, id, placeholder, selected, style, type_, value)
import Html.Events exposing (onClick, onInput)
import List exposing (head)
import List.Extra exposing (find)
import Maybe
import Maybe.Extra as MaybeExtra exposing (isNothing)
import RemoteData exposing (RemoteData(..), fromResult, isLoading, isSuccess)
import Set exposing (Set)
import String exposing (fromInt)
import Time exposing (Posix, Zone)
import Tuple exposing (first, second)


type ColumnChooserMessage
    = ColumnChooserSubmit
    | ColumnChooserToggleColumn String Bool
    | ColumnChooserToggle


type alias ColumnChooserModel =
    { allColumns : Dict String ( Attributo AttributoType, Int )
    , chosenColumns : Set String
    , editingColumns : Set String
    , open : Bool
    }


attributiDictFromList : List (Attributo AttributoType) -> Dict String ( Attributo AttributoType, Int )
attributiDictFromList attributi =
    List.foldl (\( a, i ) prior -> Dict.insert a.name ( a, i ) prior) Dict.empty (List.indexedMap (\i a -> ( a, i )) attributi)


initColumnChooser : List (Attributo AttributoType) -> ColumnChooserModel
initColumnChooser allColumnsList =
    { allColumns = attributiDictFromList allColumnsList
    , chosenColumns = List.foldl (\a prior -> Set.insert a.name prior) Set.empty allColumnsList
    , editingColumns = Set.empty
    , open = False
    }


columnChooserResolveChosen : ColumnChooserModel -> List (Attributo AttributoType)
columnChooserResolveChosen { chosenColumns, allColumns } =
    List.map first <| List.sortBy second <| List.foldl (\c prior -> MaybeExtra.unwrap [] List.singleton (Dict.get c allColumns) ++ prior) [] (Set.toList chosenColumns)


columnChooserHiddenColumnCount : ColumnChooserModel -> Int
columnChooserHiddenColumnCount { allColumns, chosenColumns } =
    Dict.size allColumns - Set.size chosenColumns


viewColumnChooser : ColumnChooserModel -> Html ColumnChooserMessage
viewColumnChooser columnChooser =
    let
        viewAttributoButton ( attributoName, attributo ) =
            let
                isChecked =
                    Set.member attributoName columnChooser.editingColumns
            in
            [ input_
                [ type_ "checkbox"
                , class "btn-check"
                , id ("column-" ++ attributoName)
                , autocomplete False
                , checked isChecked
                , onInput (always (ColumnChooserToggleColumn attributoName (not isChecked)))
                ]
            , label
                [ class
                    ("btn "
                        ++ (if isChecked then
                                "btn-outline-success"

                            else
                                "btn-outline-secondary"
                           )
                    )
                , for ("column-" ++ attributoName)
                ]
                [ text attributoName ]
            ]

        hiddenColumnCount =
            columnChooserHiddenColumnCount columnChooser
    in
    div [ class "accordion" ]
        [ div [ class "accordion-item" ]
            [ h2 [ class "accordion-header" ]
                [ button [ class "accordion-button", type_ "button", onClick ColumnChooserToggle ]
                    [ icon { name = "columns" }
                    , span [ class "ms-1" ]
                        [ text <|
                            " Choose columns"
                                ++ (if hiddenColumnCount > 0 then
                                        " (" ++ String.fromInt hiddenColumnCount ++ " hidden)"

                                    else
                                        ""
                                   )
                        ]
                    ]
                ]
            , div
                [ class
                    ("accordion-collapse collapse"
                        ++ (if columnChooser.open then
                                " show"

                            else
                                ""
                           )
                    )
                ]
                [ div [ class "accordion-body" ]
                    [ p [ class "lead" ] [ text "Click to enable/disable columns, then press \"Confirm\"." ]
                    , div [ class "btn-group-vertical mb-3" ] (List.concatMap viewAttributoButton (Dict.toList columnChooser.allColumns))
                    , p_
                        [ button [ class "btn btn-primary me-2", type_ "button", onClick ColumnChooserSubmit ] [ icon { name = "save" }, text " Confirm" ]
                        , button [ class "btn btn-secondary", type_ "button", onClick ColumnChooserToggle ] [ icon { name = "x-lg" }, text " Cancel" ]
                        ]
                    ]
                ]
            ]
        ]


type Msg
    = RunsReceived RunsResponse
    | Refresh Posix
    | EventFormChange EventForm
    | EventFormSubmit
    | EventFormSubmitFinished (Result RequestError ())
    | EventDelete Int
    | EventDeleteFinished (Result RequestError ())
    | EventFormSubmitDismiss
    | RunEditInfoValueUpdate AttributoNameWithValueUpdate
    | RunEditSubmit
    | RunEditFinished (Result RequestError ())
    | RunInitiateEdit Run
    | RunEditCancel
    | Nop
    | CurrentExperimentTypeChanged String
    | ColumnChooserMessage ColumnChooserMessage


type alias EventForm =
    { userName : String
    , message : String
    }


emptyEventForm : EventForm
emptyEventForm =
    EventForm "P11User" ""


eventFormValid : EventForm -> Bool
eventFormValid { userName, message } =
    userName /= "" && message /= ""


type alias RunEditInfo =
    { runId : Int
    , editableAttributi : EditableAttributiAndOriginal

    -- This is to handle a tricky case: usually we want to stay with the latest run so we can quickly change settings.
    -- If we manually click on an older run to edit it, we don't want to then jump to the latest one.
    , initiatedManually : Bool
    }


type alias Model =
    { runs : RemoteData RequestError RunsResponseContent
    , myTimeZone : Zone
    , refreshRequest : RemoteData RequestError ()
    , eventForm : EventForm
    , eventRequest : RemoteData RequestError ()
    , now : Posix
    , runEditInfo : Maybe RunEditInfo
    , runEditRequest : RemoteData RequestError ()
    , submitErrors : List String
    , currentExperimentType : Maybe String
    , columnChooser : ColumnChooserModel
    }


init : HereAndNow -> ( Model, Cmd Msg )
init { zone, now } =
    ( { runs = Loading
      , myTimeZone = zone
      , refreshRequest = NotAsked
      , eventForm = emptyEventForm
      , eventRequest = NotAsked
      , now = now
      , runEditInfo = Nothing
      , runEditRequest = NotAsked
      , submitErrors = []
      , currentExperimentType = Nothing
      , columnChooser = initColumnChooser []
      }
    , httpGetRuns RunsReceived
    )


attributiColumnHeaders : List (Attributo AttributoType) -> List (Html msg)
attributiColumnHeaders =
    List.map (th_ << makeAttributoHeader)


attributiColumns : Zone -> Dict Int String -> List (Attributo AttributoType) -> Run -> List (Html Msg)
attributiColumns zone sampleIds attributi run =
    List.map (viewAttributoCell { shortDateTime = True } zone sampleIds run.attributi) <| List.filter (\a -> a.associatedTable == AssociatedTable.Run) attributi


viewRunRow : Zone -> Dict Int String -> List (Attributo AttributoType) -> Run -> Html Msg
viewRunRow zone sampleIds attributi r =
    tr_ <|
        td_ [ text (fromInt r.id) ]
            :: attributiColumns zone sampleIds attributi r
            ++ [ td_
                    [ button
                        [ class "btn btn-link amarcord-small-link-button"
                        , onClick (RunInitiateEdit r)
                        ]
                        [ icon { name = "pencil-square" } ]
                    ]
               ]


viewEventRow : Zone -> Int -> Event -> Html Msg
viewEventRow zone attributoColumnCount e =
    tr [ class "bg-light" ]
        [ td_ []
        , td_ [ text <| formatPosixTimeOfDayHumanFriendly zone e.created ]
        , td [ colspan attributoColumnCount ]
            [ button [ class "btn btn-sm btn-link amarcord-small-link-button", type_ "button", onClick (EventDelete e.id) ] [ icon { name = "trash" } ]
            , strongText <| " [" ++ e.source ++ "] "
            , text <| e.text ++ " "
            ]
        ]


viewRunAndEventRows : Zone -> Dict Int String -> List (Attributo AttributoType) -> List Run -> List Event -> List (Html Msg)
viewRunAndEventRows zone sampleIds attributi runs events =
    case ( head runs, head events ) of
        -- No elements, neither runs nor events, left anymore
        ( Nothing, Nothing ) ->
            []

        -- Only runs left
        ( Just _, Nothing ) ->
            List.map (viewRunRow zone sampleIds attributi) runs

        -- Only events left
        ( Nothing, Just _ ) ->
            List.map (viewEventRow zone (List.length attributi)) events

        -- We have events and runs and have to compare the dates
        ( Just run, Just event ) ->
            case Maybe.andThen extractDateTime <| retrieveAttributoValue attributoStarted run.attributi of
                Just runStarted ->
                    if posixBefore event.created runStarted then
                        viewRunRow zone sampleIds attributi run :: viewRunAndEventRows zone sampleIds attributi (List.drop 1 runs) events

                    else
                        viewEventRow zone (List.length attributi) event :: viewRunAndEventRows zone sampleIds attributi runs (List.drop 1 events)

                -- We don't have a start time...take the run
                Nothing ->
                    viewRunRow zone sampleIds attributi run :: viewRunAndEventRows zone sampleIds attributi (List.drop 1 runs) events


viewRunsTable : Zone -> List (Attributo AttributoType) -> RunsResponseContent -> Html Msg
viewRunsTable zone chosenColumns { runs, attributi, events, samples } =
    let
        runRows : List (Html Msg)
        runRows =
            viewRunAndEventRows zone (sampleIdDict samples) chosenColumns runs events
    in
    table [ class "table amarcord-table-fix-head table-bordered table-hover" ]
        [ thead_
            [ tr [ class "align-top" ] <|
                th_ [ text "ID" ]
                    :: attributiColumnHeaders chosenColumns
                    ++ [ th_ [ text "Actions" ] ]
            ]
        , tbody_ runRows
        ]


viewEventForm : RemoteData RequestError () -> EventForm -> Html Msg
viewEventForm eventRequest { userName, message } =
    let
        eventError =
            case eventRequest of
                Success _ ->
                    p [ class "text-success" ] [ text "Message added!" ]

                Failure e ->
                    makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to add message!" ] ] ++ [ showRequestError e ]

                _ ->
                    text ""
    in
    form_
        [ h5_ [ text "Did something happen just now? Tell us!" ]
        , div
            [ class "input-group mb-3" ]
            [ input_
                [ value userName
                , type_ "text"
                , class "form-control form-control-sm"
                , style "width" "15%"
                , placeholder "User name"
                , onInput (\e -> EventFormChange { userName = e, message = message })
                ]
            , input_
                [ value message
                , type_ "text"
                , class "form-control form-control-sm"
                , placeholder "What happened? What did you do?"
                , style "width" "70%"
                , onEnter EventFormSubmit
                , onInput (\e -> EventFormChange { userName = userName, message = e })
                ]
            , button
                [ onClick EventFormSubmit
                , disabled (isLoading eventRequest || userName == "" || message == "")
                , type_ "button"
                , class "btn btn-primary btn-sm"
                , style "width" "15%"
                , style "white-space" "nowrap"
                ]
                [ icon { name = "send" }, text " Post" ]
            ]
        , eventError
        ]


calculateEtaMillis : Int -> Int -> Int -> Int -> Maybe Int
calculateEtaMillis totalHits framesInRun hitsInRun runLengthMillis =
    if runLengthMillis == 0 || framesInRun == 0 then
        Nothing

    else
        let
            framesPerSecond =
                toFloat framesInRun / toFloat runLengthMillis * 1000.0

            hitRate =
                toFloat hitsInRun / toFloat framesInRun

            hitsPerSecond =
                framesPerSecond * hitRate

            targetHits =
                max 0 <| 10000 - totalHits

            secondsNeeded =
                toFloat targetHits / hitsPerSecond
        in
        Just <| round <| secondsNeeded * 1000.0


viewCurrentRun : Zone -> Posix -> Maybe String -> RunsResponseContent -> List (Html Msg)
viewCurrentRun zone now currentExperimentType rrc =
    -- Here, we assume runs are ordered so the first one is the latest one.
    case head rrc.runs of
        Nothing ->
            List.singleton <| text ""

        Just { id, attributi, dataSets } ->
            let
                runStarted : Maybe Posix
                runStarted =
                    retrieveDateTimeAttributoValue attributoStarted attributi

                runStopped : Maybe Posix
                runStopped =
                    retrieveDateTimeAttributoValue attributoStopped attributi

                stoppedOrNow : Posix
                stoppedOrNow =
                    Maybe.withDefault now runStopped

                runLengthMillis : Maybe Int
                runLengthMillis =
                    Maybe.map (posixDiffMillis stoppedOrNow) runStarted

                runFrames : Maybe Int
                runFrames =
                    retrieveIntAttributoValue attributoFrames attributi

                runHits : Maybe Int
                runHits =
                    retrieveIntAttributoValue attributoHits attributi

                isRunning =
                    isNothing runStopped

                header =
                    case ( runStarted, runStopped ) of
                        ( Just started, Nothing ) ->
                            [ h1_
                                [ span [ class "text-success" ] [ spinner ]
                                , text <| " Run " ++ fromInt id
                                ]
                            , p [ class "lead text-success" ] [ text <| "Running for " ++ posixDiffHumanFriendly now started ]
                            ]

                        ( Just started, Just stopped ) ->
                            [ h1_ [ text <| " Run " ++ fromInt id ]
                            , p [ class "lead" ] [ text <| "Stopped, " ++ posixDiffHumanFriendly stopped now ++ " ago (duration " ++ posixDiffHumanFriendly started stopped ++ ")" ]
                            ]

                        _ ->
                            []

                viewExperimentTypeOption experimentType =
                    option [ selected (Just experimentType == currentExperimentType), value experimentType ] [ text experimentType ]

                dataSetSelection =
                    [ form_
                        [ div [ class "mb-3" ]
                            [ label [ for "current-experiment-type" ] [ text "Experiment Type" ]
                            , select
                                [ class "form-select"
                                , Html.Attributes.id "current-experiment-type"
                                , onInput CurrentExperimentTypeChanged
                                ]
                                (option [ selected (isNothing currentExperimentType) ] [ text "«no value»" ] :: List.map viewExperimentTypeOption rrc.experimentTypes)
                            ]
                        ]
                    ]

                currentRunDataSet : Maybe DataSet
                currentRunDataSet =
                    case currentExperimentType of
                        Nothing ->
                            Nothing

                        Just experimentType ->
                            find (\ds -> ds.experimentType == experimentType && List.member ds.id dataSets) rrc.dataSets

                dataSetInformation =
                    case currentRunDataSet of
                        Nothing ->
                            [ h3_ [ text "Not part of any data set" ] ]

                        Just ds ->
                            let
                                footer : DataSetSummary -> Html msg
                                footer { numberOfRuns, hits, frames } =
                                    let
                                        eta : Maybe Int
                                        eta =
                                            MaybeExtra.join <| Maybe.map3 (calculateEtaMillis hits) runFrames runHits runLengthMillis

                                        etaWarning =
                                            MaybeExtra.unwrap "bg-success"
                                                (\realEta ->
                                                    if realEta > 12 * 60 * 60 * 1000 then
                                                        "bg-warning"

                                                    else
                                                        "bg-success"
                                                )
                                                eta

                                        etaDisplay =
                                            MaybeExtra.unwrap []
                                                (\realEta ->
                                                    if realEta > 0 then
                                                        [ span_ [ text "Time until 10k hits:" ]
                                                        , br_
                                                        , em_ [ text <| millisDiffHumanFriendly realEta ]
                                                        ]

                                                    else
                                                        []
                                                )
                                                eta

                                        toGoalPercent =
                                            floor <| toFloat hits * 100.0 / 10000.0

                                        overshootPercent =
                                            floor <| 100.0 / toFloat toGoalPercent * 100.0

                                        progressBar =
                                            if toGoalPercent <= 100 then
                                                div [ class "progress" ]
                                                    [ div
                                                        [ class
                                                            ("progress-bar progress-bar-striped "
                                                                ++ etaWarning
                                                                ++ (if isRunning then
                                                                        " progress-bar-animated"

                                                                    else
                                                                        ""
                                                                   )
                                                            )
                                                        , style "width" (String.fromInt toGoalPercent ++ "%")
                                                        ]
                                                        []
                                                    ]

                                            else
                                                div [ class "progress" ]
                                                    [ div
                                                        [ class "progress-bar progress-bar-striped bg-success"
                                                        , style "width" (String.fromInt overshootPercent ++ "%")
                                                        ]
                                                        []
                                                    , div
                                                        [ class "progress-bar progress-bar-striped"
                                                        , style "width" (String.fromInt (100 - overshootPercent) ++ "%")
                                                        ]
                                                        []
                                                    ]
                                    in
                                    tfoot []
                                        [ tr_ [ td_ [ text "Runs" ], td_ [ text (String.fromInt numberOfRuns) ] ]
                                        , tr_ [ td_ [ text "Frames" ], td_ [ text (String.fromInt frames) ] ]
                                        , tr_
                                            [ td_ [ text "Hits" ]
                                            , td_ <|
                                                [ text (String.fromInt hits)
                                                , progressBar
                                                ]
                                                    ++ etaDisplay
                                            ]
                                        , tr_
                                            [ td_ [ text "Hit Rate" ]
                                            , td_
                                                [ text
                                                    (if frames /= 0 then
                                                        formatFloatHumanFriendly (toFloat hits / toFloat frames * 100.0) ++ "%"

                                                     else
                                                        ""
                                                    )
                                                ]
                                            ]
                                        ]
                            in
                            [ h3_ [ text "Data set" ]
                            , viewDataSetTable rrc.attributi
                                zone
                                (sampleIdDict rrc.samples)
                                ds
                                True
                                (Maybe.map footer ds.summary)
                            ]
            in
            header ++ dataSetSelection ++ dataSetInformation


viewRunAttributiForm : Maybe Run -> List String -> RemoteData RequestError () -> List (Sample Int a b) -> Maybe RunEditInfo -> List (Html Msg)
viewRunAttributiForm latestRun submitErrorsList runEditRequest samples rei =
    case rei of
        Nothing ->
            []

        Just { runId, editableAttributi } ->
            let
                filteredAttributi =
                    List.filter (\a -> a.associatedTable == AssociatedTable.Run && a.group == manualAttributiGroup && not (List.member a.name [ "started", "stopped" ])) editableAttributi.editableAttributi

                submitErrors =
                    case submitErrorsList of
                        [] ->
                            [ text "" ]

                        errors ->
                            [ p_ [ strongText "There were submission errors:" ]
                            , ul [ class "text-danger" ] <| List.map (\e -> li_ [ text e ]) errors
                            ]

                submitSuccess =
                    if isSuccess runEditRequest then
                        [ p [ class "text-success" ] [ text "Saved!" ] ]

                    else
                        []

                buttons =
                    [ button
                        [ class "btn btn-secondary me-2"
                        , disabled (isLoading runEditRequest || not (unsavedAttributoChanges editableAttributi.editableAttributi))
                        , type_ "button"
                        , onClick RunEditSubmit
                        ]
                        [ icon { name = "save" }, text " Save changes" ]
                    , button
                        [ class "btn btn-outline-secondary"
                        , type_ "button"
                        , onClick RunEditCancel
                        ]
                        [ icon { name = "x-lg" }, text " Cancel" ]
                    ]

                isLatestRun =
                    Just runId == Maybe.map .id latestRun
            in
            h2_
                [ text <|
                    if isLatestRun then
                        "Run Info"

                    else
                        "Edit Run " ++ fromInt runId
                ]
                :: [ if not isLatestRun then
                        p [ class "text-warning" ] [ text "You are currently editing an older run!" ]

                     else
                        text ""
                   , form [ class "mb-3" ]
                        (List.map (Html.map RunEditInfoValueUpdate << viewAttributoForm samples) filteredAttributi ++ submitErrors ++ submitSuccess ++ buttons)
                   ]


viewInner : Model -> RunsResponseContent -> List (Html Msg)
viewInner model rrc =
    [ div
        [ class "row" ]
        [ div [ class "col-6" ]
            (viewCurrentRun model.myTimeZone model.now model.currentExperimentType rrc ++ [ viewEventForm model.eventRequest model.eventForm ])
        , div [ class "col-6" ] (viewRunAttributiForm (head rrc.runs) model.submitErrors model.runEditRequest rrc.samples model.runEditInfo)
        ]
    , hr_
    , Html.map ColumnChooserMessage (viewColumnChooser model.columnChooser)
    , hr_
    , div [ class "row" ] [ viewRunsTable model.myTimeZone (columnChooserResolveChosen model.columnChooser) rrc ]
    ]


view : Model -> Html Msg
view model =
    div [ class "container" ] <|
        case model.runs of
            NotAsked ->
                List.singleton <| text "Impossible state reached: time zone, but no runs in progress?"

            Loading ->
                List.singleton <| loadingBar "Loading runs..."

            Failure e ->
                List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve runs" ] ] ++ [ showRequestError e ]

            Success a ->
                case model.refreshRequest of
                    Loading ->
                        viewInner model a ++ [ loadingBar "Refreshing..." ]

                    _ ->
                        viewInner model a


updateRunEditInfoFromContent : Zone -> Maybe RunEditInfo -> RunsResponseContent -> Maybe RunEditInfo
updateRunEditInfoFromContent zone runEditInfoRaw { runs, attributi } =
    case runEditInfoRaw of
        -- We have no previous edit info
        Nothing ->
            case head runs of
                -- We have no edit info, and no runs
                Nothing ->
                    Nothing

                -- We have no edit info, and now we have a run
                Just latestRun ->
                    Just { runId = latestRun.id, editableAttributi = createEditableAttributi zone attributi latestRun.attributi, initiatedManually = False }

        -- We have previous edit info
        Just runEditInfo ->
            -- We have unsaved changes to the previous run
            -- OR we have a manually edited run
            if runEditInfo.initiatedManually || unsavedAttributoChanges runEditInfo.editableAttributi.editableAttributi then
                Just runEditInfo

            else
                -- We have no unsaved changes and a run
                case head runs of
                    -- This case cannot really occur
                    Nothing ->
                        Nothing

                    Just latestRun ->
                        Just { runId = latestRun.id, editableAttributi = createEditableAttributi zone attributi latestRun.attributi, initiatedManually = False }


updateRunEditInfo : Zone -> Maybe RunEditInfo -> RunsResponse -> Maybe RunEditInfo
updateRunEditInfo zone runEditInfoRaw responseRaw =
    case responseRaw of
        Ok content ->
            updateRunEditInfoFromContent zone runEditInfoRaw content

        Err _ ->
            runEditInfoRaw


updateColumnChooser : ColumnChooserModel -> RemoteData RequestError RunsResponseContent -> RunsResponse -> ColumnChooserModel
updateColumnChooser ccm currentRuns runsResponse =
    case ( currentRuns, runsResponse ) of
        ( Success lastResponseUnpacked, Ok currentResponseUnpacked ) ->
            let
                lastAttributi =
                    List.filter (\a -> a.associatedTable == AssociatedTable.Run) lastResponseUnpacked.attributi

                currentAttributi =
                    List.filter (\a -> a.associatedTable == AssociatedTable.Run) currentResponseUnpacked.attributi

                currentAttributiNames =
                    Set.fromList <| List.map .name currentAttributi

                lastAttributiNames =
                    Set.fromList <| List.map .name lastAttributi

                newAttributiNames =
                    Set.diff currentAttributiNames lastAttributiNames

                deletedAttributiNames =
                    Set.diff lastAttributiNames currentAttributiNames
            in
            { allColumns = attributiDictFromList currentAttributi
            , chosenColumns = Set.union newAttributiNames (Set.diff ccm.chosenColumns deletedAttributiNames)
            , editingColumns = Set.diff ccm.editingColumns deletedAttributiNames
            , open = ccm.open
            }

        ( _, Ok currentResponseUnpacked ) ->
            { allColumns = attributiDictFromList <| List.filter (\a -> a.associatedTable == AssociatedTable.Run) currentResponseUnpacked.attributi
            , chosenColumns =
                List.foldl
                    (\a prior ->
                        if a.associatedTable == AssociatedTable.Run then
                            Set.insert a.name prior

                        else
                            prior
                    )
                    Set.empty
                    currentResponseUnpacked.attributi
            , editingColumns = Set.empty
            , open = False
            }

        ( _, Err _ ) ->
            { allColumns = Dict.empty
            , chosenColumns = Set.empty
            , editingColumns = Set.empty
            , open = False
            }


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        RunsReceived response ->
            ( { model
                | runs = fromResult response
                , runEditInfo = updateRunEditInfo model.myTimeZone model.runEditInfo response
                , columnChooser = updateColumnChooser model.columnChooser model.runs response
                , refreshRequest =
                    if isSuccess model.runs then
                        Success ()

                    else
                        model.refreshRequest
                , currentExperimentType =
                    case ( model.currentExperimentType, response ) of
                        -- If we currently don't have an experiment type, then the only reason is that we
                        -- didn't receive any yet. If we did now, set the experiment type.
                        ( Nothing, Ok { experimentTypes } ) ->
                            List.head experimentTypes

                        -- No experiment types, but an error in the request
                        ( Nothing, _ ) ->
                            Nothing

                        -- We have an experiment type and need to check it
                        ( Just currentExperimentType, Ok { experimentTypes } ) ->
                            -- Could be that the experiment type disappeared!
                            if List.member currentExperimentType experimentTypes then
                                Just currentExperimentType

                            else
                                List.head experimentTypes

                        -- We have an experiment type, but an error now. Just keep it for now.
                        ( Just currentExperimentType, _ ) ->
                            Just currentExperimentType
              }
            , Cmd.none
            )

        Refresh now ->
            ( { model | refreshRequest = Loading, now = now }, httpGetRuns RunsReceived )

        EventFormChange eventForm ->
            ( { model | eventForm = eventForm }, Cmd.none )

        EventFormSubmit ->
            if eventFormValid model.eventForm then
                ( { model | eventRequest = Loading }, httpCreateEvent EventFormSubmitFinished model.eventForm.userName model.eventForm.message )

            else
                ( model, Cmd.none )

        EventFormSubmitFinished result ->
            case result of
                Err e ->
                    ( { model | eventRequest = Failure e }, Cmd.none )

                Ok _ ->
                    ( { model | eventRequest = Success (), eventForm = { userName = model.eventForm.userName, message = "" } }, httpGetRuns RunsReceived )

        EventDelete eventId ->
            ( model, httpDeleteEvent EventDeleteFinished eventId )

        EventDeleteFinished result ->
            case result of
                Ok _ ->
                    ( model, httpGetRuns RunsReceived )

                _ ->
                    ( model, Cmd.none )

        EventFormSubmitDismiss ->
            ( { model | eventRequest = NotAsked }, Cmd.none )

        RunEditInfoValueUpdate v ->
            case model.runEditInfo of
                -- This is the unlikely case that we have an "attributo was edited" message, but sample is edited
                Nothing ->
                    ( model, Cmd.none )

                Just oldRunEditInfo ->
                    let
                        newEditable =
                            editEditableAttributi oldRunEditInfo.editableAttributi.editableAttributi v

                        newRunEditInfo =
                            { oldRunEditInfo | editableAttributi = { editableAttributi = newEditable, originalAttributi = oldRunEditInfo.editableAttributi.originalAttributi } }
                    in
                    ( { model | runEditInfo = Just newRunEditInfo }, Cmd.none )

        RunEditSubmit ->
            case model.runEditInfo of
                Nothing ->
                    ( model, Cmd.none )

                Just runEditInfo ->
                    case convertEditValues runEditInfo.editableAttributi of
                        Err errorList ->
                            ( { model | submitErrors = List.map (\( name, errorMessage ) -> name ++ ": " ++ errorMessage) errorList }, Cmd.none )

                        Ok editedAttributi ->
                            let
                                run =
                                    { id = runEditInfo.runId
                                    , attributi = editedAttributi
                                    , files = []
                                    , dataSets = []
                                    }
                            in
                            ( { model | runEditRequest = Loading }, httpUpdateRun RunEditFinished run )

        RunEditFinished result ->
            case result of
                Err e ->
                    ( { model | runEditRequest = Failure e }, Cmd.none )

                Ok _ ->
                    case model.runs of
                        Success _ ->
                            let
                                resetEditedFlags : RunEditInfo -> RunEditInfo
                                resetEditedFlags rei =
                                    { runId = rei.runId
                                    , editableAttributi =
                                        { originalAttributi = rei.editableAttributi.originalAttributi
                                        , editableAttributi = List.map resetEditableAttributo rei.editableAttributi.editableAttributi
                                        }

                                    -- Reset manual edit flag, so we automatically jump to the latest run again
                                    , initiatedManually = False
                                    }
                            in
                            ( { model | runEditRequest = Success (), submitErrors = [], runEditInfo = Maybe.map resetEditedFlags model.runEditInfo }, httpGetRuns RunsReceived )

                        _ ->
                            -- Super unlikely case, we don't really have a successful runs request, after finishing editing a run?
                            ( { model | runEditRequest = Success (), submitErrors = [], runEditInfo = Nothing }, httpGetRuns RunsReceived )

        RunInitiateEdit run ->
            case model.runs of
                Success rrc ->
                    ( { model
                        | runEditInfo =
                            Just
                                { runId = run.id
                                , editableAttributi = createEditableAttributi model.myTimeZone rrc.attributi run.attributi
                                , initiatedManually = True
                                }
                      }
                    , scrollToTop (always Nop)
                    )

                _ ->
                    ( model, Cmd.none )

        RunEditCancel ->
            case model.runs of
                Success response ->
                    ( { model | runEditInfo = updateRunEditInfoFromContent model.myTimeZone Nothing response, submitErrors = [], runEditRequest = NotAsked }, Cmd.none )

                _ ->
                    ( { model | runEditInfo = Nothing }, Cmd.none )

        Nop ->
            ( model, Cmd.none )

        CurrentExperimentTypeChanged string ->
            ( { model
                | currentExperimentType =
                    if string == "" then
                        Nothing

                    else
                        Just string
              }
            , Cmd.none
            )

        ColumnChooserMessage columnChooserMessage ->
            case columnChooserMessage of
                ColumnChooserSubmit ->
                    let
                        newColumnChooser =
                            { allColumns = model.columnChooser.allColumns
                            , editingColumns = Set.empty
                            , chosenColumns = model.columnChooser.editingColumns
                            , open = False
                            }
                    in
                    ( { model | columnChooser = newColumnChooser }, Cmd.none )

                ColumnChooserToggleColumn attributo turnOn ->
                    let
                        newColumnChooser =
                            { allColumns = model.columnChooser.allColumns
                            , editingColumns =
                                if turnOn then
                                    Set.insert attributo model.columnChooser.editingColumns

                                else
                                    Set.remove attributo model.columnChooser.editingColumns
                            , chosenColumns = model.columnChooser.chosenColumns
                            , open = True
                            }
                    in
                    ( { model | columnChooser = newColumnChooser }, Cmd.none )

                ColumnChooserToggle ->
                    if model.columnChooser.open then
                        let
                            newColumnChooser =
                                { allColumns = model.columnChooser.allColumns
                                , editingColumns = Set.empty
                                , chosenColumns = model.columnChooser.chosenColumns
                                , open = False
                                }
                        in
                        ( { model | columnChooser = newColumnChooser }, Cmd.none )

                    else
                        let
                            newColumnChooser =
                                { allColumns = model.columnChooser.allColumns
                                , editingColumns = model.columnChooser.chosenColumns
                                , chosenColumns = model.columnChooser.chosenColumns
                                , open = True
                                }
                        in
                        ( { model | columnChooser = newColumnChooser }, Cmd.none )
