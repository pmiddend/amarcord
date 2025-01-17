-- Certain tools (elm2nix, for example) assume that Main is directly below src/, so it's here.
-- In case you're wondering...


module Main exposing (main)

import Amarcord.Bootstrap exposing (viewRemoteDataHttp)
import Amarcord.Html exposing (h1_, img_)
import Amarcord.HttpError exposing (HttpError, send)
import Amarcord.LocalStorage exposing (LocalStorage, decodeLocalStorage)
import Amarcord.Menu exposing (viewMenu)
import Amarcord.Pages.AdvancedControls as AdvancedControls
import Amarcord.Pages.AnalysisOverview as AnalysisOverview
import Amarcord.Pages.Attributi as Attributi
import Amarcord.Pages.BeamtimeSelection as BeamtimeSelection
import Amarcord.Pages.Chemicals as Chemicals
import Amarcord.Pages.DataSets as DataSets
import Amarcord.Pages.EventLog as EventLog
import Amarcord.Pages.ExperimentTypes as ExperimentTypes
import Amarcord.Pages.Help as Help
import Amarcord.Pages.Import as Import
import Amarcord.Pages.MergeResult as MergeResult
import Amarcord.Pages.RunAnalysis as RunAnalysis
import Amarcord.Pages.RunOverview as RunOverview
import Amarcord.Pages.Runs as Runs
import Amarcord.Pages.Schedule as Schedule
import Amarcord.Pages.SingleDataSet as SingleDataSet
import Amarcord.Route as Route exposing (Route)
import Amarcord.Util exposing (HereAndNow, retrieveHereAndNow)
import Amarcord.Version exposing (version)
import Api.Data exposing (JsonBeamtime)
import Api.Request.Beamtimes exposing (readBeamtimeApiBeamtimesBeamtimeIdGet)
import Browser exposing (Document, UrlRequest)
import Browser.Navigation as Nav
import Html exposing (..)
import Html.Attributes exposing (..)
import RemoteData exposing (RemoteData(..))
import String exposing (contains, endsWith)
import Task
import Url as URL exposing (Url)


pageSubscriptions : Model -> List (Sub Msg)
pageSubscriptions rootModel =
    case rootModel.page of
        RunsPage model ->
            List.map (Sub.map RunsPageMsg) (Runs.subscriptions model)

        SchedulePage model ->
            List.map (Sub.map ScheduleMsg) (Schedule.subscriptions model)

        AdvancedControlsPage model ->
            List.map (Sub.map AdvancedControlsPageMsg) (AdvancedControls.subscriptions model)

        RunOverviewPage model ->
            List.map (Sub.map RunOverviewPageMsg) (RunOverview.subscriptions model)

        SingleDataSetPage model ->
            List.map (Sub.map SingleDataSetPageMsg) (SingleDataSet.subscriptions model)

        AnalysisOverviewPage model ->
            List.map (Sub.map AnalysisOverviewPageMsg) (AnalysisOverview.subscriptions model)

        EventLogPage model ->
            List.map (Sub.map EventLogPageMsg) (EventLog.subscriptions model)

        _ ->
            []


main : Program (Maybe String) Model Msg
main =
    Browser.application
        { init = init
        , view = view
        , update = update
        , subscriptions = Sub.batch << pageSubscriptions
        , onUrlRequest = LinkClicked
        , onUrlChange = UrlChanged
        }


type Msg
    = AttributiPageMsg Attributi.Msg
    | ChemicalsPageMsg Chemicals.Msg
    | MergeResultPageMsg MergeResult.Msg
    | RunOverviewPageMsg RunOverview.Msg
    | ImportPageMsg Import.Msg
    | RunsPageMsg Runs.Msg
    | AdvancedControlsPageMsg AdvancedControls.Msg
    | BeamtimeSelectionPageMsg BeamtimeSelection.Msg
    | DataSetsMsg DataSets.Msg
    | ExperimentTypesMsg ExperimentTypes.Msg
    | AnalysisOverviewPageMsg AnalysisOverview.Msg
    | SingleDataSetPageMsg SingleDataSet.Msg
    | RunAnalysisPageMsg RunAnalysis.Msg
    | ScheduleMsg Schedule.ScheduleMsg
    | EventLogPageMsg EventLog.Msg
    | LinkClicked UrlRequest
    | UrlChanged Url
    | HereAndNowReceived HereAndNow
    | BeamtimeReceived (Result HttpError JsonBeamtime)


type Page
    = RootPage
    | AttributiPage Attributi.Model
    | ChemicalsPage Chemicals.Model
    | MergeResultPage MergeResult.Model
    | RunOverviewPage RunOverview.Model
    | RunsPage Runs.Model
    | ImportPage Import.Model
    | AdvancedControlsPage AdvancedControls.Model
    | BeamtimeSelectionPage BeamtimeSelection.Model
    | DataSetsPage DataSets.DataSetModel
    | SchedulePage Schedule.ScheduleModel
    | EventLogPage EventLog.Model
    | ExperimentTypesPage ExperimentTypes.Model
    | AnalysisOverviewPage AnalysisOverview.Model
    | SingleDataSetPage SingleDataSet.Model
    | RunAnalysisPage RunAnalysis.Model


type alias Model =
    { route : Route
    , page : Page
    , navKey : Nav.Key
    , metadata : Metadata
    }


type alias Metadata =
    { hereAndNow : Maybe HereAndNow
    , beamtimeRequest : RemoteData HttpError JsonBeamtime
    , localStorage : Maybe LocalStorage
    }


retrieveRouteBeamtime : Route -> Cmd Msg
retrieveRouteBeamtime r =
    case Route.beamtimeIdInRoute r of
        Nothing ->
            Cmd.none

        Just btId ->
            send BeamtimeReceived (readBeamtimeApiBeamtimesBeamtimeIdGet btId)


init : Maybe String -> Url -> Nav.Key -> ( Model, Cmd Msg )
init localStorageStr url navKey =
    let
        route =
            Route.parseUrlFragment url

        model =
            { route = route
            , page = RootPage
            , navKey = navKey
            , metadata =
                { hereAndNow = Nothing
                , beamtimeRequest =
                    case Route.beamtimeIdInRoute route of
                        Nothing ->
                            NotAsked

                        Just _ ->
                            Loading
                , localStorage = Maybe.andThen decodeLocalStorage localStorageStr
                }
            }
    in
    ( model, Cmd.batch [ Task.perform HereAndNowReceived retrieveHereAndNow, retrieveRouteBeamtime route ] )


buildTitleForPage : Page -> String
buildTitleForPage page =
    case page of
        RootPage ->
            "Beamtime Selection"

        AttributiPage model ->
            Attributi.pageTitle model

        ChemicalsPage model ->
            Chemicals.pageTitle model

        MergeResultPage model ->
            MergeResult.pageTitle model

        RunOverviewPage model ->
            RunOverview.pageTitle model

        ImportPage model ->
            Import.pageTitle model

        RunsPage model ->
            Runs.pageTitle model

        AdvancedControlsPage model ->
            AdvancedControls.pageTitle model

        BeamtimeSelectionPage model ->
            BeamtimeSelection.pageTitle model

        DataSetsPage model ->
            DataSets.pageTitle model

        SchedulePage model ->
            Schedule.pageTitle model

        EventLogPage model ->
            EventLog.pageTitle model

        ExperimentTypesPage model ->
            ExperimentTypes.pageTitle model

        AnalysisOverviewPage model ->
            AnalysisOverview.pageTitle model

        SingleDataSetPage model ->
            SingleDataSet.pageTitle model

        RunAnalysisPage model ->
            RunAnalysis.pageTitle model


buildTitle : Model -> String
buildTitle model =
    case model.metadata.beamtimeRequest of
        Success { title } ->
            let
                prefix =
                    buildTitleForPage model.page

                suffix =
                    " | " ++ title ++ " | AMARCORD"
            in
            prefix ++ suffix

        NotAsked ->
            "AMARCORD | Beamtime Selection"

        _ ->
            "AMARCORD"


view : Model -> Document Msg
view model =
    let
        tabTitle =
            buildTitle model

        displayTitle =
            case model.metadata.beamtimeRequest of
                Success { title, externalId } ->
                    div [ class "vstack me-md-auto" ]
                        [ div [ class "fs-4" ] [ a [ class "text-dark text-decoration-none" ] [ text title ] ]
                        , div [] [ small [] [ text ("ID: " ++ externalId) ] ]
                        ]

                _ ->
                    text ""
    in
    { title = tabTitle
    , body =
        [ main_ []
            [ div [ class "container" ]
                [ header
                    [ class "d-flex align-items-center justify-content-center py-3 mb-4 border-bottom" ]
                    [ img [ src "amarcord-logo.png", alt "AMARCORD logo", class "img-fluid amarcord-logo" ] [], displayTitle, viewMenu model.route ]
                ]
            , currentViewOuter model
            , div [ class "container mt-5 text-center" ]
                [ p [ class "text-muted" ] [ img_ [ src "desy-cfel.png", alt "DESY and CFEL logo combined", class "img-fluid amarcord-logo" ], text <| "AMARCORD Version: " ++ version ]
                ]
            ]
        ]
    }


currentViewOuter : Model -> Html Msg
currentViewOuter model =
    case model.metadata.beamtimeRequest of
        Success _ ->
            currentView model

        NotAsked ->
            currentView model

        _ ->
            div [ class "container" ]
                [ h1_ [ text "Web server is offline" ]
                , p [ class "lead" ] [ text "Might just be temporary (so please retry loading this in a minute). Otherwise, please contact the Admin." ]
                , viewRemoteDataHttp "Configuration" model.metadata.beamtimeRequest
                ]


currentView : Model -> Html Msg
currentView model =
    case model.page of
        RootPage ->
            Help.view

        AttributiPage pageModel ->
            div []
                [ Attributi.view pageModel
                    |> Html.map AttributiPageMsg
                ]

        AdvancedControlsPage pageModel ->
            div []
                [ AdvancedControls.view pageModel
                    |> Html.map AdvancedControlsPageMsg
                ]

        SchedulePage sm ->
            div []
                [ Schedule.view sm
                    |> Html.map ScheduleMsg
                ]

        EventLogPage sm ->
            div []
                [ EventLog.view sm
                    |> Html.map EventLogPageMsg
                ]

        ChemicalsPage pageModel ->
            div []
                [ Chemicals.view pageModel
                    |> Html.map ChemicalsPageMsg
                ]

        MergeResultPage pageModel ->
            div []
                [ MergeResult.view pageModel
                    |> Html.map MergeResultPageMsg
                ]

        RunsPage pageModel ->
            div []
                [ Runs.view pageModel
                    |> Html.map RunsPageMsg
                ]

        RunOverviewPage pageModel ->
            div []
                [ RunOverview.view pageModel
                    |> Html.map RunOverviewPageMsg
                ]

        ImportPage pageModel ->
            div []
                [ Import.view pageModel
                    |> Html.map ImportPageMsg
                ]

        AnalysisOverviewPage pageModel ->
            div []
                [ AnalysisOverview.view pageModel
                    |> Html.map AnalysisOverviewPageMsg
                ]

        SingleDataSetPage pageModel ->
            div []
                [ SingleDataSet.view pageModel
                    |> Html.map SingleDataSetPageMsg
                ]

        RunAnalysisPage pageModel ->
            div []
                [ RunAnalysis.view pageModel
                    |> Html.map RunAnalysisPageMsg
                ]

        DataSetsPage dataSetModel ->
            div []
                [ DataSets.view dataSetModel
                    |> Html.map DataSetsMsg
                ]

        ExperimentTypesPage experimentTypeModel ->
            div []
                [ ExperimentTypes.view experimentTypeModel
                    |> Html.map ExperimentTypesMsg
                ]

        BeamtimeSelectionPage beamtimeSelectionModel ->
            div []
                [ BeamtimeSelection.view beamtimeSelectionModel
                    |> Html.map BeamtimeSelectionPageMsg
                ]


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        BeamtimeReceived response ->
            let
                oldMetadata =
                    model.metadata
            in
            case response of
                Err e ->
                    ( { model | metadata = { oldMetadata | beamtimeRequest = Failure e } }, Cmd.none )

                Ok beamtime ->
                    let
                        newModel =
                            { model | metadata = { oldMetadata | beamtimeRequest = Success beamtime } }
                    in
                    case model.metadata.hereAndNow of
                        Nothing ->
                            ( newModel, Cmd.none )

                        Just hereAndNowUnpacked ->
                            initCurrentPage model.metadata.localStorage hereAndNowUnpacked ( newModel, Cmd.none )

        HereAndNowReceived hereAndNow ->
            let
                oldMetadata =
                    model.metadata
            in
            case model.metadata.beamtimeRequest of
                Success _ ->
                    initCurrentPage model.metadata.localStorage hereAndNow ( { model | metadata = { oldMetadata | hereAndNow = Just hereAndNow } }, Cmd.none )

                NotAsked ->
                    initCurrentPage
                        model.metadata.localStorage
                        hereAndNow
                        ( { model | metadata = { oldMetadata | hereAndNow = Just hereAndNow } }, Cmd.none )

                _ ->
                    ( { model | metadata = { oldMetadata | hereAndNow = Just hereAndNow } }, Cmd.none )

        _ ->
            case ( model.metadata.hereAndNow, model.metadata.beamtimeRequest ) of
                ( Just hereAndNow, Success _ ) ->
                    updateInner hereAndNow msg model

                ( Just hereAndNow, NotAsked ) ->
                    updateInner hereAndNow msg model

                _ ->
                    ( model, Cmd.none )


updateInner : HereAndNow -> Msg -> Model -> ( Model, Cmd Msg )
updateInner hereAndNow msg model =
    case ( msg, model.page ) of
        ( AttributiPageMsg subMsg, AttributiPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    Attributi.update subMsg pageModel
            in
            ( { model | page = AttributiPage updatedPageModel }
            , Cmd.map AttributiPageMsg updatedCmd
            )

        ( AdvancedControlsPageMsg subMsg, AdvancedControlsPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    AdvancedControls.update subMsg pageModel
            in
            ( { model | page = AdvancedControlsPage updatedPageModel }
            , Cmd.map AdvancedControlsPageMsg updatedCmd
            )

        ( ChemicalsPageMsg subMsg, ChemicalsPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    Chemicals.update subMsg pageModel
            in
            ( { model | page = ChemicalsPage updatedPageModel }
            , Cmd.map ChemicalsPageMsg updatedCmd
            )

        ( MergeResultPageMsg subMsg, MergeResultPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    MergeResult.update subMsg pageModel
            in
            ( { model | page = MergeResultPage updatedPageModel }
            , Cmd.map MergeResultPageMsg updatedCmd
            )

        ( ScheduleMsg scheduleMsg, SchedulePage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    Schedule.updateSchedule scheduleMsg pageModel
            in
            ( { model | page = SchedulePage updatedPageModel }
            , Cmd.map ScheduleMsg updatedCmd
            )

        ( EventLogPageMsg eventLogMsg, EventLogPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    EventLog.update eventLogMsg pageModel
            in
            ( { model | page = EventLogPage updatedPageModel }
            , Cmd.map EventLogPageMsg updatedCmd
            )

        ( RunsPageMsg subMsg, RunsPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    Runs.update subMsg pageModel
            in
            ( { model | page = RunsPage updatedPageModel }
            , Cmd.map RunsPageMsg updatedCmd
            )

        ( RunOverviewPageMsg subMsg, RunOverviewPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    RunOverview.update subMsg pageModel
            in
            ( { model | page = RunOverviewPage updatedPageModel }
            , Cmd.map RunOverviewPageMsg updatedCmd
            )

        ( ImportPageMsg subMsg, ImportPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    Import.update subMsg pageModel
            in
            ( { model | page = ImportPage updatedPageModel }
            , Cmd.map ImportPageMsg updatedCmd
            )

        ( BeamtimeSelectionPageMsg subMsg, BeamtimeSelectionPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    BeamtimeSelection.update subMsg pageModel
            in
            ( { model | page = BeamtimeSelectionPage updatedPageModel }
            , Cmd.map BeamtimeSelectionPageMsg updatedCmd
            )

        ( DataSetsMsg subMsg, DataSetsPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    DataSets.updateDataSet subMsg pageModel
            in
            ( { model | page = DataSetsPage updatedPageModel }
            , Cmd.map DataSetsMsg updatedCmd
            )

        ( ExperimentTypesMsg subMsg, ExperimentTypesPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    ExperimentTypes.update subMsg pageModel
            in
            ( { model | page = ExperimentTypesPage updatedPageModel }
            , Cmd.map ExperimentTypesMsg updatedCmd
            )

        ( AnalysisOverviewPageMsg subMsg, AnalysisOverviewPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    AnalysisOverview.update subMsg pageModel
            in
            ( { model | page = AnalysisOverviewPage updatedPageModel }
            , Cmd.map AnalysisOverviewPageMsg updatedCmd
            )

        ( SingleDataSetPageMsg subMsg, SingleDataSetPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    SingleDataSet.update subMsg pageModel
            in
            ( { model | page = SingleDataSetPage updatedPageModel }
            , Cmd.map SingleDataSetPageMsg updatedCmd
            )

        ( RunAnalysisPageMsg subMsg, RunAnalysisPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    RunAnalysis.update subMsg pageModel
            in
            ( { model | page = RunAnalysisPage updatedPageModel }
            , Cmd.map RunAnalysisPageMsg updatedCmd
            )

        ( LinkClicked urlRequest, _ ) ->
            case urlRequest of
                Browser.Internal url ->
                    -- Special case here; if this wasn't present, we'd try to open the /api prefix stuff and the
                    -- routing would fail.
                    if contains "api/files/" url.path || endsWith "/log" url.path || endsWith "/errorlog" url.path || contains "spreadsheet.zip" url.path || contains "run-bulk-import-template" url.path then
                        ( model, Nav.load (URL.toString url) )

                    else
                        ( model, Nav.pushUrl model.navKey (URL.toString url) )

                -- So we can create links without a href attribute and do things like open the navigation popup menu
                -- at the top. Solution taken from here:
                -- https://stackoverflow.com/questions/52708345/a-elements-without-href-or-with-an-empty-href-causes-page-reload-when-using-br
                Browser.External "" ->
                    ( model, Cmd.none )

                Browser.External url ->
                    ( model, Nav.load url )

        ( UrlChanged url, _ ) ->
            -- Here our URL changed. We might have switched beam times
            -- (if we came from the overview, or something), so here
            -- we have to check if we need another beam time request.
            let
                newRoute =
                    Route.parseUrlFragment url

                beamtimeIdInRoute =
                    Route.beamtimeIdInRoute newRoute

                oldBeamtimeId =
                    case model.metadata.beamtimeRequest of
                        Success { id } ->
                            Just id

                        _ ->
                            Nothing

                newBeamtimeRequest =
                    case beamtimeIdInRoute of
                        Nothing ->
                            NotAsked

                        Just newBeamtimeId ->
                            if Just newBeamtimeId == oldBeamtimeId then
                                model.metadata.beamtimeRequest

                            else
                                Loading

                retrieveRouteBeamtimeCmd : Cmd Msg
                retrieveRouteBeamtimeCmd =
                    case beamtimeIdInRoute of
                        Nothing ->
                            Cmd.none

                        Just btId ->
                            if Just btId == oldBeamtimeId then
                                Cmd.none

                            else
                                send BeamtimeReceived (readBeamtimeApiBeamtimesBeamtimeIdGet btId)

                oldMetadata =
                    model.metadata

                newMetadata =
                    { oldMetadata | beamtimeRequest = newBeamtimeRequest }
            in
            ( { model | route = newRoute, metadata = newMetadata }, retrieveRouteBeamtimeCmd )
                |> initCurrentPage model.metadata.localStorage hereAndNow

        _ ->
            ( model, Cmd.none )


initCurrentPage : Maybe LocalStorage -> HereAndNow -> ( Model, Cmd Msg ) -> ( Model, Cmd Msg )
initCurrentPage localStorage hereAndNow ( model, existingCmds ) =
    let
        oldMetadata =
            model.metadata

        ( currentPage, mappedPageCmds ) =
            case model.route of
                Route.Root _ ->
                    ( RootPage, Cmd.none )

                Route.Attributi beamtimeId ->
                    let
                        ( pageModel, pageCmds ) =
                            Attributi.init hereAndNow beamtimeId
                    in
                    ( AttributiPage pageModel, Cmd.map AttributiPageMsg pageCmds )

                Route.AdvancedControls beamtimeId ->
                    let
                        ( pageModel, pageCmds ) =
                            AdvancedControls.init hereAndNow beamtimeId
                    in
                    ( AdvancedControlsPage pageModel, Cmd.map AdvancedControlsPageMsg pageCmds )

                Route.Chemicals beamtimeId ->
                    let
                        ( pageModel, pageCmds ) =
                            Chemicals.init hereAndNow beamtimeId
                    in
                    ( ChemicalsPage pageModel, Cmd.map ChemicalsPageMsg pageCmds )

                Route.MergeResult beamtimeId experimentTypeId dataSetId mergeResultId ->
                    let
                        ( pageModel, pageCmds ) =
                            MergeResult.init model.navKey hereAndNow beamtimeId experimentTypeId dataSetId mergeResultId
                    in
                    ( MergeResultPage pageModel, Cmd.map MergeResultPageMsg pageCmds )

                Route.Runs beamtimeId runsRange ->
                    let
                        ( pageModel, pageCmds ) =
                            Runs.init hereAndNow localStorage beamtimeId runsRange
                    in
                    ( RunsPage pageModel, Cmd.map RunsPageMsg pageCmds )

                Route.RunOverview beamtimeId ->
                    let
                        ( pageModel, pageCmds ) =
                            RunOverview.init hereAndNow localStorage beamtimeId
                    in
                    ( RunOverviewPage pageModel, Cmd.map RunOverviewPageMsg pageCmds )

                Route.Import beamtimeId step ->
                    let
                        ( pageModel, pageCmds ) =
                            Import.init hereAndNow beamtimeId step
                    in
                    ( ImportPage pageModel, Cmd.map ImportPageMsg pageCmds )

                Route.AnalysisOverview beamtimeId filters across mergeFilter ->
                    let
                        ( pageModel, pageCmds ) =
                            AnalysisOverview.init model.navKey hereAndNow beamtimeId filters across mergeFilter
                    in
                    ( AnalysisOverviewPage pageModel, Cmd.map AnalysisOverviewPageMsg pageCmds )

                Route.AnalysisDataSet beamtimeId dsId ->
                    let
                        ( pageModel, pageCmds ) =
                            SingleDataSet.init model.navKey hereAndNow beamtimeId dsId
                    in
                    ( SingleDataSetPage pageModel, Cmd.map SingleDataSetPageMsg pageCmds )

                Route.RunAnalysis beamtimeId ->
                    let
                        ( pageModel, pageCmds ) =
                            RunAnalysis.init hereAndNow beamtimeId
                    in
                    ( RunAnalysisPage pageModel, Cmd.map RunAnalysisPageMsg pageCmds )

                Route.DataSets beamtimeId ->
                    let
                        ( pageModel, pageCmds ) =
                            DataSets.initDataSet hereAndNow beamtimeId
                    in
                    ( DataSetsPage pageModel, Cmd.map DataSetsMsg pageCmds )

                Route.Schedule beamtimeId ->
                    let
                        ( pageModel, pageCmds ) =
                            Schedule.initSchedule hereAndNow beamtimeId
                    in
                    ( SchedulePage pageModel, Cmd.map ScheduleMsg pageCmds )

                Route.EventLog beamtimeId ->
                    let
                        ( pageModel, pageCmds ) =
                            EventLog.init hereAndNow beamtimeId
                    in
                    ( EventLogPage pageModel, Cmd.map EventLogPageMsg pageCmds )

                Route.ExperimentTypes beamtimeId ->
                    let
                        ( pageModel, pageCmds ) =
                            ExperimentTypes.init hereAndNow.zone beamtimeId
                    in
                    ( ExperimentTypesPage pageModel, Cmd.map ExperimentTypesMsg pageCmds )

                Route.BeamtimeSelection ->
                    let
                        ( pageModel, pageCmds ) =
                            BeamtimeSelection.init hereAndNow
                    in
                    ( BeamtimeSelectionPage pageModel, Cmd.map BeamtimeSelectionPageMsg pageCmds )
    in
    ( { model | page = currentPage, metadata = { oldMetadata | hereAndNow = Just hereAndNow } }, Cmd.batch [ existingCmds, mappedPageCmds ] )
