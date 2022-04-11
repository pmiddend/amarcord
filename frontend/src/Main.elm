-- Certain tools (elm2nix, for example) assume that Main is directly below src/, so it's here.
-- In case you're wondering...


module Main exposing (main)

import Amarcord.API.Requests exposing (AppConfig, RequestError, httpGetConfig)
import Amarcord.Bootstrap exposing (viewRemoteData)
import Amarcord.Html exposing (h1_, img_)
import Amarcord.LocalStorage exposing (LocalStorage, decodeLocalStorage)
import Amarcord.Menu exposing (viewMenu)
import Amarcord.Pages.AdvancedControls as AdvancedControls
import Amarcord.Pages.Analysis as Analysis
import Amarcord.Pages.Attributi as Attributi
import Amarcord.Pages.DataSets as DataSets
import Amarcord.Pages.ExperimentTypes as ExperimentTypes
import Amarcord.Pages.Help as Help
import Amarcord.Pages.RunOverview as RunOverview
import Amarcord.Pages.Samples as Samples
import Amarcord.Route as Route exposing (Route)
import Amarcord.Util exposing (HereAndNow, retrieveHereAndNow)
import Amarcord.Version exposing (version)
import Browser exposing (Document, UrlRequest)
import Browser.Navigation as Nav
import Html as Html exposing (..)
import Html.Attributes exposing (..)
import RemoteData exposing (RemoteData(..))
import String exposing (contains)
import Task
import Time exposing (Posix, Zone)
import Url as URL exposing (Url)


main : Program (Maybe String) Model Msg
main =
    Browser.application
        { init = init
        , view = view
        , update = update
        , subscriptions = \_ -> Time.every 10000 RefreshMsg
        , onUrlRequest = LinkClicked
        , onUrlChange = UrlChanged
        }


type Msg
    = AttributiPageMsg Attributi.Msg
    | SamplesPageMsg Samples.Msg
    | RunOverviewPageMsg RunOverview.Msg
    | AdvancedControlsPageMsg AdvancedControls.Msg
    | DataSetsMsg DataSets.DataSetMsg
    | ExperimentTypesMsg ExperimentTypes.ExperimentTypeMsg
    | AnalysisPageMsg Analysis.Msg
    | LinkClicked UrlRequest
    | UrlChanged Url
    | RefreshMsg Posix
    | HereAndNowReceived HereAndNow
    | ConfigReceived (Result RequestError AppConfig)


type Page
    = RootPage
    | AttributiPage Attributi.Model
    | SamplesPage Samples.Model
    | RunOverviewPage RunOverview.Model
    | AdvancedControlsPage AdvancedControls.Model
    | DataSetsPage DataSets.DataSetModel
    | ExperimentTypesPage ExperimentTypes.ExperimentTypeModel
    | AnalysisPage Analysis.Model


type alias Model =
    { route : Route
    , page : Page
    , navKey : Nav.Key
    , metadata : Metadata
    }


type alias Metadata =
    { hereAndNow : Maybe HereAndNow
    , appConfigRequest : RemoteData RequestError AppConfig
    , localStorage : Maybe LocalStorage
    }


init : Maybe String -> Url -> Nav.Key -> ( Model, Cmd Msg )
init localStorageStr url navKey =
    let
        model =
            { route = Route.parseUrlFragment url
            , page = RootPage
            , navKey = navKey
            , metadata =
                { hereAndNow = Nothing
                , appConfigRequest = Loading
                , localStorage = Maybe.andThen decodeLocalStorage localStorageStr
                }
            }
    in
    ( model, Cmd.batch [ Task.perform HereAndNowReceived <| retrieveHereAndNow, httpGetConfig ConfigReceived ] )


view : Model -> Document Msg
view model =
    let
        displayTitle =
            case model.metadata.appConfigRequest of
                Success { title } ->
                    title

                _ ->
                    "AMARCORD"
    in
    { title = displayTitle
    , body =
        [ main_ []
            [ div [ class "container" ]
                [ header
                    [ class "d-flex flex-wrap justify-content-center py-3 mb-4 border-bottom" ]
                    [ img [ src "amarcord-logo.png", alt "AMARCORD logo", class "img-fluid amarcord-logo" ] []
                    , a [ class "d-flex align-items-center mb-3 mb-md-0 me-md-auto text-dark text-decoration-none" ]
                        [ span [ class "fs-4" ] [ text displayTitle ]
                        ]
                    , viewMenu model.route
                    ]
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
    case model.metadata.appConfigRequest of
        Success _ ->
            currentView model

        _ ->
            div [ class "container" ]
                [ h1_ [ text "Web server is offline" ]
                , p [ class "lead" ] [ text "Might just be temporary (so please retry loading this in a minute). Otherwise, please contact the Admin." ]
                , viewRemoteData "Configuration" model.metadata.appConfigRequest
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

        SamplesPage pageModel ->
            div []
                [ Samples.view pageModel
                    |> Html.map SamplesPageMsg
                ]

        RunOverviewPage pageModel ->
            div []
                [ RunOverview.view pageModel
                    |> Html.map RunOverviewPageMsg
                ]

        AnalysisPage pageModel ->
            div []
                [ Analysis.view pageModel
                    |> Html.map AnalysisPageMsg
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


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        ConfigReceived appConfig ->
            let
                oldMetadata =
                    model.metadata
            in
            case appConfig of
                Err e ->
                    ( { model | metadata = { oldMetadata | appConfigRequest = Failure e } }, Cmd.none )

                Ok appConfigUnpacked ->
                    let
                        newModel =
                            { model | metadata = { oldMetadata | appConfigRequest = Success appConfigUnpacked } }
                    in
                    case model.metadata.hereAndNow of
                        Nothing ->
                            ( newModel, Cmd.none )

                        Just hereAndNowUnpacked ->
                            initCurrentPage model.metadata.localStorage hereAndNowUnpacked appConfigUnpacked ( newModel, Cmd.none )

        HereAndNowReceived hereAndNow ->
            let
                oldMetadata =
                    model.metadata
            in
            case model.metadata.appConfigRequest of
                Success appConfig ->
                    initCurrentPage model.metadata.localStorage hereAndNow appConfig ( { model | metadata = { oldMetadata | hereAndNow = Just hereAndNow } }, Cmd.none )

                _ ->
                    ( { model | metadata = { oldMetadata | hereAndNow = Just hereAndNow } }, Cmd.none )

        _ ->
            case ( model.metadata.hereAndNow, model.metadata.appConfigRequest ) of
                ( Just hereAndNow, Success appConfig ) ->
                    updateInner hereAndNow appConfig msg model

                ( _, _ ) ->
                    ( model, Cmd.none )


updateInner : HereAndNow -> AppConfig -> Msg -> Model -> ( Model, Cmd Msg )
updateInner hereAndNow appConfig msg model =
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

        ( SamplesPageMsg subMsg, SamplesPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    Samples.update subMsg pageModel
            in
            ( { model | page = SamplesPage updatedPageModel }
            , Cmd.map SamplesPageMsg updatedCmd
            )

        ( RunOverviewPageMsg subMsg, RunOverviewPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    RunOverview.update subMsg pageModel
            in
            ( { model | page = RunOverviewPage updatedPageModel }
            , Cmd.map RunOverviewPageMsg updatedCmd
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
                    ExperimentTypes.updateExperimentType subMsg pageModel
            in
            ( { model | page = ExperimentTypesPage updatedPageModel }
            , Cmd.map ExperimentTypesMsg updatedCmd
            )

        ( AnalysisPageMsg subMsg, AnalysisPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    Analysis.update subMsg pageModel
            in
            ( { model | page = AnalysisPage updatedPageModel }
            , Cmd.map AnalysisPageMsg updatedCmd
            )

        ( RefreshMsg t, RunOverviewPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    RunOverview.update (RunOverview.Refresh t) pageModel
            in
            ( { model | page = RunOverviewPage updatedPageModel }
            , Cmd.map RunOverviewPageMsg updatedCmd
            )

        ( RefreshMsg t, AdvancedControlsPage pageModel ) ->
            let
                ( updatedPageModel, updatedCmd ) =
                    AdvancedControls.update (AdvancedControls.Refresh t) pageModel
            in
            ( { model | page = AdvancedControlsPage updatedPageModel }
            , Cmd.map AdvancedControlsPageMsg updatedCmd
            )

        ( LinkClicked urlRequest, _ ) ->
            case urlRequest of
                Browser.Internal url ->
                    -- Special case here; if this wasn't present, we'd try to open the /api prefix stuff and the
                    -- routing would fail.
                    if contains "api/files/" url.path then
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
            let
                newRoute =
                    Route.parseUrlFragment url
            in
            ( { model | route = newRoute }, Cmd.none )
                |> initCurrentPage model.metadata.localStorage hereAndNow appConfig

        ( _, _ ) ->
            ( model, Cmd.none )


initCurrentPage : Maybe LocalStorage -> HereAndNow -> AppConfig -> ( Model, Cmd Msg ) -> ( Model, Cmd Msg )
initCurrentPage localStorage hereAndNow appConfig ( model, existingCmds ) =
    let
        oldMetadata =
            model.metadata

        ( currentPage, mappedPageCmds ) =
            case model.route of
                Route.Root ->
                    ( RootPage, Cmd.none )

                Route.Attributi ->
                    let
                        ( pageModel, pageCmds ) =
                            Attributi.init hereAndNow
                    in
                    ( AttributiPage pageModel, Cmd.map AttributiPageMsg pageCmds )

                Route.AdvancedControls ->
                    let
                        ( pageModel, pageCmds ) =
                            AdvancedControls.init
                    in
                    ( AdvancedControlsPage pageModel, Cmd.map AdvancedControlsPageMsg pageCmds )

                Route.Samples ->
                    let
                        ( pageModel, pageCmds ) =
                            Samples.init hereAndNow
                    in
                    ( SamplesPage pageModel, Cmd.map SamplesPageMsg pageCmds )

                Route.RunOverview ->
                    let
                        ( pageModel, pageCmds ) =
                            RunOverview.init hereAndNow localStorage
                    in
                    ( RunOverviewPage pageModel, Cmd.map RunOverviewPageMsg pageCmds )

                Route.Analysis ->
                    let
                        ( pageModel, pageCmds ) =
                            Analysis.init hereAndNow
                    in
                    ( AnalysisPage pageModel, Cmd.map AnalysisPageMsg pageCmds )

                Route.DataSets ->
                    let
                        ( pageModel, pageCmds ) =
                            DataSets.initDataSet hereAndNow
                    in
                    ( DataSetsPage pageModel, Cmd.map DataSetsMsg pageCmds )

                Route.ExperimentTypes ->
                    let
                        ( pageModel, pageCmds ) =
                            ExperimentTypes.initExperimentType
                    in
                    ( ExperimentTypesPage pageModel, Cmd.map ExperimentTypesMsg pageCmds )
    in
    ( { model | page = currentPage, metadata = { oldMetadata | hereAndNow = Just hereAndNow } }, Cmd.batch [ existingCmds, mappedPageCmds ] )
