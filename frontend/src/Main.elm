-- Certain tools (elm2nix, for example) assume that Main is directly below src/, so it's here.
-- In case you're wondering...


module Main exposing (main)

import Amarcord.Html exposing (h1_)
import Amarcord.Menu exposing (viewMenu)
import Amarcord.Pages.Analysis as Analysis
import Amarcord.Pages.Attributi as Attributi
import Amarcord.Pages.DataSets as DataSets
import Amarcord.Pages.ExperimentTypes as ExperimentTypes
import Amarcord.Pages.RunOverview as RunOverview
import Amarcord.Pages.Samples as Samples
import Amarcord.Route as Route exposing (Route)
import Amarcord.Util exposing (HereAndNow, retrieveHereAndNow)
import Amarcord.Version exposing (version)
import Browser exposing (Document, UrlRequest)
import Browser.Navigation as Nav
import Html as Html exposing (..)
import Html.Attributes exposing (..)
import String exposing (startsWith)
import Task
import Time exposing (Posix, Zone)
import Url as URL exposing (Url)


main : Program () Model Msg
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
    | DataSetsMsg DataSets.DataSetMsg
    | ExperimentTypesMsg ExperimentTypes.ExperimentTypeMsg
    | AnalysisPageMsg Analysis.Msg
    | LinkClicked UrlRequest
    | UrlChanged Url
    | RefreshMsg Posix
    | HereAndNowReceived HereAndNow


type Page
    = RootPage
    | AttributiPage Attributi.Model
    | SamplesPage Samples.Model
    | RunOverviewPage RunOverview.Model
    | DataSetsPage DataSets.DataSetModel
    | ExperimentTypesPage ExperimentTypes.ExperimentTypeModel
    | AnalysisPage Analysis.Model


type alias Model =
    { route : Route
    , page : Page
    , navKey : Nav.Key
    , hereAndNow : Maybe HereAndNow
    }


init : () -> Url -> Nav.Key -> ( Model, Cmd Msg )
init _ url navKey =
    let
        model =
            { route = Route.parseUrlFragment url
            , page = RootPage
            , navKey = navKey
            , hereAndNow = Nothing
            }
    in
    ( model, Task.perform HereAndNowReceived <| retrieveHereAndNow )


view : Model -> Document Msg
view model =
    { title = "AMARCORD"
    , body =
        [ main_ []
            [ div [ class "container" ]
                [ header
                    [ class "d-flex flex-wrap justify-content-center py-3 mb-4 border-bottom" ]
                    [ img [ src "desy-cfel.png", alt "DESY and CFEL logo combined", class "img-fluid amarcord-logo" ] []
                    , a [ class "d-flex align-items-center mb-3 mb-md-0 me-md-auto text-dark text-decoration-none" ]
                        [ span [ class "fs-4" ] [ text "AMARCORD" ]
                        , span [ class "text-muted" ] [ sub [] [ text version ] ]
                        ]
                    , viewMenu model.route
                    ]
                ]
            , currentView model
            ]
        ]
    }


currentView : Model -> Html Msg
currentView model =
    case model.page of
        RootPage ->
            div [ class "container" ] [ h1_ [ text "Welcome to AMARCORD!" ], p [ class "lead" ] [ text "Depending on what you want to do, check out the sections at the top." ] ]

        AttributiPage pageModel ->
            div []
                [ Attributi.view pageModel
                    |> Html.map AttributiPageMsg
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
        HereAndNowReceived hereAndNow ->
            initCurrentPage hereAndNow ( { model | hereAndNow = Just hereAndNow }, Cmd.none )

        _ ->
            case model.hereAndNow of
                Nothing ->
                    ( model, Cmd.none )

                Just hereAndNow ->
                    updateInner hereAndNow msg model


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

        ( LinkClicked urlRequest, _ ) ->
            case urlRequest of
                Browser.Internal url ->
                    -- Special case here; if this wasn't present, we'd try to open the /api prefix stuff and the
                    -- routing would fail.
                    if startsWith "/api/files/" url.path then
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
                |> initCurrentPage hereAndNow

        ( _, _ ) ->
            ( model, Cmd.none )


initCurrentPage : HereAndNow -> ( Model, Cmd Msg ) -> ( Model, Cmd Msg )
initCurrentPage hereAndNow ( model, existingCmds ) =
    let
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

                Route.Samples ->
                    let
                        ( pageModel, pageCmds ) =
                            Samples.init hereAndNow
                    in
                    ( SamplesPage pageModel, Cmd.map SamplesPageMsg pageCmds )

                Route.RunOverview ->
                    let
                        ( pageModel, pageCmds ) =
                            RunOverview.init hereAndNow
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
    ( { model | page = currentPage, hereAndNow = Just hereAndNow }, Cmd.batch [ existingCmds, mappedPageCmds ] )
