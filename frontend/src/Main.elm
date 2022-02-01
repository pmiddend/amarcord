-- Certain tools (elm2nix, for example) assume that Main is directly below src/, so it's here.
-- In case you're wondering...


module Main exposing (main)

import Amarcord.Attributi as Attributi
import Amarcord.Bootstrap exposing (icon)
import Amarcord.Html exposing (h1_)
import Amarcord.Route as Route exposing (Route, makeLink, parseUrl)
import Amarcord.RunOverview as RunOverview
import Amarcord.Samples as Samples
import Browser exposing (Document, UrlRequest)
import Browser.Navigation as Nav
import Html as Html exposing (..)
import Html.Attributes exposing (..)
import String exposing (startsWith)
import Url as URL exposing (Url)


main : Program () Model Msg
main =
    Browser.application
        { init = init
        , view = view
        , update = update
        , subscriptions = \_ -> Sub.none
        , onUrlRequest = LinkClicked
        , onUrlChange = UrlChanged
        }


type Msg
    = AttributiPageMsg Attributi.Msg
    | SamplesPageMsg Samples.Msg
    | RunOverviewPageMsg RunOverview.Msg
    | LinkClicked UrlRequest
    | UrlChanged Url


type Page
    = RootPage
    | AttributiPage Attributi.Model
    | SamplesPage Samples.Model
    | RunOverviewPage RunOverview.Model


type alias Model =
    { route : Route
    , page : Page
    , navKey : Nav.Key
    }


init : () -> Url -> Nav.Key -> ( Model, Cmd Msg )
init _ url navKey =
    let
        model =
            { route = parseUrl url, page = RootPage, navKey = navKey }
    in
    initCurrentPage ( model, Cmd.none )


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
                        ]
                    , ul [ class "nav nav-pills" ]
                        [ li [ class "nav-item" ]
                            [ a
                                [ href (makeLink Route.RunOverview)
                                , class
                                    ((if model.route == Route.RunOverview then
                                        "active "

                                      else
                                        ""
                                     )
                                        ++ "nav-link"
                                    )
                                ]
                                [ icon { name = "card-list" }, text " Overview" ]
                            ]
                        , li [ class "nav-item" ]
                            [ a
                                [ href (makeLink Route.Samples)
                                , class
                                    ((if model.route == Route.Samples then
                                        "active "

                                      else
                                        ""
                                     )
                                        ++ "nav-link"
                                    )
                                ]
                                [ icon { name = "gem" }, text " Samples" ]
                            ]
                        , li [ class "nav-item" ]
                            [ a
                                [ href (makeLink Route.Attributi)
                                , class
                                    ((if model.route == Route.Attributi then
                                        "active "

                                      else
                                        ""
                                     )
                                        ++ "nav-link"
                                    )
                                ]
                                [ icon { name = "card-list" }, text " Attributi" ]
                            ]
                        ]
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


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
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

        ( LinkClicked urlRequest, _ ) ->
            case urlRequest of
                Browser.Internal url ->
                    -- Special case here; if this wasn't present, we'd try to open the /api prefix stuff and the
                    -- routing would fail.
                    if startsWith "/api/files/" url.path then
                        ( model, Nav.load (URL.toString url) )

                    else
                        ( model, Nav.pushUrl model.navKey (URL.toString url) )

                Browser.External url ->
                    ( model, Nav.load url )

        ( UrlChanged url, _ ) ->
            let
                newRoute =
                    Route.parseUrl url
            in
            ( { model | route = newRoute }, Cmd.none )
                |> initCurrentPage

        ( _, _ ) ->
            ( model, Cmd.none )


initCurrentPage : ( Model, Cmd Msg ) -> ( Model, Cmd Msg )
initCurrentPage ( model, existingCmds ) =
    let
        ( currentPage, mappedPageCmds ) =
            case model.route of
                Route.Root ->
                    ( RootPage, Cmd.none )

                Route.Attributi ->
                    let
                        ( pageModel, pageCmds ) =
                            Attributi.init ()
                    in
                    ( AttributiPage pageModel, Cmd.map AttributiPageMsg pageCmds )

                Route.Samples ->
                    let
                        ( pageModel, pageCmds ) =
                            Samples.init ()
                    in
                    ( SamplesPage pageModel, Cmd.map SamplesPageMsg pageCmds )

                Route.RunOverview ->
                    let
                        ( pageModel, pageCmds ) =
                            RunOverview.init ()
                    in
                    ( RunOverviewPage pageModel, Cmd.map RunOverviewPageMsg pageCmds )
    in
    ( { model | page = currentPage }, Cmd.batch [ existingCmds, mappedPageCmds ] )
