module Amarcord.Pages.BeamtimeSelection exposing (Model, Msg(..), init, pageTitle, update, view)

import Amarcord.API.Requests exposing (invalidBeamtimeId)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, makeAlert, viewMarkdownSupportText)
import Amarcord.Html exposing (div_, form_, h2_, h4_, strongText)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.MarkdownUtil exposing (markupWithoutErrors)
import Amarcord.Route exposing (Route(..), makeLink)
import Amarcord.Util exposing (HereAndNow, formatPosixDateTimeCompatible, formatPosixHumanFriendly, localDateTimeStringToPosix, scrollToTop)
import Api.Data exposing (JsonBeamtime, JsonReadBeamtime)
import Api.Request.Beamtimes exposing (createBeamtimeApiBeamtimesPost, readBeamtimesApiBeamtimesGet, updateBeamtimeApiBeamtimesPatch)
import Html exposing (Html, a, button, div, input, label, li, p, span, table, tbody, td, text, textarea, th, thead, tr, ul)
import Html.Attributes as Attrs exposing (attribute, class, for, href, id, style, type_, value)
import Html.Events exposing (onClick, onInput)
import List exposing (sort)
import RemoteData exposing (RemoteData(..), fromResult)
import Result.Extra as ResultExtra
import Time exposing (Zone, millisToPosix, posixToMillis, utc)


type alias Model =
    { beamtimeResult : RemoteData HttpError (List JsonBeamtime)
    , beamtimeEdit : Maybe JsonBeamtime
    , modifyRequest : RemoteData HttpError ()
    , zone : Zone
    }


pageTitle : Model -> String
pageTitle _ =
    "Beamtime Selection"


type Msg
    = BeamtimesReceived (Result HttpError JsonReadBeamtime)
    | AddBeamtime
    | Nop
    | EditBeamtimeStart JsonBeamtime
    | EditBeamtimeSubmit
    | ChangeEditBeamtime (JsonBeamtime -> JsonBeamtime)
    | EditBeamtimeCancel
    | EditBeamtimeFinished (Result HttpError {})


init : HereAndNow -> ( Model, Cmd Msg )
init hereAndNow =
    ( { beamtimeResult = Loading
      , beamtimeEdit = Nothing
      , modifyRequest = NotAsked
      , zone = hereAndNow.zone
      }
    , send BeamtimesReceived readBeamtimesApiBeamtimesGet
    )


emptyBeamtime : JsonBeamtime
emptyBeamtime =
    { beamline = ""
    , comment = ""
    , start = 0
    , end = 0
    , externalId = ""
    , id = invalidBeamtimeId
    , proposal = ""
    , title = ""
    , chemicalNames = []
    }


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        Nop ->
            ( model, Cmd.none )

        ChangeEditBeamtime btModifier ->
            case model.beamtimeEdit of
                Nothing ->
                    ( model, Cmd.none )

                Just bt ->
                    ( { model | beamtimeEdit = Just (btModifier bt) }, Cmd.none )

        BeamtimesReceived response ->
            ( { model | beamtimeResult = fromResult (Result.map .beamtimes response) }, Cmd.none )

        AddBeamtime ->
            ( { model | beamtimeEdit = Just emptyBeamtime }, Cmd.none )

        EditBeamtimeStart bt ->
            ( { model | beamtimeEdit = Just bt }, scrollToTop (always Nop) )

        EditBeamtimeCancel ->
            ( { model | beamtimeEdit = Nothing }, Cmd.none )

        EditBeamtimeFinished result ->
            case result of
                Err e ->
                    ( { model | modifyRequest = Failure e }, Cmd.none )

                Ok _ ->
                    ( { model
                        | modifyRequest = Success ()
                        , beamtimeEdit = Nothing
                      }
                    , send BeamtimesReceived readBeamtimesApiBeamtimesGet
                    )

        EditBeamtimeSubmit ->
            case model.beamtimeEdit of
                Nothing ->
                    ( model, Cmd.none )

                Just bt ->
                    let
                        body =
                            { beamline = bt.beamline
                            , comment = bt.comment
                            , end = bt.end
                            , externalId = bt.externalId
                            , id = bt.id
                            , proposal = bt.proposal
                            , start = bt.start
                            , title = bt.title
                            }
                    in
                    ( { model | modifyRequest = Loading }
                    , if bt.id <= 0 then
                        send (EditBeamtimeFinished << Result.map (always {})) (createBeamtimeApiBeamtimesPost body)

                      else
                        send (EditBeamtimeFinished << Result.map (always {})) (updateBeamtimeApiBeamtimesPatch body)
                    )


viewBeamtimeTableRow : Zone -> JsonBeamtime -> Html Msg
viewBeamtimeTableRow zone ({ beamline, comment, start, end, externalId, id, proposal, title, chemicalNames } as bt) =
    tr []
        [ td [] [ strongText externalId ]
        , td [] [ text (String.fromInt id) ]
        , td [] [ a [ href (makeLink (RunOverview id)) ] [ text title ] ]
        , td [] [ text beamline ]
        , td [ class "text-nowrap" ] [ text proposal ]
        , td [] [ text (formatPosixHumanFriendly zone (millisToPosix start)) ]
        , td [] [ text (formatPosixHumanFriendly zone (millisToPosix end)) ]
        , td [] [ markupWithoutErrors comment ]
        , td []
            [ span [ class "accordion accordion-flush" ]
                [ div [ class "accordion-item" ]
                    [ div [ class "accordion-header" ]
                        [ button
                            [ class "accordion-button accordion-merge-parameters-header-button"
                            , type_ "button"
                            , attribute "data-bs-toggle" "collapse"
                            , attribute "data-bs-target" ("#chemicals" ++ String.fromInt id)
                            ]
                            [ text "Show chemicals"
                            ]
                        ]
                    , div [ class "accordion-collapse collapse", Attrs.id ("chemicals" ++ String.fromInt id) ]
                        [ div [ class "accordion-body" ]
                            [ ul [] (List.map (\chemicalName -> li [] [ text chemicalName ]) (sort chemicalNames))
                            ]
                        ]
                    ]
                ]
            ]
        , td []
            [ button
                [ class "btn btn-sm btn-info", class "amarcord-edit-button", onClick (EditBeamtimeStart bt) ]
                [ icon { name = "pencil-square" } ]
            ]
        ]


viewBeamtimes : Zone -> List JsonBeamtime -> Html Msg
viewBeamtimes zone beamtimes =
    table [ class "table table-striped", id "beamtime-table" ]
        [ thead []
            [ tr []
                [ th [] [ text "External ID" ]
                , th [] [ text "ID" ]
                , th [] [ text "Title" ]
                , th [] [ text "Beamline" ]
                , th [] [ text "Proposal" ]
                , th [] [ text "Start" ]
                , th [] [ text "End" ]
                , th [] [ text "Comment" ]
                , th [] [ text "Chemicals" ]
                , th [] [ text "Actions" ]
                ]
            ]
        , tbody [] (List.map (viewBeamtimeTableRow zone) beamtimes)
        ]


viewEditForm : JsonBeamtime -> Html Msg
viewEditForm bt =
    let
        addOrEditHeadline =
            h4_
                [ icon { name = "plus-lg" }
                , text
                    (if bt.id == invalidBeamtimeId then
                        " Add new beamtime"

                     else
                        " Edit " ++ bt.title
                    )
                ]
    in
    form_
        [ addOrEditHeadline
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
                , value (formatPosixDateTimeCompatible utc (millisToPosix bt.start))
                , onInput
                    (\newValue ->
                        ChangeEditBeamtime
                            (\bt2 ->
                                ResultExtra.unwrap
                                    bt2
                                    (\newParsed -> { bt2 | start = posixToMillis newParsed })
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
                , value (formatPosixDateTimeCompatible utc (millisToPosix bt.end))
                , onInput
                    (\newValue ->
                        ChangeEditBeamtime
                            (\bt2 ->
                                ResultExtra.unwrap
                                    bt2
                                    (\newParsed -> { bt2 | end = posixToMillis newParsed })
                                    (localDateTimeStringToPosix utc newValue)
                            )
                    )
                ]
                []
            , label [ for "beamtime-edit-end" ] [ text "End" ]
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


view : Model -> Html Msg
view model =
    div [ class "container" ]
        [ h2_ [ icon { name = "arrow-left-right" }, text " Beamtimes" ]
        , case model.beamtimeEdit of
            Nothing ->
                button
                    [ class "btn btn-primary", onClick AddBeamtime, id "add-beamtime-button" ]
                    [ icon { name = "plus-lg" }, text " Add Beamtime" ]

            Just beamtime ->
                div_
                    [ viewEditForm beamtime
                    ]
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
                viewBeamtimes model.zone beamtimes

            Failure e ->
                div [] [ makeAlert [ AlertDanger ] [ showError e ] ]

            _ ->
                text "Loading..."
        ]
