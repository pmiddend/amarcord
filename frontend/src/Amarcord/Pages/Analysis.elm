module Amarcord.Pages.Analysis exposing (Model, Msg, init, update, view)

import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo
    exposing
        ( Attributo
        , AttributoMap
        , AttributoName
        , AttributoType
        , AttributoValue
        , attributoDecoder
        , attributoMapDecoder
        , attributoTypeDecoder
        , httpGetAndDecodeAttributi
        )
import Amarcord.AttributoHtml exposing (viewAttributoCell)
import Amarcord.Bootstrap exposing (AlertProperty(..), loadingBar, makeAlert, showHttpError)
import Amarcord.File exposing (File)
import Amarcord.Html exposing (form_, h3_, input_, tbody_, td_, th_, thead_, tr_)
import Amarcord.Sample exposing (Sample, sampleDecoder, sampleIdDict)
import Amarcord.Util exposing (HereAndNow)
import Dict exposing (Dict)
import Html exposing (Html, button, div, h4, label, p, table, text)
import Html.Attributes exposing (checked, class, disabled, for, id, type_)
import Html.Events exposing (onClick, onInput)
import Http exposing (jsonBody)
import Json.Decode as Decode
import Json.Encode as Encode
import List.Extra as ListExtra
import RemoteData exposing (RemoteData(..), fromResult, isLoading)
import String exposing (join)


type alias GroupedRun =
    { runIds : List Int
    , totalMinutes : Int
    , attributi : AttributoMap AttributoValue
    }


groupedRunDecoder : Decode.Decoder GroupedRun
groupedRunDecoder =
    Decode.map3
        GroupedRun
        (Decode.field "runIds" (Decode.list Decode.int))
        (Decode.field "totalMinutes" Decode.int)
        (Decode.field "attributi" attributoMapDecoder)


type Msg
    = AttributiReceived (Result Http.Error (List (Attributo AttributoType)))
    | ToggleAttributo (Attributo AttributoType)
    | GroupedRunsReceived (Result Http.Error GroupedRunResponse)
    | GroupedRunsSubmit


type alias GroupedRunResponse =
    { groups : List GroupedRun
    , samples : List (Sample Int (AttributoMap AttributoValue) File)
    , attributi : List (Attributo AttributoType)
    }


type alias Model =
    { hereAndNow : HereAndNow
    , attributiRequest : RemoteData Http.Error (List (Attributo AttributoType))
    , selectedAttributi : List (Attributo AttributoType)
    , groupedRunsRequest : RemoteData Http.Error GroupedRunResponse
    }


httpGetGroupedRuns : List AttributoName -> Cmd Msg
httpGetGroupedRuns attributi =
    Http.post
        { url = "/api/analysis/grouped-runs"
        , expect =
            Http.expectJson GroupedRunsReceived <|
                Decode.map3 GroupedRunResponse
                    (Decode.field "groups" <| Decode.list groupedRunDecoder)
                    (Decode.field "samples" <| Decode.list sampleDecoder)
                    (Decode.field "attributi" <| Decode.list (attributoDecoder attributoTypeDecoder))
        , body = jsonBody (Encode.object [ ( "attributi", Encode.list Encode.string attributi ) ])
        }


init : HereAndNow -> ( Model, Cmd Msg )
init hereAndNow =
    ( { hereAndNow = hereAndNow
      , attributiRequest = Loading
      , selectedAttributi = []
      , groupedRunsRequest = NotAsked
      }
    , httpGetAndDecodeAttributi AttributiReceived
    )


viewInner : Model -> List (Attributo AttributoType) -> Html Msg
viewInner model attributi =
    let
        viewCheckbox : Attributo AttributoType -> Html Msg
        viewCheckbox a =
            div [ class "form-check" ]
                [ input_
                    [ class "form-check-input"
                    , type_ "checkbox"
                    , id ("checkbox-" ++ a.name)
                    , checked (List.any (\selected -> selected.name == a.name) model.selectedAttributi)
                    , onInput (always <| ToggleAttributo a)
                    ]
                , label [ for ("checkbox-" ++ a.name) ] [ text a.name ]
                ]

        checkboxes =
            List.map viewCheckbox (List.filter (\a -> a.associatedTable == AssociatedTable.Run) attributi)
    in
    div [ class "row" ]
        [ div [ class "col-6" ]
            [ h3_ [ text "Select attributi" ]
            , form_
                (checkboxes
                    ++ [ button
                            [ type_ "button"
                            , class "btn btn-primary mt-3"
                            , disabled (isLoading model.groupedRunsRequest)
                            , onClick GroupedRunsSubmit
                            ]
                            [ text "Update" ]
                       ]
                )
            ]
        , div [ class "col-6" ] (viewResults model)
        ]


viewRunIds : List Int -> String
viewRunIds =
    let
        groupToString : ( Int, List Int ) -> String
        groupToString ( x, xs ) =
            case ListExtra.last xs of
                Just last ->
                    String.fromInt x ++ "-" ++ String.fromInt last

                Nothing ->
                    String.fromInt x
    in
    join "," << List.map groupToString << ListExtra.groupWhile (\x y -> x == y - 1) << List.sort


viewResults : Model -> List (Html Msg)
viewResults model =
    let
        viewAttributoHeader : Attributo AttributoType -> Html msg
        viewAttributoHeader a =
            th_ [ text a.name ]

        attributiHeader : List (Html msg)
        attributiHeader =
            List.map viewAttributoHeader model.selectedAttributi

        viewGroup : Dict Int String -> List (Attributo AttributoType) -> GroupedRun -> Html Msg
        viewGroup sampleIds attributi g =
            tr_ <|
                td_ [ text <| viewRunIds g.runIds ]
                    :: td_ [ text <| String.fromInt g.totalMinutes ]
                    :: List.map (viewAttributoCell { shortDateTime = True } model.hereAndNow.zone sampleIds g.attributi) model.selectedAttributi
    in
    case model.groupedRunsRequest of
        Loading ->
            List.singleton <| loadingBar "Loading results..."

        NotAsked ->
            [ h3_ [ text "Results" ]
            , p [ class "lead" ] [ text "Choose some attributi and press the update button..." ]
            ]

        Failure e ->
            List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve samples" ] ] ++ showHttpError e

        Success groupedRunResponse ->
            [ h3_ [ text "Results" ]
            , table [ class "table" ]
                [ thead_ [ tr_ <| th_ [ text "Runs" ] :: th_ [ text "Total time [min]" ] :: attributiHeader ]
                , tbody_ (List.map (viewGroup (sampleIdDict groupedRunResponse.samples) groupedRunResponse.attributi) groupedRunResponse.groups)
                ]
            ]


view : Model -> Html Msg
view model =
    div [ class "container" ]
        [ case model.attributiRequest of
            NotAsked ->
                text ""

            Loading ->
                loadingBar "Loading attributi..."

            Failure e ->
                makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve Attributi" ] ] ++ showHttpError e

            Success attributi ->
                viewInner model attributi
        ]


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        AttributiReceived attributi ->
            ( { model | attributiRequest = fromResult attributi }, Cmd.none )

        ToggleAttributo a ->
            ( { model
                | selectedAttributi =
                    if List.any (\o -> o.name == a.name) model.selectedAttributi then
                        List.filter (\o -> o.name /= a.name) model.selectedAttributi

                    else
                        a :: model.selectedAttributi
              }
            , Cmd.none
            )

        GroupedRunsReceived result ->
            case result of
                Err e ->
                    ( { model | groupedRunsRequest = Failure e }, Cmd.none )

                Ok groupedRuns ->
                    ( { model | groupedRunsRequest = Success groupedRuns }, Cmd.none )

        GroupedRunsSubmit ->
            ( { model | groupedRunsRequest = Loading }, httpGetGroupedRuns (List.map .name model.selectedAttributi) )
