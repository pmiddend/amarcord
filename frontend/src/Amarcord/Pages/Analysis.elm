module Amarcord.Pages.Analysis exposing (Model, Msg, init, update, view)

import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoName, AttributoType, AttributoValue, attributoDecoder, attributoMapDecoder, attributoRequestDecoder, attributoTypeDecoder)
import Amarcord.AttributoHtml exposing (formatFloatHumanFriendly, viewAttributoCell)
import Amarcord.Bootstrap exposing (AlertProperty(..), loadingBar, makeAlert, showHttpError)
import Amarcord.File exposing (File)
import Amarcord.Html exposing (div_, form_, h1_, h2_, h3_, input_, tbody_, td_, th_, thead_, tr_)
import Amarcord.Sample exposing (Sample, sampleDecoder, sampleIdDict)
import Amarcord.Util exposing (HereAndNow)
import Dict exposing (Dict)
import Html exposing (Html, button, div, form, h4, hr, label, p, table, td, text)
import Html.Attributes exposing (checked, class, disabled, for, id, style, type_)
import Html.Events exposing (onClick, onInput)
import Http exposing (Error(..), Response(..), emptyBody, jsonBody, stringResolver)
import Json.Decode as Decode
import Json.Decode.Pipeline exposing (required)
import Json.Encode as Encode
import List.Extra as ListExtra
import RemoteData exposing (RemoteData(..), fromResult, isLoading)
import String exposing (join)
import Task


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


type alias InitialRequestResponse =
    { attributi : List (Attributo AttributoType)
    , results : List CfelAnalysisResult
    }


type Msg
    = InitialRequest (Result Http.Error InitialRequestResponse)
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
    , initialRequest : RemoteData Http.Error InitialRequestResponse
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


httpResponseToResult : (body -> Result String a) -> Response body -> Result Http.Error a
httpResponseToResult toResult response =
    case response of
        BadUrl_ url ->
            Err (BadUrl url)

        Timeout_ ->
            Err Timeout

        NetworkError_ ->
            Err NetworkError

        BadStatus_ metadata _ ->
            Err (BadStatus metadata.statusCode)

        GoodStatus_ _ body ->
            Result.mapError BadBody (toResult body)


jsonResolver : Decode.Decoder a -> Http.Resolver Http.Error a
jsonResolver decoder =
    stringResolver (httpResponseToResult (Result.mapError Decode.errorToString << Decode.decodeString decoder))


httpJsonTask : String.String -> String.String -> Decode.Decoder a -> Task.Task Error a
httpJsonTask method url decoder =
    Http.task
        { method = method
        , headers = []
        , url = url
        , body = emptyBody
        , resolver = jsonResolver decoder
        , timeout = Nothing
        }


type alias CfelAnalysisResult =
    { directoryName : String
    , runFrom : Int
    , runTo : Int
    , resolution : String
    , rsplit : Float
    , cchalf : Float
    , ccstar : Float
    , snr : Float
    , completeness : Float
    , multiplicity : Float
    , totalMeasurements : Int
    , uniqueReflections : Int
    , wilsonB : Float
    , outerShell : String
    , numPatterns : Int
    , numHits : Int
    , indexedPatterns : Int
    , indexedCrystals : Int
    , comment : String
    }


cfelAnalysisDecoder : Decode.Decoder CfelAnalysisResult
cfelAnalysisDecoder =
    Decode.succeed CfelAnalysisResult
        |> required "directoryName" Decode.string
        |> required "runFrom" Decode.int
        |> required "runTo" Decode.int
        |> required "resolution" Decode.string
        |> required "rsplit" Decode.float
        |> required "cchalf" Decode.float
        |> required "ccstar" Decode.float
        |> required "snr" Decode.float
        |> required "completeness" Decode.float
        |> required "multiplicity" Decode.float
        |> required "totalMeasurements" Decode.int
        |> required "uniqueReflections" Decode.int
        |> required "wilsonB" Decode.float
        |> required "outerShell" Decode.string
        |> required "numPatterns" Decode.int
        |> required "numHits" Decode.int
        |> required "indexedPatterns" Decode.int
        |> required "indexedCrystals" Decode.int
        |> required "comment" Decode.string


cfelResultsDecoder : Decode.Decoder (List CfelAnalysisResult)
cfelResultsDecoder =
    Decode.field "results" (Decode.list cfelAnalysisDecoder)


init : HereAndNow -> ( Model, Cmd Msg )
init hereAndNow =
    ( { hereAndNow = hereAndNow
      , initialRequest = Loading
      , selectedAttributi = []
      , groupedRunsRequest = NotAsked
      }
    , Task.attempt InitialRequest <|
        Task.map2
            InitialRequestResponse
            (httpJsonTask "GET" "/api/attributi" attributoRequestDecoder)
            (httpJsonTask "GET" "/api/analysis/cfel-results" cfelResultsDecoder)
    )


viewInner : Model -> List (Attributo AttributoType) -> List CfelAnalysisResult -> List (Html Msg)
viewInner model attributi results =
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
    [ div [ class "row" ]
        [ div [ class "col-6" ]
            [ h3_ [ text "Select attributi" ]
            , form [ class "mb-3" ]
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
    , viewResultsTable results
    ]


viewResultsTable : List CfelAnalysisResult -> Html msg
viewResultsTable results =
    let
        headerNames =
            [ "Runs"
            , "Directory"
            , "Resolution"
            , "RSplit"
            , "CCHalf"
            , "CC*"
            , "SNR"
            , "Completeness"
            , "Multiplicity"
            , "Total Measurements"
            , "Unique Reflections"
            , "Wilson B"
            , "Outer Shell"
            , "Num. patterns"
            , "Num Hits"
            , "Indexed Patterns"
            , "Indexed Crystals"
            , "Comment"
            ]

        textTd =
            td_ << List.singleton << text

        floatTd =
            textTd << formatFloatHumanFriendly

        intTd =
            textTd << String.fromInt

        viewResultRow : CfelAnalysisResult -> Html msg
        viewResultRow { directoryName, runFrom, runTo, resolution, rsplit, cchalf, ccstar, snr, completeness, multiplicity, totalMeasurements, uniqueReflections, wilsonB, outerShell, numPatterns, numHits, indexedPatterns, indexedCrystals, comment } =
            tr_
                [ td [ style "white-space" "nowrap" ] [ text <| String.fromInt runFrom ++ "-" ++ String.fromInt runTo ]
                , textTd directoryName
                , textTd resolution
                , floatTd rsplit
                , floatTd cchalf
                , floatTd ccstar
                , floatTd snr
                , floatTd completeness
                , floatTd multiplicity
                , intTd totalMeasurements
                , intTd uniqueReflections
                , floatTd wilsonB
                , textTd outerShell
                , intTd numPatterns
                , intTd numHits
                , intTd indexedPatterns
                , intTd indexedCrystals
                , textTd comment
                ]
    in
    div_
        [ h2_ [ text "Results table" ]
        , table [ class "table table-striped table-sm" ]
            [ thead_ [ tr_ (List.map (th_ << List.singleton << text) headerNames) ]
            , tbody_ (List.map viewResultRow results)
            ]
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
    div [ class "container" ] <|
        case model.initialRequest of
            NotAsked ->
                List.singleton <| text ""

            Loading ->
                List.singleton <| loadingBar "Loading attributi and table..."

            Failure e ->
                List.singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve Attributi" ] ] ++ showHttpError e

            Success { attributi, results } ->
                viewInner model attributi results


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        InitialRequest attributi ->
            ( { model | initialRequest = fromResult attributi }, Cmd.none )

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
