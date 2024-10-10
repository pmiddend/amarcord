module Amarcord.Pages.AnalysisOverview exposing (Model, Msg(..), init, subscriptions, update, view)

import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.Bootstrap exposing (AlertProperty(..), loadingBar, viewAlert)
import Amarcord.Html exposing (br_, div_, hr_, li_, span_, strongText, ul_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.Route exposing (Route(..), makeLink)
import Amarcord.Util exposing (HereAndNow)
import Api.Data exposing (JsonExperimentType, JsonReadExperimentTypes)
import Api.Request.Experimenttypes exposing (readExperimentTypesApiExperimentTypesBeamtimeIdGet)
import Browser.Navigation as Nav
import Dict exposing (Dict)
import Html exposing (Html, a, div, h4, li, nav, ol, text)
import Html.Attributes exposing (class, href)
import List.Extra
import RemoteData exposing (RemoteData(..), fromResult)
import String
import Time exposing (Posix)


subscriptions : Model -> List (Sub Msg)
subscriptions _ =
    [ Time.every 10000 Refresh ]


type Msg
    = ExperimentTypesReceived (Result HttpError JsonReadExperimentTypes)
    | Refresh Posix


type alias Model =
    { hereAndNow : HereAndNow
    , navKey : Nav.Key
    , experimentTypesRequest : RemoteData HttpError JsonReadExperimentTypes
    , beamtimeId : BeamtimeId
    }


init : Nav.Key -> HereAndNow -> BeamtimeId -> ( Model, Cmd Msg )
init navKey hereAndNow beamtimeId =
    ( { hereAndNow = hereAndNow
      , navKey = navKey
      , experimentTypesRequest = Loading
      , beamtimeId = beamtimeId
      }
    , send ExperimentTypesReceived (readExperimentTypesApiExperimentTypesBeamtimeIdGet beamtimeId)
    )


viewExperimentTypeFilter : BeamtimeId -> JsonReadExperimentTypes -> List (Html Msg)
viewExperimentTypeFilter beamtimeId r =
    let
        attributoIdToName : Dict Int String
        attributoIdToName =
            List.foldr (\newElement -> Dict.insert newElement.id newElement.name) Dict.empty r.attributi

        attributoNamesForEt : JsonExperimentType -> List String
        attributoNamesForEt { attributi } =
            List.filterMap (\a -> Dict.get a.id attributoIdToName) attributi

        etNumberOfRuns : JsonExperimentType -> Int
        etNumberOfRuns inputEt =
            List.Extra.find (\et -> et.id == inputEt.id) r.experimentTypeIdToRun |> Maybe.map (\{ numberOfRuns } -> numberOfRuns) |> Maybe.withDefault 0

        makeRadio : JsonExperimentType -> Html Msg
        makeRadio et =
            let
                numberOfRuns =
                    etNumberOfRuns et
            in
            div [ class "form-check" ]
                [ a [ href (makeLink (AnalysisExperimentType beamtimeId et.id)) ] [ text et.name ]
                , div [ class "form-text" ]
                    [ if r.currentExperimentTypeId == Just et.id then
                        span_ [ strongText "Currently selected", br_ ]

                      else
                        text ""
                    , text ("Number of runs: " ++ String.fromInt numberOfRuns)
                    , br_
                    , text "Attributes:"
                    , ul_ (List.map (\name -> li_ [ text name ]) (List.sort (attributoNamesForEt et)))
                    ]
                ]
    in
    [ nav []
        [ ol [ class "breadcrumb" ]
            [ li [ class "breadcrumb-item active" ] [ text "/ ", a [ href (makeLink (AnalysisOverview beamtimeId)) ] [ text "Analysis Overview" ] ]
            ]
        ]
    , div_
        (List.map
            (\group ->
                div
                    [ class "row" ]
                    (List.map (\groupElement -> div [ class "col-6" ] [ groupElement ]) group)
            )
         <|
            List.Extra.groupsOf 2 (List.map makeRadio (List.sortBy (\et -> et.name) r.experimentTypes))
        )
    , hr_
    ]


view : Model -> Html Msg
view model =
    div [ class "container" ] <|
        case model.experimentTypesRequest of
            NotAsked ->
                List.singleton <| text ""

            Loading ->
                List.singleton <| loadingBar "Loading experiment types..."

            Failure e ->
                List.singleton <|
                    viewAlert [ AlertDanger ] <|
                        [ h4 [ class "alert-heading" ]
                            [ text "Failed to retrieve experiment types. Try reloading and if that doesn't work, contact the admins." ]
                        , showError e
                        ]

            Success experimentTypeResult ->
                viewExperimentTypeFilter model.beamtimeId experimentTypeResult


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        ExperimentTypesReceived experimentTypes ->
            ( { model | experimentTypesRequest = fromResult experimentTypes }
            , Cmd.none
            )

        Refresh _ ->
            ( model, Cmd.none )
