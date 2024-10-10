module Amarcord.Pages.AnalysisExperimentType exposing (Model, Msg(..), init, subscriptions, update, view)

import Amarcord.API.ExperimentType exposing (ExperimentTypeId)
import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.Attributo exposing (Attributo, AttributoId, AttributoType, AttributoValue, attributoIsChemicalId, convertAttributoFromApi, convertAttributoMapFromApi, convertAttributoValueFromApi, prettyPrintAttributoValue)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, viewAlert)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Html exposing (br_, div_, li_, span_, tbody_, td_, th_, thead_, tr_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.Route exposing (Route(..), makeLink)
import Amarcord.Util exposing (HereAndNow, none)
import Api.Data exposing (JsonDataSet, JsonDataSetWithoutIndexingResults, JsonExperimentType, JsonReadAnalysisResults)
import Api.Request.Analysis exposing (readAnalysisResultsApiAnalysisAnalysisResultsBeamtimeIdExperimentTypeIdGet)
import Browser.Navigation as Nav
import Dict exposing (Dict)
import Dict.Extra
import Html exposing (Html, a, button, div, h4, input, label, li, nav, ol, span, table, td, text, th, tr, ul)
import Html.Attributes exposing (attribute, checked, class, colspan, for, href, style, type_)
import Html.Events exposing (onClick, onInput)
import List.Extra
import RemoteData exposing (RemoteData(..), fromResult)
import Set exposing (Set)
import String
import Time exposing (Posix)


subscriptions : Model -> List (Sub Msg)
subscriptions _ =
    [ Time.every 10000 Refresh ]


type Msg
    = AnalysisResultsReceived (Result HttpError JsonReadAnalysisResults)
    | Refresh Posix
    | SortDataSetsAscending
    | SetFilterExperimentTypeAttributo ExperimentTypeAttributoFilter String
    | SetFilterExperimentTypeAllAttributi (List ExperimentTypeAttributoFilter)
    | SetFilterExperimentTypeNoneAttributi (List ExperimentTypeAttributoFilter)


type alias ExperimentTypeAttributoFilter =
    { attrId : AttributoId, attrValue : AttributoValue }


type alias Model =
    { hereAndNow : HereAndNow
    , navKey : Nav.Key
    , analysisRequest : RemoteData HttpError JsonReadAnalysisResults
    , selectedExperimentTypeId : Int
    , hiddenExperimentTypeAttributiFilters : List ExperimentTypeAttributoFilter
    , dataSetsSortingAscending : Bool
    , beamtimeId : BeamtimeId
    }


init : Nav.Key -> HereAndNow -> BeamtimeId -> ExperimentTypeId -> ( Model, Cmd Msg )
init navKey hereAndNow beamtimeId etId =
    ( { hereAndNow = hereAndNow
      , navKey = navKey
      , analysisRequest = Loading
      , selectedExperimentTypeId = etId
      , hiddenExperimentTypeAttributiFilters = []
      , dataSetsSortingAscending = True
      , beamtimeId = beamtimeId
      }
    , send AnalysisResultsReceived
        (readAnalysisResultsApiAnalysisAnalysisResultsBeamtimeIdExperimentTypeIdGet
            beamtimeId
            etId
        )
    )


viewInner : Model -> JsonReadAnalysisResults -> Html Msg
viewInner model { attributi, chemicalIdToName, dataSets, experimentType } =
    viewResultsTableForSingleExperimentType
        model
        (List.map convertAttributoFromApi attributi)
        (List.foldr (\{ chemicalId, name } -> Dict.insert chemicalId name) Dict.empty chemicalIdToName)
        experimentType
        dataSets


viewDataSet :
    Model
    -> List (Attributo AttributoType)
    -> Dict Int String
    -> JsonDataSetWithoutIndexingResults
    -> List (Html Msg)
viewDataSet model attributi chemicalIdsToName { dataSet, runs } =
    [ tr []
        [ td_ [ text (String.fromInt dataSet.id) ]
        , td_
            [ viewDataSetTable attributi
                model.hereAndNow.zone
                chemicalIdsToName
                (convertAttributoMapFromApi dataSet.attributi)
                False
                False
                Nothing
            ]
        , td_ (List.intersperse br_ <| List.map text runs)
        ]
    , tr []
        [ td [ colspan 4 ]
            [ div [ class "mb-5" ]
                [ a
                    [ href
                        (makeLink (AnalysisDataSet model.beamtimeId dataSet.id))
                    ]
                    [ text "→ Go to processing results" ]
                ]
            ]
        ]
    ]


viewResultsTableForSingleExperimentType :
    Model
    -> List (Attributo AttributoType)
    -> Dict Int String
    -> JsonExperimentType
    -> List JsonDataSetWithoutIndexingResults
    -> Html Msg
viewResultsTableForSingleExperimentType model attributi chemicalIdsToName experimentType dataSets =
    let
        experimentTypeAttributi : Set AttributoId
        experimentTypeAttributi =
            Set.fromList <| List.map .id experimentType.attributi

        attributoValueSelector : AttributoId -> AttributoValue -> String
        attributoValueSelector attrId attrValue =
            case List.Extra.find (\a -> a.id == attrId) attributi of
                Nothing ->
                    "what is this? " ++ String.fromInt attrId

                Just attr ->
                    if attributoIsChemicalId attr.type_ then
                        case String.toInt (prettyPrintAttributoValue attrValue) of
                            Nothing ->
                                prettyPrintAttributoValue attrValue

                            Just chemicalId ->
                                case Dict.get chemicalId chemicalIdsToName of
                                    Nothing ->
                                        prettyPrintAttributoValue attrValue

                                    Just ch ->
                                        ch

                    else
                        prettyPrintAttributoValue attrValue

        checkAttributeFilterIsPresent : ExperimentTypeAttributoFilter -> List ExperimentTypeAttributoFilter -> Bool
        checkAttributeFilterIsPresent { attrId, attrValue } hiddenFilters =
            List.member { attrId = attrId, attrValue = attrValue } hiddenFilters

        allNoneResetCheckButton : AttributoId -> List AttributoValue -> Html Msg
        allNoneResetCheckButton attributoId valuesForAttributo =
            let
                filterFromAttributoPair : AttributoId -> AttributoValue -> ExperimentTypeAttributoFilter
                filterFromAttributoPair aid avalue =
                    { attrId = aid, attrValue = avalue }

                attrFilters : List ExperimentTypeAttributoFilter
                attrFilters =
                    List.map
                        (filterFromAttributoPair attributoId)
                        valuesForAttributo
            in
            li_
                [ div [ class "dropdown-item btn-group" ]
                    [ button
                        [ type_ "button"
                        , class "btn btn-sm btn-outline-primary"
                        , onClick (SetFilterExperimentTypeAllAttributi attrFilters)
                        ]
                        [ text "All" ]
                    , button
                        [ type_ "button"
                        , class "btn btn-sm btn-outline-secondary"
                        , onClick (SetFilterExperimentTypeNoneAttributi attrFilters)
                        ]
                        [ text "None" ]
                    ]
                ]

        checkboxForOneAttributoValue : ( AttributoId, AttributoValue ) -> Html Msg
        checkboxForOneAttributoValue ( attrId, attrValue ) =
            let
                selectedAttributoValue =
                    { attrId = attrId, attrValue = attrValue }
            in
            li_
                [ div [ class "dropdown-item" ]
                    [ div [ class "form-check" ]
                        [ input
                            [ class "form-check-input"
                            , type_ "checkbox"
                            , for (String.fromInt attrId)
                            , onInput (SetFilterExperimentTypeAttributo selectedAttributoValue)
                            , checked <| not <| checkAttributeFilterIsPresent selectedAttributoValue model.hiddenExperimentTypeAttributiFilters
                            ]
                            []
                        , label [ class "form-check-label" ] [ text <| attributoValueSelector attrId attrValue ]
                        ]
                    ]
                ]

        dataSetMatchesAttributoFilter : JsonDataSet -> ExperimentTypeAttributoFilter -> Bool
        dataSetMatchesAttributoFilter ds { attrId, attrValue } =
            case List.Extra.find (\{ attributoId } -> attributoId == attrId) ds.attributi of
                -- can't find the attributo in the data set
                Nothing ->
                    False

                Just attributoInDs ->
                    attrValue == convertAttributoValueFromApi attributoInDs

        dataSetMatchesAttributiFilters : JsonDataSetWithoutIndexingResults -> Bool
        dataSetMatchesAttributiFilters { dataSet } =
            -- A little subltety here: we define the _hidden_
            -- attributi, so we want to match all data sets where
            -- _none_ of the filters match for it to be _not_ hidden.
            none (dataSetMatchesAttributoFilter dataSet) model.hiddenExperimentTypeAttributiFilters

        dictMapValues : (b -> c) -> Dict a b -> Dict a c
        dictMapValues f =
            Dict.map (\_ -> f)

        dictAttrIdValues : Dict AttributoId (List AttributoValue)
        dictAttrIdValues =
            List.concatMap (Dict.toList << convertAttributoMapFromApi << .attributi << .dataSet) dataSets
                |> List.Extra.unique
                |> Dict.Extra.groupBy Tuple.first
                |> dictMapValues (List.map Tuple.second)

        dropdownForAttributo : Attributo AttributoType -> Html Msg
        dropdownForAttributo attributo =
            let
                attributoValueCheckBoxes : AttributoId -> List AttributoValue -> List (Html Msg)
                attributoValueCheckBoxes attrName attrValues =
                    List.map (\v -> checkboxForOneAttributoValue ( attrName, v )) attrValues
            in
            case Dict.get attributo.id dictAttrIdValues of
                Nothing ->
                    span_ []

                Just listValues ->
                    let
                        checkBoxes =
                            attributoValueCheckBoxes attributo.id listValues
                    in
                    if List.length checkBoxes > 1 then
                        span [ class "dropdown px-1" ]
                            [ button
                                [ class "btn btn-sm  btn-outline-secondary dropdown-toggle dropdown-toggle-split"
                                , attribute "data-bs-toggle" "dropdown"
                                , attribute "data-bs-auto-close" "outside"
                                ]
                                [ text (attributo.name ++ " ") ]
                            , ul [ class "dropdown-menu" ] <|
                                allNoneResetCheckButton attributo.id listValues
                                    :: checkBoxes
                            ]

                    else
                        span_ []

        attributiFilters : List (Html Msg)
        attributiFilters =
            List.map dropdownForAttributo <| List.filterMap (\aid -> List.Extra.find (\a -> a.id == aid) attributi) <| Set.toList experimentTypeAttributi
    in
    div_
        [ nav []
            [ ol [ class "breadcrumb" ]
                [ li [ class "breadcrumb-item active" ] [ text "/ ", a [ href (makeLink (AnalysisOverview model.beamtimeId)) ] [ text "Analysis Overview" ] ]
                , li [ class "breadcrumb-item active" ] [ a [ href (makeLink (AnalysisExperimentType model.beamtimeId experimentType.id)) ] [ text experimentType.name ] ]
                ]
            ]
        , div
            [ class "pb-3"
            , style "border-bottom" "1pt solid lightgray"
            ]
          <|
            label [ style "width" "10rem" ] [ icon { name = "card-list" }, text " Attributi Filter " ]
                :: attributiFilters
        , table [ class "table amarcord-table-fix-head table-borderless" ]
            [ thead_
                [ tr_
                    [ th [ onClick SortDataSetsAscending ]
                        [ text "ID"
                        , icon
                            { name =
                                if model.dataSetsSortingAscending then
                                    "caret-down-fill"

                                else
                                    "caret-up-fill"
                            }
                        ]
                    , th_ [ text "Attributi" ]
                    , th_ [ text "Runs" ]
                    ]
                ]
            , let
                filteredDataSets : List JsonDataSetWithoutIndexingResults
                filteredDataSets =
                    List.filter dataSetMatchesAttributiFilters dataSets

                sortedDatasets =
                    List.sortWith (\a b -> compare a.dataSet.id b.dataSet.id) filteredDataSets

                requestedSortedDatasets : List JsonDataSetWithoutIndexingResults
                requestedSortedDatasets =
                    if model.dataSetsSortingAscending then
                        List.reverse <| sortedDatasets

                    else
                        sortedDatasets
              in
              tbody_ <|
                List.concatMap
                    (viewDataSet
                        model
                        attributi
                        chemicalIdsToName
                    )
                    requestedSortedDatasets
            ]
        ]


view : Model -> Html Msg
view model =
    div [ class "container" ] <|
        case model.analysisRequest of
            NotAsked ->
                List.singleton <| text ""

            Loading ->
                List.singleton <| loadingBar "Loading analysis results..."

            Failure e ->
                List.singleton <|
                    viewAlert [ AlertDanger ] <|
                        [ h4 [ class "alert-heading" ]
                            [ text "Failed to retrieve analysis results. Try reloading and if that doesn't work, contact the admins"
                            ]
                        , showError e
                        ]

            Success r ->
                [ viewInner model r
                ]


possiblyRefresh : Model -> Cmd Msg
possiblyRefresh model =
    send AnalysisResultsReceived
        (readAnalysisResultsApiAnalysisAnalysisResultsBeamtimeIdExperimentTypeIdGet
            model.beamtimeId
            model.selectedExperimentTypeId
        )


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        AnalysisResultsReceived analysisResults ->
            ( { model | analysisRequest = fromResult analysisResults }, Cmd.none )

        Refresh posix ->
            let
                newHereAndNow =
                    { now = posix, zone = model.hereAndNow.zone }
            in
            ( { model | hereAndNow = newHereAndNow }
            , possiblyRefresh model
            )

        SortDataSetsAscending ->
            let
                dss =
                    model.dataSetsSortingAscending
            in
            ( { model | dataSetsSortingAscending = not dss }, Cmd.none )

        SetFilterExperimentTypeAttributo attrFilter _ ->
            let
                hlfs =
                    model.hiddenExperimentTypeAttributiFilters
            in
            if List.member attrFilter model.hiddenExperimentTypeAttributiFilters then
                ( { model
                    | hiddenExperimentTypeAttributiFilters =
                        List.Extra.unique <|
                            List.Extra.remove attrFilter hlfs
                  }
                , Cmd.none
                )

            else
                ( { model | hiddenExperimentTypeAttributiFilters = List.Extra.unique (attrFilter :: hlfs) }, Cmd.none )

        SetFilterExperimentTypeAllAttributi attrFilters ->
            let
                hlfs =
                    model.hiddenExperimentTypeAttributiFilters

                newHlfs =
                    List.filter (\af -> List.Extra.notMember af attrFilters) hlfs
            in
            ( { model | hiddenExperimentTypeAttributiFilters = List.Extra.unique <| newHlfs }, Cmd.none )

        SetFilterExperimentTypeNoneAttributi attrFilters ->
            let
                hlfs =
                    model.hiddenExperimentTypeAttributiFilters

                newHlfs =
                    List.Extra.unique (hlfs ++ attrFilters)
            in
            ( { model | hiddenExperimentTypeAttributiFilters = List.Extra.unique <| newHlfs }
            , Cmd.none
            )
