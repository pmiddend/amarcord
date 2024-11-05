module Amarcord.Pages.AnalysisOverview exposing (Model, Msg(..), init, pageTitle, subscriptions, update, view)

import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.Attributo exposing (Attributo, AttributoId, AttributoMap, AttributoType, AttributoValue, ChemicalNameDict, attributoValueToJson, convertAttributoFromApi, convertAttributoMapFromApi, convertAttributoValueFromApi, prettyPrintAttributoValue)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, viewAlert)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Html exposing (div_, h4_, li_, tbody_, td_, th_, thead_, tr_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.MultiDict as MultiDict exposing (MultiDict)
import Amarcord.Route exposing (Route(..), makeLink)
import Amarcord.Util exposing (HereAndNow)
import Api.Data exposing (JsonDataSet, JsonExperimentType, JsonReadNewAnalysisOutput)
import Api.Request.Analysis exposing (readAnalysisResultsApiAnalysisAnalysisResultsBeamtimeIdPost)
import AssocSet
import Browser.Navigation as Nav
import Dict
import Html exposing (Html, a, button, div, h4, input, label, li, nav, small, span, table, td, text, ul)
import Html.Attributes exposing (attribute, checked, class, colspan, disabled, href, id, style, type_)
import Html.Events exposing (onClick, onInput)
import RemoteData exposing (RemoteData(..), fromResult)
import Scroll exposing (scrollY)
import String
import Task
import Time exposing (Posix)


subscriptions : Model -> List (Sub Msg)
subscriptions _ =
    [ Time.every 10000 Refresh ]


type alias AttributoValueSet =
    AssocSet.Set AttributoValue


type Msg
    = ExperimentTypesReceived (Result HttpError JsonReadNewAnalysisOutput)
    | Refresh Posix
    | UpdateAttributoValueFilter (Attributo AttributoType) (AttributoValueSet -> AttributoValueSet)
    | UpdateFilter
    | Scroll String
    | Nop


type alias AttributoWithValues =
    { attributo : Attributo AttributoType
    , values : AttributoValueSet
    }


type alias Model =
    { hereAndNow : HereAndNow
    , navKey : Nav.Key
    , experimentTypesRequest : RemoteData HttpError JsonReadNewAnalysisOutput
    , beamtimeId : BeamtimeId
    , attributoValueFilters : MultiDict AttributoId AttributoValue
    }


pageTitle : Model -> String
pageTitle _ =
    "Analysis Overview"


init : Nav.Key -> HereAndNow -> BeamtimeId -> ( Model, Cmd Msg )
init navKey hereAndNow beamtimeId =
    ( { hereAndNow = hereAndNow
      , navKey = navKey
      , experimentTypesRequest = Loading
      , beamtimeId = beamtimeId
      , attributoValueFilters = MultiDict.empty
      }
    , send ExperimentTypesReceived (readAnalysisResultsApiAnalysisAnalysisResultsBeamtimeIdPost beamtimeId { attributiFilter = [] })
    )


viewExperimentTypeHeading : MultiDict Int JsonDataSet -> JsonExperimentType -> Html Msg
viewExperimentTypeHeading byExperimentType { id, name } =
    li
        [ class
            "nav-item"
        ]
        [ button
            [ onClick (Scroll ("et-" ++ String.fromInt id))
            , class "btn btn-link w-100"
            , type_ "button"
            , disabled (not (MultiDict.member id byExperimentType))
            ]
            [ text name ]
        ]


buildAttributiWithValues : List ( AttributoId, AttributoValue ) -> List (Attributo AttributoType) -> List AttributoWithValues
buildAttributiWithValues attributoValues attributoTypes =
    let
        attributiTypeDict : AttributoMap (Attributo AttributoType)
        attributiTypeDict =
            List.foldr (\newType -> Dict.insert newType.id newType) Dict.empty attributoTypes

        attributoValuesDict : MultiDict AttributoId AttributoValue
        attributoValuesDict =
            List.foldr
                (\( newAttributoId, newAttributoValue ) -> MultiDict.insert newAttributoId newAttributoValue)
                MultiDict.empty
                attributoValues

        foldMultidict : AttributoId -> AttributoValueSet -> List AttributoWithValues -> List AttributoWithValues
        foldMultidict aid values prevValues =
            case Dict.get aid attributiTypeDict of
                Nothing ->
                    prevValues

                Just atype ->
                    { attributo = atype, values = values } :: prevValues
    in
    MultiDict.foldr foldMultidict [] attributoValuesDict


buildAttributiWithValuesFromRequest : JsonReadNewAnalysisOutput -> List AttributoWithValues
buildAttributiWithValuesFromRequest { attributi, attributiValues } =
    let
        convertedAttributiValues : List ( AttributoId, AttributoValue )
        convertedAttributiValues =
            List.map (\jsonValue -> ( jsonValue.attributoId, convertAttributoValueFromApi jsonValue )) attributiValues

        convertedAttributiTypes : List (Attributo AttributoType)
        convertedAttributiTypes =
            List.map convertAttributoFromApi attributi
    in
    buildAttributiWithValues convertedAttributiValues convertedAttributiTypes


viewCheckboxForOneAttributoValue : ChemicalNameDict -> Attributo AttributoType -> AssocSet.Set AttributoValue -> AttributoValue -> Html Msg
viewCheckboxForOneAttributoValue chemicalIdsToName attributo selectedValues valueForCheckbox =
    li_
        [ div [ class "dropdown-item" ]
            [ div [ class "form-check" ]
                [ input
                    [ class "form-check-input"
                    , type_ "checkbox"
                    , onInput
                        (always <|
                            UpdateAttributoValueFilter attributo
                                (\setBefore ->
                                    if AssocSet.member valueForCheckbox setBefore then
                                        AssocSet.remove valueForCheckbox setBefore

                                    else
                                        AssocSet.insert valueForCheckbox setBefore
                                )
                        )
                    , checked <| AssocSet.member valueForCheckbox selectedValues
                    ]
                    []
                , label [ class "form-check-label" ]
                    [ text (prettyPrintAttributoValue (Just chemicalIdsToName) valueForCheckbox)
                    ]
                ]
            ]
        ]


viewAllNoneResetCheckButton : Attributo AttributoType -> AssocSet.Set AttributoValue -> Html Msg
viewAllNoneResetCheckButton attributo valuesForAttributo =
    li_
        [ div [ class "dropdown-item btn-group" ]
            [ button
                [ type_ "button"
                , class "btn btn-sm btn-outline-primary"
                , onClick (UpdateAttributoValueFilter attributo (always valuesForAttributo))
                ]
                [ text "All" ]
            , button
                [ type_ "button"
                , class "btn btn-sm btn-outline-secondary"
                , onClick (UpdateAttributoValueFilter attributo (always AssocSet.empty))
                ]
                [ text "None" ]
            ]
        ]


viewDropdownForAttributoValues : ChemicalNameDict -> AttributoValueSet -> AttributoWithValues -> Html Msg
viewDropdownForAttributoValues chemicalIdsToName selectedValues { attributo, values } =
    let
        viewValueCheckboxes : AssocSet.Set AttributoValue -> List (Html Msg)
        viewValueCheckboxes =
            List.map (viewCheckboxForOneAttributoValue chemicalIdsToName attributo selectedValues) << AssocSet.toList

        checkboxes =
            viewValueCheckboxes values
    in
    if List.length checkboxes > 1 then
        span [ class "dropdown px-1" ]
            [ button
                [ class "btn btn-sm  btn-outline-secondary dropdown-toggle dropdown-toggle-split"
                , attribute "data-bs-toggle" "dropdown"
                , attribute "data-bs-auto-close" "outside"
                ]
                [ text (attributo.name ++ " ") ]
            , ul [ class "dropdown-menu" ] <|
                viewAllNoneResetCheckButton attributo values
                    :: viewValueCheckboxes values
            ]

    else
        text ""


viewDataSet :
    Model
    -> List (Attributo AttributoType)
    -> ChemicalNameDict
    -> JsonDataSet
    -> List (Html Msg)
viewDataSet model attributi chemicalIdsToName dataSet =
    [ tr_
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
        ]
    , tr_
        [ td [ colspan 2 ]
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


viewFilterForm : MultiDict AttributoId AttributoValue -> JsonReadNewAnalysisOutput -> Html Msg
viewFilterForm attributoValueFilters request =
    let
        -- These will be _all_ attributi and values, whereas attributoValueFilters will only be the selected ones
        attributiWithValues =
            buildAttributiWithValuesFromRequest request

        chemicalIdsToName : ChemicalNameDict
        chemicalIdsToName =
            List.foldr (\{ chemicalId, name } -> Dict.insert chemicalId name) Dict.empty request.chemicalIdToName

        attributiFilters =
            List.map
                (\attributoWithValues ->
                    viewDropdownForAttributoValues
                        chemicalIdsToName
                        (MultiDict.get attributoWithValues.attributo.id attributoValueFilters)
                        attributoWithValues
                )
                attributiWithValues
    in
    div
        [ class "pb-3"
        , style "border-bottom" "1pt solid lightgray"
        ]
        [ h4_ [ icon { name = "card-list" }, text " Attributi Filter " ]
        , div [ class "form-text mb-2" ] [ small [] [ text "Note: Dropdowns will only be shown if there is more than one attributo value in a data set." ] ]
        , div_ attributiFilters
        , button
            [ class "btn btn-primary mt-2"
            , type_ "button"
            , onClick UpdateFilter
            ]
            [ text "Update" ]
        ]


viewDataSetForExperimentType : Model -> JsonReadNewAnalysisOutput -> JsonExperimentType -> AssocSet.Set JsonDataSet -> List (Html Msg)
viewDataSetForExperimentType model analysisResults et dataSets =
    if AssocSet.isEmpty dataSets then
        []

    else
        tr_ [ td [ colspan 2 ] [ h4 [ id ("et-" ++ String.fromInt et.id) ] [ text et.name ] ] ]
            :: List.concatMap
                (viewDataSet
                    model
                    (List.map convertAttributoFromApi analysisResults.attributi)
                    (List.foldr (\{ chemicalId, name } -> Dict.insert chemicalId name)
                        Dict.empty
                        analysisResults.chemicalIdToName
                    )
                )
                (AssocSet.toList dataSets)


viewDataSets : Model -> JsonReadNewAnalysisOutput -> MultiDict Int JsonDataSet -> Html Msg
viewDataSets model analysisResults byExperimentType =
    table [ class "table amarcord-table-fix-head table-borderless" ]
        [ thead_
            [ tr_
                [ th_
                    [ text "ID"
                    ]
                , th_ [ text "Attributi" ]
                ]
            ]
        , tbody_ <|
            List.concatMap
                (\et -> viewDataSetForExperimentType model analysisResults et (MultiDict.get et.id byExperimentType))
                analysisResults.experimentTypes
        ]


viewResults : Model -> JsonReadNewAnalysisOutput -> Html Msg
viewResults ({ attributoValueFilters } as model) analysisResults =
    let
        byExperimentType : MultiDict Int JsonDataSet
        byExperimentType =
            List.foldr (\newDataSet -> MultiDict.insert newDataSet.experimentTypeId newDataSet) MultiDict.empty analysisResults.filteredDataSets
    in
    div_
        [ viewFilterForm attributoValueFilters analysisResults
        , div [ class "row" ]
            [ div [ class "col-4" ]
                [ nav [ id "analysis-nav", class "navbar h-100 flex-column align-items-stretch pe-4 border-end" ]
                    [ ul [ class "nav nav-pills flex-column" ] (List.map (viewExperimentTypeHeading byExperimentType) analysisResults.experimentTypes) ]
                ]
            , div [ class "col-8" ]
                [ viewDataSets model analysisResults byExperimentType
                ]
            ]
        ]


view : Model -> Html Msg
view model =
    div [ class "container" ] <|
        case model.experimentTypesRequest of
            NotAsked ->
                List.singleton <| text ""

            Loading ->
                List.singleton <| loadingBar "Loading analysis…"

            Failure e ->
                List.singleton <|
                    viewAlert [ AlertDanger ] <|
                        [ h4 [ class "alert-heading" ]
                            [ text "Failed to retrieve analysis results. Try reloading and if that doesn't work, contact the admins." ]
                        , showError e
                        ]

            Success experimentTypeResult ->
                [ viewResults model experimentTypeResult ]


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        Nop ->
            ( model, Cmd.none )

        Scroll destination ->
            ( model, Task.attempt (always Nop) (scrollY destination 0 0) )

        ExperimentTypesReceived experimentTypes ->
            ( { model | experimentTypesRequest = fromResult experimentTypes }
            , Cmd.none
            )

        Refresh _ ->
            ( model, Cmd.none )

        UpdateAttributoValueFilter updateAttributo modifier ->
            let
                newFilters =
                    if MultiDict.member updateAttributo.id model.attributoValueFilters then
                        MultiDict.update updateAttributo.id modifier model.attributoValueFilters

                    else
                        AssocSet.foldr
                            (MultiDict.insert updateAttributo.id)
                            model.attributoValueFilters
                            (modifier AssocSet.empty)
            in
            ( { model | attributoValueFilters = newFilters }, Cmd.none )

        UpdateFilter ->
            ( model
            , send
                ExperimentTypesReceived
                (readAnalysisResultsApiAnalysisAnalysisResultsBeamtimeIdPost
                    model.beamtimeId
                    { attributiFilter =
                        MultiDict.foldr
                            (\attributoId attributoValues priorFilters ->
                                AssocSet.foldr (\newAttributoValue priorList -> attributoValueToJson attributoId newAttributoValue :: priorList) priorFilters attributoValues
                            )
                            []
                            model.attributoValueFilters
                    }
                )
            )
