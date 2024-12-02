module Amarcord.Pages.AnalysisOverview exposing (Model, Msg(..), init, pageTitle, subscriptions, update, view)

import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.Attributo exposing (Attributo, AttributoId, AttributoMap, AttributoType, AttributoValue(..), ChemicalNameDict, attributoValueToJson, convertAttributoFromApi, convertAttributoMapFromApi, convertAttributoValueFromApi, prettyPrintAttributoValue)
import Amarcord.AttributoHtml exposing (formatIntHumanFriendly)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, viewAlert)
import Amarcord.DataSetHtml exposing (viewDataSetTable)
import Amarcord.Html exposing (br_, div_, h4_, input_, li_, tbody_, td_, th_, thead_, tr_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.Indexing.Util exposing (viewFormCheck)
import Amarcord.MultiDict as MultiDict exposing (MultiDict)
import Amarcord.Route as Route exposing (MergeFilter(..), Route(..), makeLink)
import Amarcord.Util exposing (HereAndNow, formatPosixHumanFriendly)
import Api.Data exposing (JsonDataSet, JsonDataSetStatistics, JsonExperimentTypeWithBeamtimeInformation, JsonMergeStatus(..), JsonReadNewAnalysisOutput)
import Api.Request.Analysis exposing (readAnalysisResultsApiAnalysisAnalysisResultsPost)
import AssocSet
import Browser.Navigation as Nav
import Dict exposing (Dict)
import Html exposing (Html, a, button, div, em, h4, input, label, li, nav, p, small, span, table, td, text, ul)
import Html.Attributes exposing (attribute, checked, class, colspan, disabled, for, href, id, style, type_)
import Html.Events exposing (onClick, onInput)
import RemoteData exposing (RemoteData(..), fromResult)
import Scroll exposing (scrollY)
import String exposing (toLower)
import Task
import Time exposing (Posix, millisToPosix, posixToMillis)


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
    | ChangeMergeFilter MergeFilter
    | Nop
    | ToggleIgnoreBeamtimeId


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
    , searchAcrossBeamtimes : Bool
    , mergeFilter : MergeFilter
    }


pageTitle : Model -> String
pageTitle _ =
    "Analysis Overview"


{-| The 'Route' uses a list, while the 'Model' here uses MultiDict, so we need two conversion functions between the two.
-}
filterListToMultidict : List Route.AnalysisFilter -> MultiDict AttributoId AttributoValue
filterListToMultidict =
    List.foldr (\{ id, value } -> MultiDict.insert id value) MultiDict.empty


{-| The 'Route' uses a list, while the 'Model' here uses MultiDict, so we need two conversion functions between the two.
-}
multidictToFilterList : MultiDict AttributoId AttributoValue -> List Route.AnalysisFilter
multidictToFilterList =
    MultiDict.foldr (\key values oldList -> List.map (\value -> { id = key, value = value }) (AssocSet.toList values) ++ oldList) []


sendUpdateRequest : Maybe Int -> MergeFilter -> MultiDict Int AttributoValue -> Cmd Msg
sendUpdateRequest beamtimeId mergeFilter filters =
    send ExperimentTypesReceived
        (readAnalysisResultsApiAnalysisAnalysisResultsPost
            -- Here we have another conversion function, this time to the request data type from our 'MultiDict'
            { attributiFilter =
                MultiDict.foldr
                    (\attributoId attributoValues priorFilters ->
                        AssocSet.foldr (\newAttributoValue priorList -> attributoValueToJson attributoId newAttributoValue :: priorList) priorFilters attributoValues
                    )
                    []
                    filters
            , mergeStatus =
                case mergeFilter of
                    Unmerged ->
                        JsonMergeStatusUnmerged

                    Merged ->
                        JsonMergeStatusMerged

                    Both ->
                        JsonMergeStatusBoth
            , beamtimeId = beamtimeId
            }
        )


init : Nav.Key -> HereAndNow -> BeamtimeId -> List Route.AnalysisFilter -> Bool -> MergeFilter -> ( Model, Cmd Msg )
init navKey hereAndNow beamtimeId filters across mergeFilter =
    ( { hereAndNow = hereAndNow
      , navKey = navKey
      , experimentTypesRequest = Loading
      , beamtimeId = beamtimeId
      , attributoValueFilters = filterListToMultidict filters
      , searchAcrossBeamtimes = across
      , mergeFilter = mergeFilter
      }
    , sendUpdateRequest
        (if across then
            Nothing

         else
            Just beamtimeId
        )
        mergeFilter
        (filterListToMultidict filters)
    )


viewExperimentTypeHeading : MultiDict Int JsonDataSet -> JsonExperimentTypeWithBeamtimeInformation -> Html Msg
viewExperimentTypeHeading byExperimentType { experimentType, beamtime } =
    li
        [ class
            "nav-item"
        ]
        [ button
            [ onClick (Scroll ("et-" ++ String.fromInt experimentType.id))
            , class "btn btn-link w-100"
            , type_ "button"
            , disabled (not (MultiDict.member experimentType.id byExperimentType))
            ]
            [ text experimentType.name, br_, small [] [ em [] [ text beamtime.title ] ] ]
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
    List.sortBy (\a -> toLower a.attributo.name) <| MultiDict.foldr foldMultidict [] attributoValuesDict


buildAttributiWithValuesFromRequest : JsonReadNewAnalysisOutput -> List AttributoWithValues
buildAttributiWithValuesFromRequest { searchableAttributi, attributiValues } =
    let
        convertedAttributiValues : List ( AttributoId, AttributoValue )
        convertedAttributiValues =
            List.map (\jsonValue -> ( jsonValue.attributoId, convertAttributoValueFromApi jsonValue )) attributiValues

        convertedAttributiTypes : List (Attributo AttributoType)
        convertedAttributiTypes =
            List.map convertAttributoFromApi searchableAttributi
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
        attributoValueComparator : AttributoValue -> AttributoValue -> Order
        attributoValueComparator x y =
            case ( x, y ) of
                ( ValueInt i, ValueInt j ) ->
                    compare i j

                ( ValueChemical i, ValueChemical j ) ->
                    compare (Maybe.withDefault "" (Dict.get i chemicalIdsToName)) (Maybe.withDefault "" (Dict.get j chemicalIdsToName))

                ( ValueDateTime i, ValueDateTime j ) ->
                    compare (posixToMillis i) (posixToMillis j)

                ( ValueString i, ValueString j ) ->
                    compare i j

                ( ValueNumber i, ValueNumber j ) ->
                    compare i j

                ( ValueBoolean i, ValueBoolean j ) ->
                    if i == j then
                        EQ

                    else if i == False then
                        LT

                    else
                        GT

                _ ->
                    EQ

        viewValueCheckboxes : AssocSet.Set AttributoValue -> List (Html Msg)
        viewValueCheckboxes =
            List.map (viewCheckboxForOneAttributoValue chemicalIdsToName attributo selectedValues) << List.sortWith attributoValueComparator << AssocSet.toList

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
    -> Maybe JsonDataSetStatistics
    -> List (Html Msg)
viewDataSet model attributi chemicalIdsToName dataSet dataSetStatistics =
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
    , tr_ [ td [ colspan 2 ] [] ]
    , tr_
        [ td [ colspan 2 ]
            [ div [ class "mb-5" ]
                [ case dataSetStatistics of
                    Nothing ->
                        text ""

                    Just { runCount, mergeResultsCount, indexedFrames } ->
                        p [ class "hstack gap-1" ] <|
                            (if runCount == 0 then
                                span [ class "badge text-bg-warning" ] [ text "no runs" ]

                             else
                                span [ class "badge text-bg-secondary" ]
                                    [ text
                                        (String.fromInt runCount
                                            ++ " run"
                                            ++ (if runCount > 1 then
                                                    "s"

                                                else
                                                    ""
                                               )
                                        )
                                    ]
                            )
                                :: (let
                                        tooLittleIndexed level =
                                            span [ class ("badge text-bg-" ++ level) ] [ text (formatIntHumanFriendly indexedFrames ++ " indexed") ]

                                        unmerged level =
                                            span [ class ("badge text-bg-" ++ level) ] [ text "unmerged" ]

                                        mergeResults level =
                                            span [ class ("badge text-bg-" ++ level) ]
                                                [ text
                                                    (String.fromInt mergeResultsCount
                                                        ++ " merge result"
                                                        ++ (if mergeResultsCount > 1 then
                                                                "s"

                                                            else
                                                                ""
                                                           )
                                                    )
                                                ]
                                    in
                                    case ( runCount == 0, indexedFrames < 500, mergeResultsCount == 0 ) of
                                        ( True, _, _ ) ->
                                            []

                                        ( _, True, True ) ->
                                            [ tooLittleIndexed "warning"
                                            , unmerged "secondary"
                                            ]

                                        ( _, True, False ) ->
                                            [ tooLittleIndexed "secondary"
                                            , mergeResults "success"
                                            ]

                                        ( _, False, True ) ->
                                            [ tooLittleIndexed "secondary", unmerged "warning" ]

                                        ( _, False, False ) ->
                                            [ tooLittleIndexed "secondary", mergeResults "success" ]
                                   )
                , a
                    [ href
                        (makeLink (AnalysisDataSet dataSet.beamtimeId dataSet.id))
                    ]
                    [ text "→ Go to processing results" ]
                ]
            ]
        ]
    ]


viewFilterForm : Bool -> MergeFilter -> MultiDict AttributoId AttributoValue -> JsonReadNewAnalysisOutput -> Html Msg
viewFilterForm searchAcrossBeamtimes mergeFilter attributoValueFilters request =
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

        viewMergedOption =
            div [ class "btn-group" ]
                [ input_
                    [ type_ "radio"
                    , class "btn-check"
                    , id "merged-filter-merged"
                    , checked (mergeFilter == Merged)
                    , onClick (ChangeMergeFilter Merged)
                    ]
                , label [ class "btn btn-outline-primary", for "merged-filter-merged" ] [ text "Only merged" ]
                , input_
                    [ type_ "radio"
                    , class "btn-check"
                    , id "merged-filter-unmerged"
                    , checked (mergeFilter == Unmerged)
                    , onClick (ChangeMergeFilter Unmerged)
                    ]
                , label [ class "btn btn-outline-primary", for "merged-filter-unmerged" ] [ text "Only unmerged" ]
                , input_
                    [ type_ "radio"
                    , class "btn-check"
                    , id "merged-filter-both"
                    , checked (mergeFilter == Both)
                    , onClick (ChangeMergeFilter Both)
                    ]
                , label [ class "btn btn-outline-primary", for "merged-filter-both" ] [ text "All" ]
                ]
    in
    div
        [ class "pb-3"
        , style "border-bottom" "1pt solid lightgray"
        ]
        [ h4_ [ icon { name = "gear" }, text " Search settings " ]
        , div [ class "row" ]
            [ div [ class "col-lg-6" ]
                [ viewFormCheck "ignore-beamtime-id"
                    "Search across beam times"
                    (Just (text "After checking this, you have to press “Update” again to see all attributi values."))
                    searchAcrossBeamtimes
                    (always ToggleIgnoreBeamtimeId)
                ]
            , div [ class "col-lg-6" ] [ text "Merge status:", br_, viewMergedOption ]
            ]
        , h4 [ class "mt-3" ] [ icon { name = "card-list" }, text " Attributi Filter " ]
        , div [ class "form-text mb-2" ] [ small [] [ text "Note: Dropdowns will only be shown if there is more than one attributo value in a data set." ] ]
        , div_ attributiFilters
        , button
            [ class "btn btn-primary mt-2"
            , type_ "button"
            , onClick UpdateFilter
            ]
            [ text "Update" ]
        ]


viewDataSetForExperimentType : Model -> JsonReadNewAnalysisOutput -> JsonExperimentTypeWithBeamtimeInformation -> Dict Int JsonDataSetStatistics -> AssocSet.Set JsonDataSet -> List (Html Msg)
viewDataSetForExperimentType model analysisResults et dsStatistics dataSets =
    if AssocSet.isEmpty dataSets then
        []

    else
        tr_
            [ td [ colspan 2 ]
                [ h4 [ id ("et-" ++ String.fromInt et.experimentType.id) ] [ text et.experimentType.name ]
                , div [ class "form-text" ]
                    [ text (formatPosixHumanFriendly model.hereAndNow.zone (millisToPosix et.beamtime.start))
                    , text (" - " ++ et.beamtime.title)
                    ]
                ]
            ]
            :: List.concatMap
                (\ds ->
                    viewDataSet
                        model
                        (List.map convertAttributoFromApi analysisResults.attributi)
                        (List.foldr (\{ chemicalId, name } -> Dict.insert chemicalId name)
                            Dict.empty
                            analysisResults.chemicalIdToName
                        )
                        ds
                        (Dict.get ds.id dsStatistics)
                )
                (AssocSet.toList dataSets)


viewDataSets : Model -> JsonReadNewAnalysisOutput -> Dict Int JsonDataSetStatistics -> MultiDict Int JsonDataSet -> Html Msg
viewDataSets model analysisResults dsStatistics byExperimentType =
    if List.isEmpty analysisResults.filteredDataSets then
        div [ class "alert alert-info mt-3" ] [ text "No results yet. Please select filters and press “Update”." ]

    else
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
                    (\et ->
                        viewDataSetForExperimentType
                            model
                            analysisResults
                            et
                            dsStatistics
                            (MultiDict.get et.experimentType.id byExperimentType)
                    )
                    analysisResults.experimentTypes
            ]


viewResults : Model -> JsonReadNewAnalysisOutput -> Html Msg
viewResults ({ attributoValueFilters, searchAcrossBeamtimes, mergeFilter } as model) analysisResults =
    let
        byExperimentType : MultiDict Int JsonDataSet
        byExperimentType =
            List.foldr (\newDataSet -> MultiDict.insert newDataSet.experimentTypeId newDataSet) MultiDict.empty analysisResults.filteredDataSets

        dsStatistics : Dict Int JsonDataSetStatistics
        dsStatistics =
            List.foldr (\newStatistic -> Dict.insert newStatistic.dataSetId newStatistic) Dict.empty analysisResults.dataSetStatistics
    in
    div_
        [ viewFilterForm searchAcrossBeamtimes mergeFilter attributoValueFilters analysisResults
        , div [ class "row" ]
            [ div [ class "col-4" ]
                [ nav [ id "analysis-nav", class "navbar h-100 flex-column align-items-stretch pe-4 border-end" ]
                    [ ul [ class "nav nav-pills flex-column" ] (List.map (viewExperimentTypeHeading byExperimentType) analysisResults.experimentTypes) ]
                ]
            , div [ class "col-8" ]
                [ viewDataSets model analysisResults dsStatistics byExperimentType
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
        ToggleIgnoreBeamtimeId ->
            ( { model | searchAcrossBeamtimes = not model.searchAcrossBeamtimes }, Cmd.none )

        Nop ->
            ( model, Cmd.none )

        ChangeMergeFilter newMergeFilter ->
            ( { model | mergeFilter = newMergeFilter }, Cmd.none )

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
              -- This will reload the page and put the new URL in the browser history, allowing for easy going forward and backward.
            , Nav.pushUrl model.navKey
                (makeLink
                    (AnalysisOverview
                        model.beamtimeId
                        (multidictToFilterList model.attributoValueFilters)
                        model.searchAcrossBeamtimes
                        model.mergeFilter
                    )
                )
            )
