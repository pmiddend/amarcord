module Amarcord.RunAttributiForm exposing (InitData, Model, Msg(..), ShowFileMode(..), init, update, view)

import Amarcord.API.Requests exposing (RunExternalId(..), RunInternalId(..), runExternalIdToString, runInternalIdToInt)
import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo exposing (Attributo, AttributoMap, AttributoType, AttributoValue, attributoMapToListOfAttributi, convertAttributoFromApi, convertAttributoMapFromApi)
import Amarcord.AttributoHtml exposing (AttributoFormMsg(..), AttributoNameWithValueUpdate, EditableAttributiAndOriginal, EditableAttributo, convertEditValues, createEditableAttributi, editEditableAttributi, isEditValueChemicalId, resetEditableAttributo, unsavedAttributoChanges, viewAttributoForm)
import Amarcord.Bootstrap exposing (icon)
import Amarcord.Chemical exposing (Chemical)
import Amarcord.Constants exposing (manualAttributiGroup, manualGlobalAttributiGroup)
import Amarcord.Html exposing (div_, h2_, h5_, input_, li_, onIntInput, p_, strongText)
import Amarcord.HttpError exposing (HttpError, send)
import Amarcord.RunFilesForm as RunFilesForm
import Amarcord.Util exposing (listContainsBy)
import Api.Data exposing (ChemicalType(..), JsonAttributo, JsonExperimentType, JsonFileOutput, JsonRun, JsonUpdateRunOutput)
import Api.Request.Runs exposing (updateRunApiRunsPatch)
import Html exposing (Html, a, button, div, form, label, option, p, select, text, ul)
import Html.Attributes exposing (checked, class, disabled, for, href, selected, type_, value)
import Html.Events exposing (onClick, onInput)
import List
import List.Extra as ListExtra
import Maybe
import RemoteData exposing (RemoteData(..), isLoading, isSuccess)
import String
import Time exposing (Posix, Zone, millisToPosix)


type alias ChemicalList =
    List (Chemical Int (AttributoMap AttributoValue) JsonFileOutput)


type ShowFileMode
    = ShowFiles
    | HideFiles


type alias Model =
    { runId : RunInternalId
    , runExternalId : RunExternalId
    , started : Posix
    , stopped : Maybe Posix
    , experimentTypeId : Int
    , editableAttributi : EditableAttributiAndOriginal
    , experimentTypes : List JsonExperimentType
    , chemicals : ChemicalList
    , attributi : List (Attributo AttributoType)

    -- This is to handle a tricky case: usually we want to stay with
    -- the latest run so we can quickly change settings. If we
    -- manually click on an older run to edit it, we don't want to
    -- then jump to the latest one.
    , initiatedManually : Bool
    , showAllAttributi : Bool
    , filesEdit : Maybe RunFilesForm.Model
    , zone : Zone
    , submitErrors : List String
    , runEditRequest : RemoteData HttpError JsonUpdateRunOutput
    }


type Msg
    = Submit
    | SubmitFinished (Result HttpError JsonUpdateRunOutput)
    | Cancel
    | ValueUpdate AttributoNameWithValueUpdate
    | ExperimentTypeIdChanged Int
    | ChangeShowAllAttributi Bool
    | UpdateRun JsonRun
    | RunFilesFormMsg RunFilesForm.Msg


type alias InitData =
    { zone : Zone
    , attributi : List JsonAttributo
    , chemicals : ChemicalList
    , experimentTypes : List JsonExperimentType
    }


init : InitData -> ShowFileMode -> JsonRun -> ( Model, Cmd Msg )
init { zone, attributi, chemicals, experimentTypes } showFileMode latestRunReal =
    ( { runId = RunInternalId latestRunReal.id
      , runExternalId = RunExternalId latestRunReal.externalId
      , started = millisToPosix latestRunReal.started
      , stopped = Maybe.map millisToPosix latestRunReal.stopped
      , experimentTypeId = latestRunReal.experimentTypeId
      , editableAttributi =
            createEditableAttributi
                zone
                (List.map convertAttributoFromApi attributi)
                (convertAttributoMapFromApi latestRunReal.attributi)
      , experimentTypes = experimentTypes
      , chemicals = chemicals
      , attributi = List.map convertAttributoFromApi attributi
      , initiatedManually = False
      , showAllAttributi = False
      , filesEdit =
            case showFileMode of
                ShowFiles ->
                    Just (RunFilesForm.init latestRunReal)

                _ ->
                    Nothing
      , zone = zone
      , submitErrors = []
      , runEditRequest = NotAsked
      }
    , Cmd.none
    )


view : Model -> Html Msg
view { runExternalId, experimentTypeId, editableAttributi, showAllAttributi, submitErrors, runEditRequest, chemicals, experimentTypes, filesEdit } =
    let
        matchesCurrentExperiment a x =
            case x of
                Nothing ->
                    True

                Just attributi ->
                    listContainsBy (\otherAttributo -> otherAttributo.id == a.id) attributi

        -- For ergonomic reasons, we want chemical attributi to be on top - everything else should be
        -- sorted alphabetically
        attributoSortKey a =
            ( if isEditValueChemicalId a.type_.editValue then
                0

              else
                1
            , a.name
            )

        currentExperimentTypeAttributi =
            ListExtra.find (\et -> et.id == experimentTypeId) experimentTypes |> Maybe.map .attributi

        attributoFilterFunction a =
            a.associatedTable
                == AssociatedTable.Run
                && (showAllAttributi || a.group == manualGlobalAttributiGroup || a.group == manualAttributiGroup && matchesCurrentExperiment a currentExperimentTypeAttributi)
                && not (List.member a.name [ "started", "stopped" ])

        filteredAttributi : List EditableAttributo
        filteredAttributi =
            List.sortBy attributoSortKey <| List.filter attributoFilterFunction editableAttributi.editableAttributi

        viewAttributoFormWithRole : EditableAttributo -> Html AttributoFormMsg
        viewAttributoFormWithRole e =
            viewAttributoForm chemicals
                (Maybe.withDefault ChemicalTypeSolution <|
                    Maybe.map .role <|
                        ListExtra.find (\awr -> awr.id == e.id) <|
                            Maybe.withDefault [] currentExperimentTypeAttributi
                )
                e

        submitErrorsHtml =
            case submitErrors of
                [] ->
                    [ text "" ]

                errors ->
                    [ p_ [ strongText "There were submission errors:" ]
                    , ul [ class "text-danger" ] <| List.map (\e -> li_ [ text e ]) errors
                    ]

        submitSuccess =
            if isSuccess runEditRequest then
                [ div [ class "mb-3" ] [ div [ class "badge text-bg-success" ] [ text "Saved!" ] ] ]

            else
                []

        buttons =
            [ button
                [ class "btn btn-secondary me-2"
                , disabled (isLoading runEditRequest)
                , type_ "button"
                , onClick Submit
                ]
                [ icon { name = "save" }, text " Save changes" ]
            , button
                [ class "btn btn-outline-secondary"
                , type_ "button"
                , onClick Cancel
                ]
                [ icon { name = "x-lg" }, text " Cancel" ]
            ]

        attributoFormMsgToMsg : AttributoFormMsg -> Msg
        attributoFormMsgToMsg x =
            case x of
                AttributoFormValueUpdate vu ->
                    ValueUpdate vu

                AttributoFormSubmit ->
                    Submit

        viewExperimentTypeOption : JsonExperimentType -> Html msg
        viewExperimentTypeOption experimentType =
            option
                [ selected (experimentType.id == experimentTypeId)
                , value (String.fromInt experimentType.id)
                ]
                [ text experimentType.name ]

        filesForm =
            case filesEdit of
                Nothing ->
                    []

                Just filesEditModel ->
                    [ h5_ [ text "File paths" ]
                    , p [ class "form-text" ]
                        [ text "You can have more than one path (glob) for each source. A path can contain the usual "
                        , a [ href "https://en.wikipedia.org/wiki/Glob_(programming)" ] [ text "globbing" ]
                        , text " patterns to choose more than one file. For example, “/some/path/*.h5” will match all h5 files in the directory “/some/path”."
                        ]
                    , Html.map RunFilesFormMsg (RunFilesForm.view filesEditModel)
                    ]
    in
    div_
        [ h2_ [ text ("Edit run " ++ runExternalIdToString runExternalId) ]
        , form [ class "mb-3" ] <|
            div [ class "form-check form-switch mb-3" ]
                [ input_ [ type_ "checkbox", Html.Attributes.id "show-all-attributi", class "form-check-input", checked showAllAttributi, onInput (always (ChangeShowAllAttributi (not showAllAttributi))) ]
                , label [ class "form-check-label", for "show-all-attributi" ] [ text "Show all attributi" ]
                ]
                :: div [ class "mb-3" ]
                    [ div [ class "form-floating" ]
                        [ select
                            [ class "form-select"
                            , Html.Attributes.id "current-experiment-type-for-specific-run"
                            , onIntInput ExperimentTypeIdChanged
                            ]
                            (List.map viewExperimentTypeOption experimentTypes)
                        , label [ for "current-experiment-type" ] [ text "Experiment Type" ]
                        ]
                    ]
                :: (List.map (Html.map attributoFormMsgToMsg << viewAttributoFormWithRole) filteredAttributi ++ filesForm ++ submitErrorsHtml ++ submitSuccess ++ buttons)
        ]


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        RunFilesFormMsg subMsg ->
            case model.filesEdit of
                Nothing ->
                    ( model, Cmd.none )

                Just filesEditReal ->
                    let
                        newFilesEdit =
                            RunFilesForm.update subMsg filesEditReal
                    in
                    ( { model | filesEdit = Just newFilesEdit }, Cmd.none )

        Submit ->
            case convertEditValues model.zone model.editableAttributi of
                Err errorList ->
                    ( { model
                        | submitErrors =
                            List.map (\( attributoId, errorMessage ) -> String.fromInt attributoId ++ ": " ++ errorMessage) errorList
                      }
                    , Cmd.none
                    )

                Ok editedAttributi ->
                    ( { model | runEditRequest = Loading }
                    , send SubmitFinished
                        (updateRunApiRunsPatch
                            { id = runInternalIdToInt model.runId
                            , experimentTypeId = model.experimentTypeId
                            , attributi = attributoMapToListOfAttributi editedAttributi
                            , files = Maybe.map RunFilesForm.retrieveFiles model.filesEdit
                            }
                        )
                    )

        Cancel ->
            ( model, Cmd.none )

        SubmitFinished result ->
            case result of
                Err e ->
                    ( { model | runEditRequest = Failure e }, Cmd.none )

                Ok editRequestResult ->
                    let
                        resetEditedFlags : Model -> Model
                        resetEditedFlags rei =
                            { model
                                | runId = rei.runId
                                , runExternalId = rei.runExternalId
                                , started = rei.started
                                , stopped = rei.stopped
                                , experimentTypeId = rei.experimentTypeId
                                , editableAttributi =
                                    { originalAttributi = rei.editableAttributi.originalAttributi
                                    , editableAttributi = List.map resetEditableAttributo rei.editableAttributi.editableAttributi
                                    }

                                -- Reset manual edit flag, so we automatically jump to the latest run again
                                , initiatedManually = False
                                , showAllAttributi = False
                                , runEditRequest = Success editRequestResult
                                , submitErrors = []
                            }
                    in
                    ( resetEditedFlags model
                    , Cmd.none
                    )

        ExperimentTypeIdChanged v ->
            ( { model | experimentTypeId = v }, Cmd.none )

        ValueUpdate v ->
            let
                newEditable =
                    editEditableAttributi model.editableAttributi.editableAttributi v
            in
            ( { model | editableAttributi = { editableAttributi = newEditable, originalAttributi = model.editableAttributi.originalAttributi } }, Cmd.none )

        ChangeShowAllAttributi newValue ->
            ( { model | showAllAttributi = newValue }, Cmd.none )

        UpdateRun newRun ->
            if unsavedAttributoChanges model.editableAttributi.editableAttributi then
                ( model, Cmd.none )

            else
                ( { model
                    | started = millisToPosix newRun.started
                    , stopped = Maybe.map millisToPosix newRun.stopped
                    , experimentTypeId = newRun.experimentTypeId
                    , editableAttributi =
                        createEditableAttributi
                            model.zone
                            model.attributi
                            (convertAttributoMapFromApi newRun.attributi)
                  }
                , Cmd.none
                )
