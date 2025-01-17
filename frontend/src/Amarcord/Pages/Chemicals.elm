module Amarcord.Pages.Chemicals exposing (Model, Msg, convertChemicalsResponse, init, pageTitle, update, view)

import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.AssociatedTable as AssociatedTable
import Amarcord.Attributo as Attributo exposing (Attributo, AttributoId, AttributoMap, AttributoType(..), AttributoValue, ChemicalNameDict, attributoMapToListOfAttributi, convertAttributoFromApi, convertAttributoMapFromApi, emptyAttributoMap, extractChemical, mapAttributo)
import Amarcord.AttributoHtml exposing (AttributoFormMsg(..), AttributoNameWithValueUpdate, EditStatus(..), EditableAttributiAndOriginal, EditableAttributo, convertEditValues, createEditableAttributi, editEditableAttributi, extractStringAttributo, findEditableAttributo, viewAttributoCell, viewAttributoForm)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert, mimeTypeToIcon, viewRemoteDataHttp)
import Amarcord.Chemical exposing (Chemical, ChemicalId, chemicalMapAttributi, chemicalMapId, chemicalTypeToApi, convertChemicalFromApi)
import Amarcord.Crystallography exposing (validateCellDescription, validatePointGroup)
import Amarcord.Dialog as Dialog
import Amarcord.Html exposing (br_, div_, em_, form_, h2_, h3_, h4_, h5_, hr_, img_, input_, li_, onIntInput, p_, span_, strongText, sup_, tbody_, td_, th_, thead_, tr_, ul_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.MarkdownUtil exposing (markupWithoutErrors)
import Amarcord.Route exposing (makeFilesLink)
import Amarcord.Util exposing (HereAndNow, monthToNumericString, scrollToTop)
import Api.Data exposing (ChemicalType(..), JsonChemical, JsonChemicalWithId, JsonChemicalWithoutId, JsonCopyChemicalOutput, JsonCreateFileOutput, JsonDeleteChemicalInput, JsonDeleteChemicalOutput, JsonFileOutput, JsonReadAllChemicals, JsonReadChemicals, JsonReadRuns, JsonRun)
import Api.Request.Chemicals exposing (copyChemicalApiCopyChemicalPost, createChemicalApiChemicalsPost, deleteChemicalApiChemicalsDelete, readAllChemicalsApiAllChemicalsGet, readChemicalsApiChemicalsBeamtimeIdGet, updateChemicalApiChemicalsPatch)
import Api.Request.Files exposing (createFileApiFilesPost)
import Api.Request.Runs exposing (readRunsApiRunsBeamtimeIdGet)
import Basics.Extra exposing (safeDivide)
import Bytes
import Dict exposing (Dict)
import File as ElmFile
import File.Select
import Html exposing (..)
import Html.Attributes exposing (attribute, checked, class, disabled, for, href, id, selected, src, style, title, type_, value)
import Html.Events exposing (onClick, onInput)
import List exposing (isEmpty, length, singleton)
import List.Extra as ListExtra
import Maybe.Extra as MaybeExtra exposing (isJust, isNothing)
import RemoteData exposing (RemoteData(..), fromResult)
import Result.Extra as ResultExtra
import Set exposing (Set)
import String
import Task
import Time exposing (Zone, millisToPosix, toMonth, toYear)


type alias ChemicalsAndAttributi =
    { chemicals : List (Chemical ChemicalId (AttributoMap AttributoValue) JsonFileOutput)
    , attributi : List (Attributo AttributoType)
    }


type alias FileWithBytes =
    { fileMetadata : ElmFile.File
    , fileBytes : Bytes.Bytes
    }


type alias NewFileUpload =
    { file : Maybe FileWithBytes
    , description : String
    }


type alias CopyPriorChemicalData =
    { priorChemicals : JsonReadAllChemicals
    , selectedChemicalId : Maybe Int
    , submitRequest : RemoteData HttpError JsonCopyChemicalOutput
    , createAttributi : Bool
    }


type alias Model =
    { chemicals : RemoteData HttpError ChemicalsAndAttributi
    , chemicalsUsedInRuns : Set ChemicalId
    , priorChemicals : RemoteData HttpError CopyPriorChemicalData
    , deleteModalOpen : Maybe ( String, ChemicalId )
    , chemicalDeleteRequest : RemoteData HttpError ()
    , fileUploadRequest : RemoteData HttpError ()
    , editChemical : Maybe (Chemical (Maybe Int) EditableAttributiAndOriginal JsonFileOutput)
    , modifyRequest : RemoteData HttpError ()
    , myTimeZone : Zone
    , submitErrors : List (Html Msg)
    , newFileUpload : NewFileUpload
    , responsiblePersonFilter : Maybe String
    , typeFilter : Maybe ChemicalType
    , beamtimeId : BeamtimeId
    }


pageTitle : Model -> String
pageTitle _ =
    "Chemicals"


type Msg
    = ChemicalsReceived (Result HttpError JsonReadChemicals)
    | PriorChemicalsReceived (Result HttpError JsonReadAllChemicals)
    | InitiateCopyFromPreviousBeamtime
    | CancelCopyFromPreviousBeamtime
    | ToggleCreateAttriutiForCopyFromPreviousBeamtime
    | SubmitCopyFromPreviousBeamtime
    | CopyFromPreviousBeamtimeIdChanged Int
    | CopyFromPreviousBeamtimeFinished (Result HttpError JsonCopyChemicalOutput)
    | RunsReceived (Result HttpError JsonReadRuns)
    | CancelDelete
    | AskDelete String ChemicalId
    | InitiateEdit (Chemical Int (AttributoMap AttributoValue) JsonFileOutput)
    | InitiateClone (Chemical Int (AttributoMap AttributoValue) JsonFileOutput)
    | ConfirmDelete ChemicalId
    | AddChemical ChemicalType
    | EditChemicalName String
    | EditChemicalResponsiblePerson String
    | ChemicalDeleteFinished (Result HttpError JsonDeleteChemicalOutput)
    | EditChemicalSubmit
    | EditChemicalCancel
    | EditChemicalFinished (Result HttpError {})
    | EditChemicalAttributo AttributoNameWithValueUpdate
    | EditNewFileDescription String
    | EditNewFileFile ElmFile.File
    | EditNewFileWithBytes FileWithBytes
    | EditFileUpload
    | EditFileUploadFinished (Result HttpError JsonCreateFileOutput)
    | EditFileDelete Int
    | EditResetNewFileUpload
    | EditNewFileOpenSelector
    | Nop
    | ChangeResponsiblePersonFilter (Maybe String)
    | ChangeTypeFilter (Maybe ChemicalType)


init : HereAndNow -> BeamtimeId -> ( Model, Cmd Msg )
init { zone } beamtimeId =
    ( { chemicals = Loading
      , priorChemicals = NotAsked
      , chemicalsUsedInRuns = Set.empty
      , deleteModalOpen = Nothing
      , chemicalDeleteRequest = NotAsked
      , modifyRequest = NotAsked
      , fileUploadRequest = NotAsked
      , editChemical = Nothing
      , myTimeZone = zone
      , submitErrors = []
      , newFileUpload = { file = Nothing, description = "" }
      , responsiblePersonFilter = Nothing
      , typeFilter = Nothing
      , beamtimeId = beamtimeId
      }
    , getChemicalsAndRuns beamtimeId
    )


getChemicalsAndRuns : BeamtimeId -> Cmd Msg
getChemicalsAndRuns beamtimeId =
    Cmd.batch
        [ send ChemicalsReceived (readChemicalsApiChemicalsBeamtimeIdGet beamtimeId)
        , send RunsReceived (readRunsApiRunsBeamtimeIdGet beamtimeId Nothing Nothing Nothing)
        ]


viewFiles : RemoteData HttpError () -> NewFileUpload -> List JsonFileOutput -> List (Html Msg)
viewFiles fileUploadError newFile files =
    let
        viewFileRow : JsonFileOutput -> Html Msg
        viewFileRow file =
            tr [ class "align-middle" ]
                [ td_ [ text (String.fromInt file.id) ]
                , td_ [ text file.fileName ]
                , td_ [ text file.type__ ]
                , td_ [ markupWithoutErrors file.description ]
                , td_ [ button [ class "btn btn-danger btn-sm", type_ "button", onClick (EditFileDelete file.id) ] [ icon { name = "trash" } ] ]
                ]

        uploadForm =
            [ div [ class "card mb-3" ]
                [ div [ class "card-header" ]
                    [ text "File upload" ]
                , div [ class "card-body" ]
                    [ viewRemoteDataHttp "Upload successful!" fileUploadError
                    , div [ class "input-group mb-3" ]
                        [ button [ type_ "button", class "btn btn-outline-secondary", onClick EditNewFileOpenSelector ] [ text "Choose file..." ]
                        , input [ type_ "text", disabled True, value (MaybeExtra.unwrap "No file selected" (ElmFile.name << .fileMetadata) newFile.file), class "form-control" ] []
                        ]
                    , div [ class "mb-3" ]
                        [ label [ for "file-description", class "form-label" ] [ text "File Description", sup_ [ text "*" ] ]
                        , input_
                            [ type_ "text"
                            , class
                                ("form-control"
                                    ++ (if String.isEmpty newFile.description && isJust newFile.file then
                                            " is-invalid"

                                        else
                                            ""
                                       )
                                )
                            , id "name"
                            , value newFile.description
                            , onInput EditNewFileDescription
                            ]
                        ]
                    , button
                        [ class "btn btn-dark me-3"
                        , type_ "button"
                        , disabled (newFile.description == "" || isNothing newFile.file)
                        , onClick EditFileUpload
                        ]
                        [ icon { name = "upload" }, text " Upload" ]
                    , button
                        [ class "btn btn-light me-3"
                        , type_ "button"
                        , onClick EditResetNewFileUpload
                        ]
                        [ icon { name = "arrow-counterclockwise" }, text " Reset" ]
                    ]
                ]
            ]
    in
    if length files == 0 then
        uploadForm

    else
        let
            filesTable =
                [ h5_ [ text "Attached files" ]
                , table [ class "table table-striped table-sm" ]
                    [ thead_
                        [ tr_
                            [ th_ [ text "ID" ]
                            , th_ [ text "File name" ]
                            , th_ [ text "Type" ]
                            , th_ [ text "Description" ]
                            , th_ [ text "Actions" ]
                            ]
                        ]
                    , tbody_ (List.map viewFileRow files)
                    ]
                ]
        in
        filesTable ++ uploadForm


viewEditForm : List (Chemical ChemicalId (AttributoMap AttributoValue) JsonFileOutput) -> RemoteData HttpError () -> List (Html Msg) -> NewFileUpload -> Chemical (Maybe Int) EditableAttributiAndOriginal JsonFileOutput -> Html Msg
viewEditForm chemicals fileUploadRequest submitErrorsList newFileUpload editingChemical =
    let
        attributoFormMsgToMsg : AttributoFormMsg -> Msg
        attributoFormMsgToMsg x =
            case x of
                AttributoFormValueUpdate vu ->
                    EditChemicalAttributo vu

                -- With chemicals, pressing return and submitting would close the form.
                -- That's very unexpected!
                AttributoFormSubmit ->
                    Nop

        attributiFormEntries =
            List.map (\attributo -> Html.map attributoFormMsgToMsg (viewAttributoForm [] Nothing attributo)) editingChemical.attributi.editableAttributi

        otherChemicalsNames =
            List.map .name <|
                case editingChemical.id of
                    Nothing ->
                        chemicals

                    Just editingId ->
                        List.filter (\s -> not <| .id s == editingId) chemicals

        submitErrors =
            case submitErrorsList of
                [] ->
                    [ text "" ]

                errors ->
                    [ p_ [ strongText "There were submission errors:" ]
                    , ul [ class "text-danger" ] <| List.map (\e -> li_ [ e ]) errors
                    ]

        isDuplicateName =
            otherChemicalsNames
                |> List.map String.trim
                |> List.member (String.trim editingChemical.name)

        isValidChemicalName =
            not isDuplicateName
                && (String.trim editingChemical.name /= "")

        isInvalidChemicalNameStyle =
            if not isValidChemicalName then
                " is-invalid"

            else
                ""

        chemicalTypeToPretty ct =
            case ct of
                ChemicalTypeCrystal ->
                    "crystals"

                ChemicalTypeSolution ->
                    "solution"

        addOrEditHeadline =
            h4_
                [ viewChemicalTypeIcon editingChemical.type_
                , text
                    (if isNothing editingChemical.id then
                        " Add new " ++ chemicalTypeToPretty editingChemical.type_

                     else
                        " Edit " ++ chemicalTypeToPretty editingChemical.type_
                    )
                ]
    in
    form_ <|
        [ addOrEditHeadline
        , p [ class "text-muted" ]
            [ text "If you prepared your crystals in "
            , em_ [ text "multiple batches" ]
            , text ", please create "
            , em_ [ text "one chemical per batch" ]
            , text ". This helps during analysis."
            ]
        , p [ class " text-muted" ]
            [ text "If you want a quick "
            , em_ [ text "refinement step" ]
            , text " at the end of the merging (see the Analysis view), upload a "
            , em_ [ text "PDB file" ]
            , text " with a base model for the protein."
            ]
        , p [ class " text-muted" ]
            [ text "For the details on the "
            , strong [ style "font-weight" "bold" ] [ text "cell description" ]
            , text " please refer to the "
            , a [ href "https://www.desy.de/~twhite/crystfel/tutorial-0.9.1.html#index" ] [ text "CrystFEL tutorial, section 10" ]
            , text ". For the "
            , strong [ style "font-weight" "bold" ] [ text "point group" ]
            , text " please refer to the "
            , a [ href "https://www.desy.de/~twhite/crystfel/tutorial-0.9.1.html#merge" ] [ text "CrystFEL tutorial, section 13" ]
            , text "."
            ]
        , div [ class "mb-3" ]
            [ label [ for "name", class "form-label" ] [ text "Name", sup_ [ text "*" ] ]
            , input_
                [ type_ "text"
                , class
                    ("form-control"
                        ++ isInvalidChemicalNameStyle
                    )
                , id "name"
                , value editingChemical.name
                , onInput EditChemicalName
                ]
            , if String.trim editingChemical.name /= "" then
                if isDuplicateName then
                    div [ class "invalid-feedback" ] [ text "Name already used" ]

                else
                    text ""

              else
                div [ class "invalid-feedback" ] [ text "Name is mandatory" ]
            ]
        , div [ class "mb-3" ]
            [ label [ for "responsible_person", class "form-label" ] [ text "Responsible person", sup_ [ text "*" ] ]
            , input_
                [ type_ "text"
                , if String.trim editingChemical.responsiblePerson == "" then
                    class "form-control is-invalid"

                  else
                    class "form-control"
                , id "responsible_person"
                , value editingChemical.responsiblePerson
                , onInput EditChemicalResponsiblePerson
                ]
            , if String.trim editingChemical.responsiblePerson == "" then
                div [ class "invalid-feedback" ] [ text "Responsible person is mandatory" ]

              else
                text ""
            ]
        ]
            ++ attributiFormEntries
            ++ viewFiles fileUploadRequest newFileUpload editingChemical.files
            ++ submitErrors
            ++ [ button
                    [ class "btn btn-primary me-3 mb-3"
                    , onClick EditChemicalSubmit
                    , disabled (not isValidChemicalName)
                    , type_ "button"
                    ]
                    [ icon { name = "plus-lg" }
                    , text
                        (if isNothing editingChemical.id then
                            " Save"

                         else
                            " Confirm edit"
                        )
                    ]
               , button
                    [ class "btn btn-secondary me-3 mb-3"
                    , onClick EditChemicalCancel
                    , type_ "button"
                    ]
                    [ icon { name = "x-lg" }, text " Cancel" ]
               ]


viewChemicalRow : Zone -> List (Attributo AttributoType) -> Set ChemicalId -> Chemical ChemicalId (AttributoMap AttributoValue) JsonFileOutput -> List (Html Msg)
viewChemicalRow zone attributi chemicalIsUsedInRun chemical =
    let
        viewFile { id, type__, fileName, description } =
            li [ class "list-group-item" ] <|
                if String.startsWith "image/" type__ then
                    [ figure [ class "figure" ] [ img_ [ src (makeFilesLink id Nothing), style "width" "20em" ], figcaption [ class "figure-caption" ] [ a [ href (makeFilesLink id Nothing) ] [ text description ] ] ] ]

                else
                    [ mimeTypeToIcon type__
                    , text " "
                    , span [ attribute "data-tooltip" description, class "align-top" ] [ a [ href (makeFilesLink id Nothing) ] [ text fileName ] ]
                    ]

        files =
            case chemical.files of
                [] ->
                    text ""

                _ ->
                    ul [ class "list-group list-group-flush" ] (List.map viewFile chemical.files)

        noAttributoColumns =
            4

        attributoColumnsPercent =
            String.fromFloat (Maybe.withDefault 0.0 <| safeDivide 100.0 (toFloat noAttributoColumns)) ++ "%"

        viewAttributoGroupItem : Attributo AttributoType -> Html msg
        viewAttributoGroupItem attributo =
            let
                attributoValue =
                    if attributo.name == "ID" then
                        text (String.fromInt chemical.id)

                    else if attributo.name == "Responsible person" then
                        text chemical.responsiblePerson

                    else if attributo.name == "Type" then
                        text <|
                            case chemical.type_ of
                                ChemicalTypeCrystal ->
                                    "Crystal"

                                ChemicalTypeSolution ->
                                    "Solution"

                    else
                        viewAttributoCell
                            { shortDateTime = False
                            , colorize = False
                            , withUnit = True
                            , withTolerance = False
                            }
                            zone
                            Dict.empty
                            chemical.attributi
                            attributo
            in
            td [ style "width" attributoColumnsPercent ] [ small [ class "text-muted" ] [ text attributo.name ], br_, attributoValue ]

        viewAttributiGroup : List (Attributo AttributoType) -> Html msg
        viewAttributiGroup groupItems =
            tr_ <| List.map viewAttributoGroupItem groupItems

        virtualChemicalTableAttributo : String -> AttributoType -> Attributo AttributoType
        virtualChemicalTableAttributo name aType =
            { id = -1
            , name = name
            , description = ""
            , group = ""
            , type_ = aType
            , associatedTable = AssociatedTable.Chemical
            }

        virtualIdAttributo : Attributo AttributoType
        virtualIdAttributo =
            virtualChemicalTableAttributo "ID" Attributo.Int

        virtualTypeAttributo : Attributo AttributoType
        virtualTypeAttributo =
            virtualChemicalTableAttributo "Type" Attributo.String

        virtualResponsiblePersonAttributo : Attributo AttributoType
        virtualResponsiblePersonAttributo =
            virtualChemicalTableAttributo "Responsible person" Attributo.String
    in
    [ div [ style "margin-bottom" "4rem" ]
        [ h3_
            [ div [ class "hstack gap-3" ]
                [ text chemical.name
                , div [ class "btn-group" ]
                    [ button [ class "btn btn-sm btn-outline-secondary", onClick (InitiateEdit chemical) ] [ icon { name = "pencil-square" }, text " Edit" ]
                    , button [ class "btn btn-sm btn-outline-info", onClick (InitiateClone chemical) ] [ icon { name = "terminal" }, text " Duplicate" ]
                    , if Set.member chemical.id chemicalIsUsedInRun then
                        button
                            [ class "btn btn-sm btn-outline-danger"
                            , style "pointer-events" "all"
                            , style "cursor" "not-allowed"
                            , disabled True
                            , title "Chemical associated to one or more runs, can only be edited."
                            ]
                            [ icon { name = "trash" }, text " Delete" ]

                      else
                        button
                            [ class "btn btn-sm btn-outline-danger"
                            , onClick (AskDelete chemical.name chemical.id)
                            ]
                            [ icon { name = "trash" }, text " Delete" ]
                    ]
                ]
            ]
        , table [ class "table table-sm" ]
            [ tbody_ (List.map viewAttributiGroup <| ListExtra.greedyGroupsOf noAttributoColumns (virtualIdAttributo :: virtualTypeAttributo :: virtualResponsiblePersonAttributo :: attributi))
            ]
        , files
        ]
    ]


viewChemicalTable :
    Set ChemicalId
    -> Zone
    -> List (Chemical ChemicalId (AttributoMap AttributoValue) JsonFileOutput)
    -> List (Attributo AttributoType)
    -> Maybe String
    -> Maybe ChemicalType
    -> Html Msg
viewChemicalTable usedChemicalIds zone chemicals attributi responsiblePersonFilter typeFilter =
    if isEmpty chemicals then
        div [ class "mt-3" ] [ h2 [ class "text-muted" ] [ text "No chemicals entered yet." ] ]

    else
        let
            viewResponsiblePersonFilterOption : String -> List (Html Msg)
            viewResponsiblePersonFilterOption responsiblePerson =
                [ input_ [ type_ "radio", class "btn-check", id ("filter" ++ responsiblePerson), checked (responsiblePersonFilter == Just responsiblePerson), onClick (ChangeResponsiblePersonFilter (Just responsiblePerson)) ]
                , label [ class "btn btn-outline-primary", for ("filter" ++ responsiblePerson) ] [ small [] [ text responsiblePerson ] ]
                ]

            viewTypeFilter =
                div_
                    [ div [ class "btn-group mb-1" ]
                        [ input_
                            [ type_ "radio"
                            , class "btn-check"
                            , id "type-filter"
                            , checked (typeFilter == Nothing)
                            , onClick (ChangeTypeFilter Nothing)
                            ]
                        , label [ class "btn btn-outline-primary", for "type-filter" ] [ text "All" ]
                        , input_
                            [ type_ "radio"
                            , class "btn-check"
                            , id "type-filter-crystals"
                            , checked (typeFilter == Just ChemicalTypeCrystal)
                            , onClick (ChangeTypeFilter (Just ChemicalTypeCrystal))
                            ]
                        , label [ class "btn btn-outline-primary", for "type-filter-crystals" ] [ viewChemicalTypeIcon ChemicalTypeCrystal, text " Crystals" ]
                        , input_
                            [ type_ "radio"
                            , class "btn-check"
                            , id "type-filter-solution"
                            , checked (typeFilter == Just ChemicalTypeSolution)
                            , onClick (ChangeTypeFilter (Just ChemicalTypeSolution))
                            ]
                        , label [ class "btn btn-outline-primary", for "type-filter-solution" ] [ viewChemicalTypeIcon ChemicalTypeSolution, text " Solution" ]
                        ]
                    ]

            viewResponsiblePersonFilter =
                div_
                    [ div [ class "btn-group mb-3" ] <|
                        List.concat
                            [ [ input_
                                    [ type_ "radio"
                                    , class "btn-check"
                                    , id "responsible-person-filter"
                                    , checked (responsiblePersonFilter == Nothing)
                                    , onClick (ChangeResponsiblePersonFilter Nothing)
                                    ]
                              , label [ class "btn btn-outline-primary", for "responsible-person-filter" ] [ small [] [ text "All" ] ]
                              ]
                            , List.concatMap viewResponsiblePersonFilterOption (Set.toList (List.foldr (\e -> Set.insert e.responsiblePerson) Set.empty chemicals))
                            ]
                    ]

            chemicalFilter c =
                MaybeExtra.unwrap True (\rpf -> c.responsiblePerson == rpf) responsiblePersonFilter && MaybeExtra.unwrap True (\tf -> c.type_ == tf) typeFilter

            filteredChemicals : List (Chemical ChemicalId (AttributoMap AttributoValue) JsonFileOutput)
            filteredChemicals =
                List.filter chemicalFilter chemicals
        in
        div [ class "mt-3" ] (h2_ [ text "Available chemicals" ] :: viewTypeFilter :: viewResponsiblePersonFilter :: hr_ :: List.concatMap (viewChemicalRow zone attributi usedChemicalIds) filteredChemicals)


viewChemicalTypeIcon : ChemicalType -> Html msg
viewChemicalTypeIcon ct =
    case ct of
        ChemicalTypeCrystal ->
            icon { name = "gem" }

        ChemicalTypeSolution ->
            icon { name = "droplet-fill" }


viewPriorChemicals : Model -> List (Attributo AttributoType) -> CopyPriorChemicalData -> Html Msg
viewPriorChemicals model attributi { priorChemicals, selectedChemicalId } =
    let
        viewPriorChemical { id, name, beamtimeId } =
            let
                title =
                    case ListExtra.find (\bt -> bt.id == beamtimeId) priorChemicals.beamtimes of
                        Nothing ->
                            name

                        Just { start } ->
                            let
                                startAsPosix =
                                    millisToPosix start
                            in
                            name
                                ++ " / "
                                ++ String.fromInt
                                    (toYear model.myTimeZone startAsPosix)
                                ++ "-"
                                ++ monthToNumericString (toMonth model.myTimeZone startAsPosix)
            in
            option
                [ value (String.fromInt id)
                , selected (selectedChemicalId == Just id)
                ]
                [ text title ]

        attributiNamesInThisBeamtime : Set String
        attributiNamesInThisBeamtime =
            List.foldr (\a -> Set.insert a.name) Set.empty attributi

        attributoIdToNameDict : Dict Int String
        attributoIdToNameDict =
            List.foldr (\{ id, name } -> Dict.insert id name) Dict.empty priorChemicals.attributiNames

        attributoIdToName : Int -> String
        attributoIdToName aid =
            Dict.get aid attributoIdToNameDict |> Maybe.withDefault "INVALID"

        viewAddedAttributiForChemical : JsonChemical -> Html Msg
        viewAddedAttributiForChemical selectedChemical =
            case
                List.filter
                    (\aname -> not <| Set.member aname attributiNamesInThisBeamtime)
                    (List.map (attributoIdToName << .attributoId) selectedChemical.attributi)
            of
                [] ->
                    text ""

                addedAttributi ->
                    div [ class "alert alert-warning" ]
                        [ p_ [ text "This chemical has the following additional attributi: " ]
                        , ul_ (List.map (\aname -> li_ [ text aname ]) addedAttributi)
                        , div [ class "form-check" ]
                            [ input_
                                [ class "form-check-input"
                                , type_ "checkbox"
                                , id "add-additional-attributi"
                                , onClick ToggleCreateAttriutiForCopyFromPreviousBeamtime
                                ]
                            , label
                                [ for "add-additional-attributi"
                                , class "form-check-label"
                                ]
                                [ text "Create and fill these attributi as well." ]
                            , div [ class "form-text" ]
                                [ text "Otherwise the attributi will be ignored — the newly copied chemical will not have these attributi. "
                                , em_ [ text "If you are not sure what to do, leave this unchecked." ]
                                ]
                            ]
                        ]

        viewAddedAttributi : Html Msg
        viewAddedAttributi =
            ListExtra.find (\c -> Just c.id == selectedChemicalId) priorChemicals.chemicals
                |> Maybe.map viewAddedAttributiForChemical
                |> Maybe.withDefault (text "")
    in
    form_ <|
        [ h4_ [ icon { name = "terminal" }, text " Select chemical to copy" ]
        , select
            [ id "chemical-to-copy"
            , class "form-select mb-3"
            , onIntInput CopyFromPreviousBeamtimeIdChanged
            ]
            (option
                [ disabled True
                , value ""
                , selected (isNothing selectedChemicalId)
                ]
                [ text "« choose a chemical »" ]
                :: List.map viewPriorChemical priorChemicals.chemicals
            )
        , viewAddedAttributi
        , div [ class "hstack gap-3 mb-3" ]
            [ button
                [ class "btn btn-primary"
                , onClick SubmitCopyFromPreviousBeamtime
                , disabled (isNothing selectedChemicalId)
                , type_ "button"
                ]
                [ icon { name = "plus-lg" }, text " Copy into this beamtime" ]
            , button
                [ class "btn btn-secondary"
                , onClick CancelCopyFromPreviousBeamtime
                , type_ "button"
                ]
                [ icon { name = "x-lg" }, text " Cancel" ]
            ]
        ]


{-| view function for anything that's not the modal
-}
viewInner : Model -> List (Html Msg)
viewInner model =
    case model.chemicals of
        NotAsked ->
            singleton <| text ""

        Loading ->
            singleton <| loadingBar "Loading chemicals..."

        Failure e ->
            singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve chemicals" ], showError e ]

        Success { chemicals, attributi } ->
            let
                prefix =
                    case model.priorChemicals of
                        Success chems ->
                            viewPriorChemicals model attributi chems

                        _ ->
                            case model.editChemical of
                                Nothing ->
                                    div [ class "hstack gap-3" ]
                                        [ button [ class "btn btn-primary", onClick (AddChemical ChemicalTypeCrystal) ] [ viewChemicalTypeIcon ChemicalTypeCrystal, text " Add crystals" ]
                                        , div [ class "vr" ] []
                                        , button [ class "btn btn-primary", onClick (AddChemical ChemicalTypeSolution) ] [ viewChemicalTypeIcon ChemicalTypeSolution, text " Add solution" ]
                                        , div [ class "vr" ] []
                                        , button [ class "btn btn-primary", onClick InitiateCopyFromPreviousBeamtime ] [ icon { name = "terminal" }, text " Copy from prior beamtime" ]
                                        ]

                                Just editChemical ->
                                    viewEditForm chemicals model.fileUploadRequest model.submitErrors model.newFileUpload editChemical

                modifyRequestResult =
                    case model.modifyRequest of
                        NotAsked ->
                            text ""

                        Loading ->
                            p [] [ text "Request in progress..." ]

                        Failure e ->
                            div [] [ makeAlert [ AlertDanger ] [ showError e ] ]

                        Success _ ->
                            div [ class "mt-3" ]
                                [ makeAlert [ AlertSuccess ] [ text "Chemical edited successfully!" ]
                                ]

                deleteRequestResult =
                    case model.chemicalDeleteRequest of
                        NotAsked ->
                            text ""

                        Loading ->
                            p [] [ text "Request in progress..." ]

                        Failure e ->
                            div [] [ makeAlert [ AlertDanger ] [ showError e ] ]

                        Success _ ->
                            div [ class "mt-3" ]
                                [ makeAlert [ AlertSuccess ] [ text "Deletion successful!" ]
                                ]
            in
            [ prefix
            , modifyRequestResult
            , deleteRequestResult
            , viewChemicalTable
                model.chemicalsUsedInRuns
                model.myTimeZone
                (List.sortBy .id chemicals)
                attributi
                model.responsiblePersonFilter
                model.typeFilter
            ]


editChemicalFromAttributiAndValues : Zone -> List (Attributo AttributoType) -> Chemical (Maybe Int) (AttributoMap AttributoValue) a -> Chemical (Maybe Int) EditableAttributiAndOriginal a
editChemicalFromAttributiAndValues zone attributi =
    chemicalMapAttributi (createEditableAttributi zone attributi)


emptyChemical : ChemicalType -> Chemical (Maybe Int) (AttributoMap a) b
emptyChemical type_ =
    { id = Nothing, name = "", responsiblePerson = "", type_ = type_, attributi = emptyAttributoMap, files = [] }


emptyNewFileUpload : { description : String, file : Maybe a }
emptyNewFileUpload =
    { description = "", file = Nothing }


validatePointGroupAndCellDescription : Chemical a EditableAttributiAndOriginal b -> Result (Html msg) ()
validatePointGroupAndCellDescription { attributi } =
    let
        pointGroupResult =
            case findEditableAttributo attributi.editableAttributi "point group" of
                Err _ ->
                    Ok ()

                foundPointGroup ->
                    foundPointGroup |> Result.andThen extractStringAttributo |> Result.andThen validatePointGroup

        cellDescriptionResult =
            case findEditableAttributo attributi.editableAttributi "cell description" of
                Err _ ->
                    Ok ()

                foundCellDescription ->
                    foundCellDescription |> Result.andThen extractStringAttributo |> Result.andThen validateCellDescription
    in
    Result.map (always ()) <| ResultExtra.combine [ pointGroupResult, cellDescriptionResult ]


view : Model -> Html Msg
view model =
    let
        maybeDeleteModal =
            case model.deleteModalOpen of
                Nothing ->
                    []

                Just ( chemicalName, chemicalId ) ->
                    [ Dialog.view
                        (Just
                            { header = Nothing
                            , body = Just (span_ [ text "Really delete chemical ", strongText chemicalName, text "?" ])
                            , closeMessage = Just CancelDelete
                            , containerClass = Nothing
                            , modalDialogClass = Nothing
                            , footer = Just (button [ class "btn btn-danger", onClick (ConfirmDelete chemicalId) ] [ text "Really delete!" ])
                            }
                        )
                    ]
    in
    div [ class "container" ] (maybeDeleteModal ++ viewInner model)


convertChemicalsResponse : JsonReadChemicals -> ChemicalsAndAttributi
convertChemicalsResponse x =
    { chemicals = List.map convertChemicalFromApi x.chemicals
    , attributi = List.map convertAttributoFromApi x.attributi
    }


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        ToggleCreateAttriutiForCopyFromPreviousBeamtime ->
            case model.priorChemicals of
                Success priorChemsSuccess ->
                    ( { model
                        | priorChemicals =
                            Success
                                { priorChemsSuccess
                                    | createAttributi = not priorChemsSuccess.createAttributi
                                }
                      }
                    , Cmd.none
                    )

                _ ->
                    ( model, Cmd.none )

        InitiateCopyFromPreviousBeamtime ->
            ( { model | priorChemicals = Loading }, send PriorChemicalsReceived readAllChemicalsApiAllChemicalsGet )

        CopyFromPreviousBeamtimeIdChanged newId ->
            case model.priorChemicals of
                Success priorChemsSuccess ->
                    ( { model | priorChemicals = Success { priorChemsSuccess | selectedChemicalId = Just newId } }
                    , Cmd.none
                    )

                _ ->
                    ( model, Cmd.none )

        PriorChemicalsReceived response ->
            case response of
                Ok v ->
                    ( { model
                        | priorChemicals =
                            Success
                                { priorChemicals = v
                                , selectedChemicalId = Nothing
                                , submitRequest = NotAsked
                                , createAttributi = False
                                }
                      }
                    , Cmd.none
                    )

                Err e ->
                    ( { model | priorChemicals = Failure e }, Cmd.none )

        SubmitCopyFromPreviousBeamtime ->
            case model.priorChemicals of
                Success priorChemsSuccess ->
                    case priorChemsSuccess.selectedChemicalId of
                        Nothing ->
                            ( model, Cmd.none )

                        Just selectedChemicalId ->
                            ( { model | priorChemicals = Success { priorChemsSuccess | submitRequest = Loading } }
                            , send CopyFromPreviousBeamtimeFinished
                                (copyChemicalApiCopyChemicalPost
                                    { chemicalId = selectedChemicalId
                                    , targetBeamtimeId = model.beamtimeId
                                    , createAttributi = priorChemsSuccess.createAttributi
                                    }
                                )
                            )

                _ ->
                    ( model, Cmd.none )

        CopyFromPreviousBeamtimeFinished finish ->
            case model.priorChemicals of
                Success priorChemsSuccess ->
                    case finish of
                        Err e ->
                            ( { model | priorChemicals = Success { priorChemsSuccess | submitRequest = Failure e } }, Cmd.none )

                        Ok _ ->
                            ( { model | priorChemicals = NotAsked }
                            , getChemicalsAndRuns model.beamtimeId
                            )

                _ ->
                    ( model, Cmd.none )

        CancelCopyFromPreviousBeamtime ->
            ( { model | priorChemicals = NotAsked }, Cmd.none )

        ChemicalsReceived response ->
            ( { model | chemicals = fromResult (Result.map convertChemicalsResponse response) }, Cmd.none )

        -- The user pressed "yes, really delete!" in the modal
        ConfirmDelete chemicalId ->
            ( { model | chemicalDeleteRequest = Loading, deleteModalOpen = Nothing }
            , send ChemicalDeleteFinished (deleteChemicalApiChemicalsDelete (JsonDeleteChemicalInput chemicalId))
            )

        -- The user closed the "Really delete?" modal
        CancelDelete ->
            ( { model | deleteModalOpen = Nothing }, Cmd.none )

        -- The deletion request for an object finished
        ChemicalDeleteFinished result ->
            case result of
                Err e ->
                    ( { model | chemicalDeleteRequest = Failure e }, Cmd.none )

                Ok _ ->
                    ( { model | chemicalDeleteRequest = Success () }
                    , getChemicalsAndRuns model.beamtimeId
                    )

        AddChemical type_ ->
            case model.chemicals of
                Success { attributi } ->
                    ( { model
                        | modifyRequest = NotAsked
                        , chemicalDeleteRequest = NotAsked
                        , editChemical = Just (editChemicalFromAttributiAndValues model.myTimeZone attributi (emptyChemical type_))
                      }
                    , Cmd.none
                    )

                _ ->
                    ( model, Cmd.none )

        -- The name of the object changed
        EditChemicalName newName ->
            case model.editChemical of
                Nothing ->
                    ( model, Cmd.none )

                Just editChemical ->
                    ( { model | editChemical = Just { editChemical | name = newName } }, Cmd.none )

        EditChemicalResponsiblePerson newRP ->
            case model.editChemical of
                Nothing ->
                    ( model, Cmd.none )

                Just editChemical ->
                    ( { model | editChemical = Just { editChemical | responsiblePerson = newRP } }, Cmd.none )

        -- The user pressed the submit change button (either creating or editing an object)
        EditChemicalSubmit ->
            case model.editChemical of
                Nothing ->
                    ( model, Cmd.none )

                Just editChemical ->
                    if model.newFileUpload.description /= "" || isJust model.newFileUpload.file then
                        ( { model | submitErrors = [ text "There is still a file in the upload form. Submit or clear the form!" ] }, Cmd.none )

                    else
                        case validatePointGroupAndCellDescription editChemical of
                            Err errorMessage ->
                                ( { model | submitErrors = [ errorMessage ] }, Cmd.none )

                            Ok _ ->
                                case convertEditValues model.myTimeZone editChemical.attributi of
                                    Err errorList ->
                                        let
                                            attributoIdToName : ChemicalNameDict
                                            attributoIdToName =
                                                List.foldr (\editableAttributo -> Dict.insert editableAttributo.id editableAttributo.name) Dict.empty editChemical.attributi.editableAttributi
                                        in
                                        ( { model
                                            | submitErrors =
                                                List.map
                                                    (\( attributoId, errorMessage ) ->
                                                        text <|
                                                            (Maybe.withDefault "unknown attributo" <| Dict.get attributoId attributoIdToName)
                                                                ++ ": "
                                                                ++ errorMessage
                                                    )
                                                    errorList
                                          }
                                        , Cmd.none
                                        )

                                    Ok editedAttributi ->
                                        case editChemical.id of
                                            Just editId ->
                                                let
                                                    chemicalToSend : JsonChemicalWithId
                                                    chemicalToSend =
                                                        { id = editId
                                                        , name = editChemical.name
                                                        , responsiblePerson = editChemical.responsiblePerson
                                                        , chemicalType = chemicalTypeToApi editChemical.type_
                                                        , attributi = attributoMapToListOfAttributi editedAttributi
                                                        , fileIds = List.map .id editChemical.files
                                                        , beamtimeId = model.beamtimeId
                                                        }
                                                in
                                                ( { model | modifyRequest = Loading }
                                                , send (EditChemicalFinished << Result.map (always {})) (updateChemicalApiChemicalsPatch chemicalToSend)
                                                )

                                            Nothing ->
                                                let
                                                    chemicalToSend : JsonChemicalWithoutId
                                                    chemicalToSend =
                                                        { name = editChemical.name
                                                        , responsiblePerson = editChemical.responsiblePerson
                                                        , chemicalType = chemicalTypeToApi editChemical.type_
                                                        , attributi = attributoMapToListOfAttributi editedAttributi
                                                        , fileIds = List.map .id editChemical.files
                                                        , beamtimeId = model.beamtimeId
                                                        }
                                                in
                                                ( { model | modifyRequest = Loading }
                                                  -- Result.map (always {}) to get rid of the create output, since we don't need the ID
                                                , send (EditChemicalFinished << Result.map (always {})) (createChemicalApiChemicalsPost chemicalToSend)
                                                )

        EditChemicalCancel ->
            ( { model
                | editChemical = Nothing
                , newFileUpload = emptyNewFileUpload
                , fileUploadRequest = NotAsked
                , modifyRequest = NotAsked
                , chemicalDeleteRequest = NotAsked
                , submitErrors = []
              }
            , Cmd.none
            )

        EditChemicalFinished result ->
            case result of
                Err e ->
                    ( { model | modifyRequest = Failure e }, Cmd.none )

                Ok _ ->
                    ( { model
                        | modifyRequest = Success ()
                        , editChemical = Nothing
                        , fileUploadRequest = NotAsked
                        , chemicalDeleteRequest = NotAsked
                        , submitErrors = []
                      }
                    , getChemicalsAndRuns model.beamtimeId
                    )

        EditChemicalAttributo v ->
            case model.editChemical of
                -- This is the unlikely case that we have an "attributo was edited" message, but chemical is edited
                Nothing ->
                    ( model, Cmd.none )

                Just editChemical ->
                    let
                        newEditable =
                            editEditableAttributi editChemical.attributi.editableAttributi v

                        newChemical =
                            { editChemical | attributi = { originalAttributi = editChemical.attributi.originalAttributi, editableAttributi = newEditable } }
                    in
                    ( { model | editChemical = Just newChemical }, Cmd.none )

        AskDelete chemicalName chemicalId ->
            ( { model | deleteModalOpen = Just ( chemicalName, chemicalId ) }, Cmd.none )

        InitiateEdit chemical ->
            case model.chemicals of
                Success { attributi } ->
                    ( { model
                        | modifyRequest = NotAsked
                        , chemicalDeleteRequest = NotAsked
                        , editChemical = Just (editChemicalFromAttributiAndValues model.myTimeZone attributi (chemicalMapId Just chemical))
                      }
                    , scrollToTop (always Nop)
                    )

                _ ->
                    ( model, Cmd.none )

        InitiateClone chemical ->
            case model.chemicals of
                Success { attributi } ->
                    let
                        -- This code is a bit too long for its own
                        -- good, but the premise is simple: Usually
                        -- we're storing all attributi in the chemical
                        -- together with the information: has this
                        -- attributi been changed by the user. Then we
                        -- use this information to only transmit the
                        -- changes, not the whole set of attributi.
                        -- However, if we duplicate a chemical, all
                        -- attributi are automatically new, thus
                        -- changed. So we have to dive deep into this
                        -- structure and change the edit status.
                        editChemicalOriginal =
                            editChemicalFromAttributiAndValues model.myTimeZone attributi (chemicalMapId (always Nothing) chemical)

                        mapEditableAttributo : EditableAttributo -> EditableAttributo
                        mapEditableAttributo =
                            mapAttributo (\attributoEditStatusAndEditValue -> { attributoEditStatusAndEditValue | editStatus = Edited })

                        makeAlreadyEdited : EditableAttributiAndOriginal -> EditableAttributiAndOriginal
                        makeAlreadyEdited editAndOrig =
                            { editAndOrig | editableAttributi = List.map mapEditableAttributo editAndOrig.editableAttributi }

                        editChemicalWithAllEdited =
                            chemicalMapAttributi makeAlreadyEdited editChemicalOriginal
                    in
                    ( { model
                        | modifyRequest = NotAsked
                        , chemicalDeleteRequest = NotAsked
                        , editChemical = Just editChemicalWithAllEdited
                      }
                    , scrollToTop (always Nop)
                    )

                _ ->
                    ( model, Cmd.none )

        EditNewFileDescription newDescription ->
            let
                oldFileUpload =
                    model.newFileUpload
            in
            ( { model | newFileUpload = { oldFileUpload | description = newDescription } }, Cmd.none )

        EditNewFileFile newFile ->
            ( model, Task.perform (EditNewFileWithBytes << FileWithBytes newFile) (ElmFile.toBytes newFile) )

        EditNewFileWithBytes fwb ->
            let
                oldFileUpload =
                    model.newFileUpload
            in
            ( { model | newFileUpload = { oldFileUpload | file = Just fwb } }, Cmd.none )

        EditFileUpload ->
            case model.newFileUpload.file of
                Nothing ->
                    ( model, Cmd.none )

                Just fileToUpload ->
                    ( { model | fileUploadRequest = Loading }
                    , send EditFileUploadFinished (createFileApiFilesPost fileToUpload.fileMetadata model.newFileUpload.description "False")
                    )

        EditFileUploadFinished result ->
            case result of
                Err e ->
                    ( { model | fileUploadRequest = Failure e }, Cmd.none )

                Ok file ->
                    let
                        newEditChemical =
                            Maybe.map
                                (\editChemical ->
                                    let
                                        newEditChemicalFile : JsonFileOutput
                                        newEditChemicalFile =
                                            { id = file.id
                                            , type__ = file.type__
                                            , description = file.description
                                            , fileName = file.fileName
                                            , originalPath = file.originalPath
                                            , sizeInBytes = file.sizeInBytes
                                            }
                                    in
                                    { editChemical | files = newEditChemicalFile :: editChemical.files }
                                )
                                model.editChemical
                    in
                    ( { model | fileUploadRequest = Success (), newFileUpload = emptyNewFileUpload, editChemical = newEditChemical }, Cmd.none )

        EditFileDelete toDeleteId ->
            case model.editChemical of
                Nothing ->
                    ( model, Cmd.none )

                Just editChemical ->
                    let
                        newEditChemical =
                            Just { editChemical | files = List.filter (\x -> x.id /= toDeleteId) editChemical.files }
                    in
                    ( { model | editChemical = newEditChemical, fileUploadRequest = NotAsked }, Cmd.none )

        EditResetNewFileUpload ->
            ( { model | newFileUpload = emptyNewFileUpload }, Cmd.none )

        EditNewFileOpenSelector ->
            ( model, File.Select.file [] EditNewFileFile )

        Nop ->
            ( model, Cmd.none )

        RunsReceived runsResponse ->
            let
                chemicalAttributoId : Attributo AttributoType -> Maybe AttributoId
                chemicalAttributoId aa =
                    case aa.type_ of
                        ChemicalId ->
                            Just aa.id

                        _ ->
                            Nothing

                valuesFromAttributo : List JsonRun -> AttributoId -> List AttributoValue
                valuesFromAttributo rrs mattrid =
                    List.filterMap (Dict.get mattrid) (List.map (convertAttributoMapFromApi << .attributi) rrs)

                chemicalsUsedInRuns =
                    case runsResponse of
                        Ok rr ->
                            Set.fromList <|
                                List.filterMap extractChemical <|
                                    List.concatMap (valuesFromAttributo rr.runs) <|
                                        List.filterMap (chemicalAttributoId << convertAttributoFromApi) rr.attributi

                        _ ->
                            Set.empty
            in
            ( { model | chemicalsUsedInRuns = chemicalsUsedInRuns }, Cmd.none )

        ChangeResponsiblePersonFilter newFilter ->
            ( { model | responsiblePersonFilter = newFilter }, Cmd.none )

        ChangeTypeFilter newFilter ->
            ( { model | typeFilter = newFilter }, Cmd.none )
