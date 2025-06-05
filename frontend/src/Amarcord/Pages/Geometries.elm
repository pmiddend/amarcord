module Amarcord.Pages.Geometries exposing (Model, Msg, init, pageTitle, update, view)

import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.Bootstrap exposing (AlertProperty(..), icon, loadingBar, makeAlert)
import Amarcord.Dialog as Dialog
import Amarcord.GeometryMetadata exposing (GeometryId(..), geometryIdToInt)
import Amarcord.Html exposing (code_, div_, em_, form_, h2_, h4_, input_, li_, onIntInput, small_, span_, strongText, sup_, tbody_, td_, th_, thead_, tr_, ul_)
import Amarcord.HttpError exposing (HttpError, send, showError)
import Amarcord.Util exposing (deadEndToString, monthToNumericString, scrollToTop)
import Api.Data exposing (JsonAttributoOutput, JsonGeometryWithoutContent, JsonReadGeometriesForAllBeamtimes, JsonReadGeometriesForSingleBeamtime, JsonReadSingleGeometryOutput)
import Api.Request.Geometries exposing (copyToBeamtimeApiGeometryCopyToBeamtimePost, createGeometryApiGeometriesPost, deleteSingleGeometryApiGeometriesGeometryIdDelete, readGeometriesForAllBeamtimesApiAllGeometriesGet, readGeometriesForSingleBeamtimeApiGeometryForBeamtimeBeamtimeIdGet, readSingleGeometryApiGeometriesGeometryIdGet, updateGeometryApiGeometriesGeometryIdPatch)
import Dict
import Html exposing (..)
import Html.Attributes exposing (attribute, class, disabled, for, id, selected, style, type_, value)
import Html.Events exposing (onClick, onInput)
import List exposing (isEmpty, singleton, sortBy)
import List.Extra
import Maybe.Extra exposing (isNothing)
import Mustache
import RemoteData exposing (RemoteData(..), fromResult, isFailure, isLoading, isNotAsked)
import Set
import String
import Time exposing (millisToPosix, toMonth, toYear, utc)


type alias CopyPriorData =
    { priorGeometries : JsonReadGeometriesForAllBeamtimes
    , selectedId : Maybe GeometryId
    , submitRequest : RemoteData HttpError JsonReadGeometriesForSingleBeamtime
    }


type alias EditableGeometry =
    { id : Maybe GeometryId
    , name : String
    , content : String
    }


type alias Model =
    { geometries : RemoteData HttpError JsonReadGeometriesForSingleBeamtime
    , priorGeometries : RemoteData HttpError CopyPriorData
    , deleteModalOpen : Maybe ( String, GeometryId )
    , geometryDeleteRequest : RemoteData HttpError ()
    , editGeometry : RemoteData HttpError EditableGeometry
    , modifyRequest : RemoteData HttpError ()
    , beamtimeId : BeamtimeId
    , templateErrors : RemoteData (List String) ()
    }


pageTitle : Model -> String
pageTitle _ =
    "Geometries"


type Msg
    = GeometriesReceived (Result HttpError JsonReadGeometriesForSingleBeamtime)
    | PriorGeometriesReceived (Result HttpError JsonReadGeometriesForAllBeamtimes)
    | InitiateCopyFromPreviousBeamtime
    | CancelCopyFromPreviousBeamtime
    | SubmitCopyFromPreviousBeamtime
    | CopyFromPreviousBeamtimeIdChanged GeometryId
    | CopyFromPreviousBeamtimeFinished (Result HttpError JsonGeometryWithoutContent)
    | CancelDelete
    | AskDelete String GeometryId
    | InitiateEdit JsonGeometryWithoutContent
    | InitiateEditComplete JsonGeometryWithoutContent (Result HttpError JsonReadSingleGeometryOutput)
    | InitiateClone JsonGeometryWithoutContent
    | InitiateCloneComplete JsonGeometryWithoutContent (Result HttpError JsonReadSingleGeometryOutput)
    | ConfirmDelete GeometryId
    | AddGeometry
    | GeometryDeleteFinished (Result HttpError JsonReadGeometriesForSingleBeamtime)
    | EditGeometryName String
    | EditGeometryContent String
    | EditGeometrySubmit
    | EditGeometryCheck
    | EditGeometryFinished (Result HttpError {})
    | EditGeometryCancel
    | Nop


init : BeamtimeId -> ( Model, Cmd Msg )
init beamtimeId =
    ( { geometries = Loading
      , priorGeometries = NotAsked
      , deleteModalOpen = Nothing
      , geometryDeleteRequest = NotAsked
      , modifyRequest = NotAsked
      , editGeometry = NotAsked
      , beamtimeId = beamtimeId
      , templateErrors = NotAsked
      }
    , getGeometries beamtimeId
    )


getGeometries : BeamtimeId -> Cmd Msg
getGeometries beamtimeId =
    send GeometriesReceived (readGeometriesForSingleBeamtimeApiGeometryForBeamtimeBeamtimeIdGet beamtimeId)


viewEditForm : List JsonGeometryWithoutContent -> Set.Set Int -> EditableGeometry -> RemoteData (List String) () -> Html Msg
viewEditForm geometries usages editingGeometry templateErrors =
    let
        otherGeometriesNames =
            List.map .name <|
                case editingGeometry.id of
                    Nothing ->
                        geometries

                    Just editingId ->
                        List.filter (\s -> not <| GeometryId s.id == editingId) geometries

        isDuplicateName =
            otherGeometriesNames
                |> List.map String.trim
                |> List.member (String.trim editingGeometry.name)

        isValidGeometryName =
            not isDuplicateName
                && (String.trim editingGeometry.name /= "")

        isInvalidGeometryNameStyle =
            if not isValidGeometryName then
                " is-invalid"

            else
                ""

        addOrEditHeadline =
            h4_
                [ text
                    (if isNothing editingGeometry.id then
                        "Add new geometry"

                     else
                        "Edit geometry"
                    )
                ]

        isUsed =
            editingGeometry.id |> Maybe.map (\gid -> Set.member (geometryIdToInt gid) usages) |> Maybe.withDefault False
    in
    form_ <|
        [ addOrEditHeadline
        , div [ class "mb-3" ]
            [ label [ for "name", class "form-label" ] [ text "Name", sup_ [ text "*" ] ]
            , input_
                [ type_ "text"
                , class
                    ("form-control"
                        ++ isInvalidGeometryNameStyle
                    )
                , id "name"
                , value editingGeometry.name
                , onInput EditGeometryName
                ]
            , if String.trim editingGeometry.name /= "" then
                if isDuplicateName then
                    div [ class "invalid-feedback" ] [ text "Name already used" ]

                else
                    text ""

              else
                div [ class "invalid-feedback" ] [ text "Name is mandatory" ]
            ]
        , div [ class "mb-3" ]
            [ label [ for "content", class "form-label" ] [ text "Content" ]
            , div [ class "form-text mb-2" ]
                [ text "You can use placeholders for Attributo values like "
                , code_ [ text "{{detector_distance}}" ]
                , text "."
                ]
            , if isUsed then
                div_
                    [ div [ class "form-text" ] [ small_ [ text "Geometry is in use; cannot change content anymore." ] ]
                    , pre [ class "text-bg-light shadow-sm" ] [ text editingGeometry.content ]
                    ]

              else
                textarea
                    [ id "content"
                    , class "form-control"
                    , value editingGeometry.content
                    , onInput EditGeometryContent
                    , style "height" "20em"
                    ]
                    [ text editingGeometry.content ]
            ]
        , button
            [ class "btn btn-primary me-3 mb-3"
            , onClick
                (case templateErrors of
                    Success _ ->
                        EditGeometrySubmit

                    _ ->
                        EditGeometryCheck
                )
            , disabled (not isValidGeometryName || isFailure templateErrors)
            , type_ "button"
            ]
            [ icon { name = "plus-lg" }
            , text
                (if isNotAsked templateErrors then
                    " Check content"

                 else if isNothing editingGeometry.id then
                    " Save"

                 else
                    " Confirm edit"
                )
            ]
        , button
            [ class "btn btn-secondary me-3 mb-3"
            , onClick EditGeometryCancel
            , type_ "button"
            ]
            [ icon { name = "x-lg" }, text " Cancel" ]
        , case templateErrors of
            Failure errors ->
                div [ class "form-text text-danger" ] [ em_ [ text (String.join "\n" errors) ] ]

            Success _ ->
                div [ class "form-text text-success" ] [ em_ [ text "Content looks good!" ] ]

            _ ->
                text ""
        ]


viewGeometryRow : Set.Set Int -> Dict.Dict Int String -> JsonGeometryWithoutContent -> List (Html Msg)
viewGeometryRow usages attributiById geometry =
    let
        isUsed =
            Set.member geometry.id usages
    in
    [ tr_
        [ td_
            [ div [ class "hstack gap-1" ]
                [ button [ class "btn btn-sm btn-outline-secondary text-nowrap", onClick (InitiateEdit geometry) ]
                    [ icon { name = "pencil-square" }
                    , text " Edit"
                    ]
                , button [ class "btn btn-sm btn-outline-info text-nowrap", onClick (InitiateClone geometry) ]
                    [ icon { name = "terminal" }
                    , text " Duplicate"
                    ]
                , button
                    [ class "btn btn-sm btn-outline-danger text-nowrap"
                    , onClick (AskDelete geometry.name (GeometryId geometry.id))
                    , disabled isUsed
                    ]
                    [ icon { name = "trash" }, text " Delete" ]
                ]
            ]
        , td_ [ text (String.fromInt geometry.id) ]
        , td [ class "w-50" ] [ text geometry.name ]
        , td [ class "w-50" ] [ ul_ (List.filterMap (\a -> Dict.get a attributiById |> Maybe.map (\aname -> li_ [ text aname ])) geometry.attributi) ]
        ]
    ]


viewGeometryTable : List JsonAttributoOutput -> List JsonGeometryWithoutContent -> Set.Set Int -> Html Msg
viewGeometryTable attributi geometries usages =
    if isEmpty geometries then
        div [ class "mt-3" ] [ h2 [ class "text-muted" ] [ text "No geometries entered yet." ] ]

    else
        div [ class "mt-3" ]
            [ h2_ [ text "Available geometries" ]
            , table [ class "table table-striped" ]
                [ thead_ [ tr_ [ th_ [], th_ [ text "ID" ], th_ [ text "Name" ], th_ [ text "Attributi" ] ] ]
                , let
                    attributiById : Dict.Dict Int String
                    attributiById =
                        List.foldr (\a -> Dict.insert a.id a.name) Dict.empty attributi
                  in
                  tbody_ (List.concatMap (viewGeometryRow usages attributiById) geometries)
                ]
            ]


viewPriorGeometries : CopyPriorData -> Html Msg
viewPriorGeometries { priorGeometries, selectedId, submitRequest } =
    let
        groupedGeometriesByBeamtime : List ( JsonGeometryWithoutContent, List JsonGeometryWithoutContent )
        groupedGeometriesByBeamtime =
            List.Extra.groupWhile (\geomA geomB -> geomA.beamtimeId == geomB.beamtimeId) (List.reverse (sortBy (\geom -> geom.beamtimeId) priorGeometries.geometries))

        viewPriorGeometry { id, name, beamtimeId } =
            let
                title =
                    case List.Extra.find (\bt -> bt.id == beamtimeId) priorGeometries.beamtimes of
                        Nothing ->
                            name

                        Just bt ->
                            let
                                startAsPosix =
                                    millisToPosix bt.startLocal
                            in
                            name
                                ++ " / "
                                ++ String.fromInt
                                    (toYear utc startAsPosix)
                                ++ "-"
                                ++ monthToNumericString (toMonth utc startAsPosix)
            in
            option
                [ value (String.fromInt id)
                , selected (selectedId == Just (GeometryId id))
                ]
                [ text title ]

        viewOptgroup : ( JsonGeometryWithoutContent, List JsonGeometryWithoutContent ) -> Html msg
        viewOptgroup ( representative, elements ) =
            let
                beamtimeLabel : String
                beamtimeLabel =
                    case List.Extra.find (\bt -> bt.id == representative.beamtimeId) priorGeometries.beamtimes of
                        Nothing ->
                            ""

                        Just bt ->
                            let
                                startAsPosix =
                                    millisToPosix bt.startLocal
                            in
                            bt.title
                                ++ " / "
                                ++ String.fromInt
                                    (toYear utc startAsPosix)
                                ++ "-"
                                ++ monthToNumericString (toMonth utc startAsPosix)
            in
            optgroup [ attribute "label" beamtimeLabel ]
                (List.map viewPriorGeometry
                    (List.reverse (List.sortBy (\geom -> geom.created) elements))
                )
    in
    form_ <|
        [ h4_ [ icon { name = "terminal" }, text " Select geometry to copy" ]
        , select
            [ id "geometry-to-copy"
            , class "form-select mb-3"
            , onIntInput (CopyFromPreviousBeamtimeIdChanged << GeometryId)
            ]
            (option
                [ disabled True
                , value ""
                , selected (isNothing selectedId)
                ]
                [ text "« choose a geometry »" ]
                :: List.map viewOptgroup groupedGeometriesByBeamtime
            )
        , div [ class "hstack gap-3 mb-3" ]
            [ button
                [ class "btn btn-primary"
                , onClick SubmitCopyFromPreviousBeamtime
                , disabled (isNothing selectedId || isLoading submitRequest)
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
    case model.geometries of
        NotAsked ->
            singleton <| text ""

        Loading ->
            singleton <| loadingBar "Loading geometries..."

        Failure e ->
            singleton <| makeAlert [ AlertDanger ] <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve geometries" ], showError e ]

        Success { geometries, geometryWithUsage, attributi } ->
            let
                usagesSet =
                    List.foldr
                        (\{ geometryId, usages } ->
                            if usages > 0 then
                                Set.insert geometryId

                            else
                                identity
                        )
                        Set.empty
                        geometryWithUsage

                prefix =
                    case model.priorGeometries of
                        Success priorGeometriesAndBeamtimes ->
                            viewPriorGeometries priorGeometriesAndBeamtimes

                        _ ->
                            case model.editGeometry of
                                Success editGeometry ->
                                    viewEditForm geometries usagesSet editGeometry model.templateErrors

                                Loading ->
                                    loadingBar "Loading geometry..."

                                _ ->
                                    div [ class "hstack gap-3" ]
                                        [ button [ class "btn btn-primary", onClick AddGeometry ] [ text " Add geometry" ]
                                        , div [ class "vr" ] []
                                        , button [ class "btn btn-primary", onClick InitiateCopyFromPreviousBeamtime ] [ icon { name = "terminal" }, text " Copy from prior beamtime" ]
                                        ]

                modifyRequestResult =
                    case model.modifyRequest of
                        NotAsked ->
                            text ""

                        Loading ->
                            p [] [ text "Edit request in progress..." ]

                        Failure e ->
                            div [] [ makeAlert [ AlertDanger ] [ showError e ] ]

                        Success _ ->
                            div [ class "mt-3" ]
                                [ makeAlert [ AlertSuccess ] [ text "Geometry edited successfully!" ]
                                ]

                deleteRequestResult =
                    case model.geometryDeleteRequest of
                        NotAsked ->
                            text ""

                        Loading ->
                            p [] [ text "Delete request in progress..." ]

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
            , viewGeometryTable attributi geometries usagesSet
            ]


view : Model -> Html Msg
view model =
    let
        maybeDeleteModal =
            case model.deleteModalOpen of
                Nothing ->
                    []

                Just ( geometryName, geometryId ) ->
                    [ Dialog.view
                        (Just
                            { header = Nothing
                            , body = Just (span_ [ text "Really delete geometry ", strongText geometryName, text "?" ])
                            , closeMessage = Just CancelDelete
                            , containerClass = Nothing
                            , modalDialogClass = Nothing
                            , footer = Just (button [ class "btn btn-danger", onClick (ConfirmDelete geometryId) ] [ text "Really delete!" ])
                            }
                        )
                    ]
    in
    div [ class "container" ] (maybeDeleteModal ++ viewInner model)


findVariablesInMustache : Mustache.Ast -> List String
findVariablesInMustache nodes =
    let
        findVariable : Mustache.AstNode -> List String
        findVariable x =
            case x of
                Mustache.Variable { name } ->
                    [ String.join "." name ]

                Mustache.Section { subsection } ->
                    List.concatMap findVariable subsection

                Mustache.InvertedSection { subsection } ->
                    List.concatMap findVariable subsection

                _ ->
                    []
    in
    List.concatMap findVariable nodes


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        EditGeometryCheck ->
            case model.geometries of
                Success { attributi } ->
                    case model.editGeometry of
                        Success { content } ->
                            case Mustache.parse content of
                                Err deadEnds ->
                                    ( { model | templateErrors = Failure (List.map deadEndToString deadEnds) }, Cmd.none )

                                Ok ast ->
                                    let
                                        attributiNames =
                                            List.foldr (\{ name } -> Set.insert name) Set.empty attributi

                                        nonExistingVariables =
                                            List.filter (\variableName -> not (Set.member variableName attributiNames)) (findVariablesInMustache ast)
                                    in
                                    if List.isEmpty nonExistingVariables then
                                        ( { model | templateErrors = Success () }, Cmd.none )

                                    else
                                        ( { model
                                            | templateErrors =
                                                Failure
                                                    [ "The following placeholders in the geometry are not valid attributi: "
                                                        ++ String.join "," nonExistingVariables
                                                    ]
                                          }
                                        , Cmd.none
                                        )

                        _ ->
                            ( model, Cmd.none )

                _ ->
                    ( model, Cmd.none )

        InitiateCopyFromPreviousBeamtime ->
            ( { model | priorGeometries = Loading }
            , send PriorGeometriesReceived readGeometriesForAllBeamtimesApiAllGeometriesGet
            )

        CopyFromPreviousBeamtimeIdChanged newId ->
            case model.priorGeometries of
                Success priorSuccess ->
                    ( { model | priorGeometries = Success { priorSuccess | selectedId = Just newId } }
                    , Cmd.none
                    )

                _ ->
                    ( model, Cmd.none )

        PriorGeometriesReceived response ->
            case response of
                Ok v ->
                    ( { model
                        | priorGeometries =
                            Success
                                { priorGeometries = v
                                , selectedId = Nothing
                                , submitRequest = NotAsked
                                }
                      }
                    , Cmd.none
                    )

                Err e ->
                    ( { model | priorGeometries = Failure e }, Cmd.none )

        SubmitCopyFromPreviousBeamtime ->
            case model.priorGeometries of
                Success priorChemsSuccess ->
                    case priorChemsSuccess.selectedId of
                        Nothing ->
                            ( model, Cmd.none )

                        Just selectedId ->
                            ( { model | priorGeometries = Success { priorChemsSuccess | submitRequest = Loading } }
                            , send CopyFromPreviousBeamtimeFinished
                                (copyToBeamtimeApiGeometryCopyToBeamtimePost
                                    { geometryId = geometryIdToInt selectedId
                                    , targetBeamtimeId = model.beamtimeId
                                    }
                                )
                            )

                _ ->
                    ( model, Cmd.none )

        CopyFromPreviousBeamtimeFinished finish ->
            case model.priorGeometries of
                Success priorChemsSuccess ->
                    case finish of
                        Err e ->
                            ( { model | priorGeometries = Success { priorChemsSuccess | submitRequest = Failure e } }, Cmd.none )

                        Ok _ ->
                            ( { model | priorGeometries = NotAsked }
                            , getGeometries model.beamtimeId
                            )

                _ ->
                    ( model, Cmd.none )

        CancelCopyFromPreviousBeamtime ->
            ( { model | priorGeometries = NotAsked }, Cmd.none )

        GeometriesReceived response ->
            ( { model | geometries = fromResult response }, Cmd.none )

        -- The user pressed "yes, really delete!" in the modal
        ConfirmDelete geometryId ->
            ( { model | geometryDeleteRequest = Loading, deleteModalOpen = Nothing }
            , send GeometryDeleteFinished (deleteSingleGeometryApiGeometriesGeometryIdDelete (geometryIdToInt geometryId))
            )

        -- The user closed the "Really delete?" modal
        CancelDelete ->
            ( { model | deleteModalOpen = Nothing }, Cmd.none )

        -- The deletion request for an object finished
        GeometryDeleteFinished result ->
            case result of
                Err e ->
                    ( { model | geometryDeleteRequest = Failure e }, Cmd.none )

                Ok _ ->
                    ( { model | geometryDeleteRequest = Success () }
                    , getGeometries model.beamtimeId
                    )

        AddGeometry ->
            case model.geometries of
                Success _ ->
                    ( { model
                        | modifyRequest = NotAsked
                        , geometryDeleteRequest = NotAsked
                        , editGeometry = Success { id = Nothing, name = "", content = "" }
                      }
                    , Cmd.none
                    )

                _ ->
                    ( model, Cmd.none )

        EditGeometryName newName ->
            case model.editGeometry of
                Success editGeometry ->
                    ( { model | editGeometry = Success { editGeometry | name = newName } }, Cmd.none )

                _ ->
                    ( model, Cmd.none )

        -- The user pressed the submit change button (either creating or editing an object)
        EditGeometrySubmit ->
            case model.editGeometry of
                Success editGeometry ->
                    case editGeometry.id of
                        Nothing ->
                            ( { model | modifyRequest = Loading }
                            , send (EditGeometryFinished << Result.map (always {}))
                                (createGeometryApiGeometriesPost
                                    { content = editGeometry.content
                                    , name = editGeometry.name
                                    , beamtimeId = model.beamtimeId
                                    }
                                )
                            )

                        Just priorId ->
                            ( { model | modifyRequest = Loading }
                            , send (EditGeometryFinished << Result.map (always {}))
                                (updateGeometryApiGeometriesGeometryIdPatch (geometryIdToInt priorId)
                                    { content = editGeometry.content
                                    , name = editGeometry.name
                                    }
                                )
                            )

                _ ->
                    ( model, Cmd.none )

        EditGeometryCancel ->
            ( { model
                | editGeometry = NotAsked
                , modifyRequest = NotAsked
                , geometryDeleteRequest = NotAsked
              }
            , Cmd.none
            )

        EditGeometryFinished result ->
            case result of
                Err e ->
                    ( { model | modifyRequest = Failure e }, Cmd.none )

                Ok _ ->
                    ( { model
                        | modifyRequest = Success ()
                        , editGeometry = NotAsked
                        , geometryDeleteRequest = NotAsked
                      }
                    , getGeometries model.beamtimeId
                    )

        AskDelete geometryName geometryId ->
            ( { model | deleteModalOpen = Just ( geometryName, geometryId ) }, Cmd.none )

        EditGeometryContent newContent ->
            case model.editGeometry of
                Success geom ->
                    ( { model | editGeometry = Success { geom | content = newContent }, templateErrors = NotAsked }, Cmd.none )

                _ ->
                    ( model, Cmd.none )

        InitiateEditComplete geometry geometryContent ->
            case geometryContent of
                Err e ->
                    ( { model | editGeometry = Failure e }, Cmd.none )

                Ok { content } ->
                    ( { model
                        | editGeometry =
                            Success
                                { id = Just (GeometryId geometry.id)
                                , name = geometry.name
                                , content = content
                                }
                      }
                    , scrollToTop (always Nop)
                    )

        InitiateEdit geometry ->
            ( { model | editGeometry = Loading }
            , send (InitiateEditComplete geometry) (readSingleGeometryApiGeometriesGeometryIdGet geometry.id)
            )

        InitiateClone geometry ->
            ( { model | editGeometry = Loading }
            , send (InitiateCloneComplete geometry) (readSingleGeometryApiGeometriesGeometryIdGet geometry.id)
            )

        InitiateCloneComplete geometry geometryContent ->
            case geometryContent of
                Err e ->
                    ( { model | editGeometry = Failure e }, Cmd.none )

                Ok { content } ->
                    ( { model
                        | editGeometry =
                            Success
                                { id = Nothing
                                , name = geometry.name
                                , content = content
                                }
                      }
                    , scrollToTop (always Nop)
                    )

        Nop ->
            ( model, Cmd.none )
