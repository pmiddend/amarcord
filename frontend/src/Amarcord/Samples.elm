module Amarcord.Samples exposing (..)

import Amarcord.Attributo as Attributo exposing (Attributo, AttributoMap, AttributoName(..), AttributoType(..), AttributoValue(..), attributoDecoder, attributoMapDecoder, attributoTypeDecoder, createAnnotatedAttributoMap, emptyAttributoMap, encodeAttributoMap, fromAttributoName, mapAttributo, retrieveAttributoValue, toAttributoName, updateAttributoMap)
import Amarcord.Bootstrap exposing (AlertType(..), icon, makeAlert, showHttpError)
import Amarcord.Dialog as Dialog
import Amarcord.File exposing (File, fileDecoder, httpCreateFile)
import Amarcord.Html exposing (br_, form_, h4_, h5_, input_, li_, p_, span_, strongText, tbody_, td_, th_, thead_, tr_)
import Amarcord.NumericRange exposing (NumericRange, emptyNumericRange, numericRangeToString, valueInRange)
import Amarcord.Route exposing (makeFilesLink)
import Amarcord.UserError exposing (UserError, userErrorDecoder)
import Amarcord.Util exposing (collectResults, formatPosixDateTimeCompatible, formatPosixHumanFriendly, httpDelete, httpPatch)
import Dict exposing (Dict)
import File as ElmFile
import File.Select
import Html exposing (..)
import Html.Attributes exposing (attribute, checked, class, disabled, for, href, id, selected, step, style, type_, value)
import Html.Events exposing (onClick, onInput)
import Http exposing (jsonBody)
import Iso8601 exposing (toTime)
import Json.Decode as Decode
import Json.Encode as Encode
import List exposing (intersperse, length, singleton)
import List.Extra as List
import Maybe exposing (withDefault)
import Maybe.Extra as Maybe exposing (isJust, isNothing, traverse, unwrap)
import RemoteData exposing (RemoteData(..), fromResult)
import Set
import String exposing (fromFloat, fromInt, join, split, toInt, trim)
import Task
import Time exposing (Month(..), Posix, Zone, here)
import Tuple exposing (first, second)


type EditStatus
    = Edited
    | Unchanged


type AttributoEditValue
    = EditValueInt String
    | EditValueDateTime String
    | EditValueBoolean Bool
    | EditValueSampleId
    | EditValueString String
    | EditValueList
        { subType : AttributoType
        , minLength : Maybe Int
        , maxLength : Maybe Int
        , editValue : String
        }
    | EditValueNumber
        { range : NumericRange
        , suffix : Maybe String
        , standardUnit : Bool
        , editValue : String
        }
    | EditValueChoice { choiceValues : List String, editValue : String }


type alias Sample idType attributiType fileType =
    { id : idType
    , name : String
    , attributi : attributiType
    , files : List fileType
    }


sampleMapAttributi : (b -> c) -> Sample a b x -> Sample a c x
sampleMapAttributi f { id, name, attributi, files } =
    { id = id, name = name, attributi = f attributi, files = files }


sampleMapId : (a -> b) -> Sample a c x -> Sample b c x
sampleMapId f { id, name, attributi, files } =
    { id = f id, name = name, attributi = attributi, files = files }


type alias EditableAttributo =
    Attributo ( EditStatus, AttributoEditValue )


type alias EditableAttributi =
    List EditableAttributo


type alias EditableAttributiAndOriginal =
    { editableAttributi : EditableAttributi
    , originalAttributi : AttributoMap AttributoValue
    }


type alias SamplesAndAttributi =
    { samples : List (Sample SampleId (AttributoMap AttributoValue) File)
    , attributi : List (Attributo AttributoType)
    }


type alias NewFileUpload =
    { file : Maybe ElmFile.File
    , description : String
    }


type alias Model =
    { samples : RemoteData Http.Error SamplesAndAttributi
    , deleteModalOpen : Maybe ( String, SampleId )
    , sampleDeleteRequest : RemoteData Http.Error ()
    , fileUploadRequest : RemoteData Http.Error ()
    , editSample : Maybe (Sample (Maybe Int) EditableAttributiAndOriginal File)
    , modifyRequest : RemoteData Http.Error ()
    , myTimeZone : Maybe Zone
    , submitErrors : List String
    , newFileUpload : NewFileUpload
    }


type alias SamplesResponse =
    Result Http.Error ( List (Sample SampleId (AttributoMap AttributoValue) File), List (Attributo AttributoType) )


type ValueUpdate
    = SetValue AttributoEditValue
    | IgnoreValue


type alias SampleId =
    Int


type Msg
    = SamplesReceived SamplesResponse
    | CancelDelete
    | TimeZoneReceived Zone
    | AskDelete String SampleId
    | InitiateEdit (Sample Int (AttributoMap AttributoValue) File)
    | ConfirmDelete SampleId
    | AddSample
    | EditSampleName String
    | SampleDeleteFinished (Result Http.Error (Maybe UserError))
    | EditSampleSubmit
    | EditSampleCancel
    | EditSampleFinished (Result Http.Error (Maybe UserError))
    | EditSampleAttributo AttributoName ValueUpdate
    | EditNewFileDescription String
    | EditNewFileFile ElmFile.File
    | EditFileUpload
    | EditFileUploadFinished (Result Http.Error File)
    | EditFileDelete Int
    | EditResetNewFileUpload
    | EditNewFileOpenSelector


sampleDecoder : Decode.Decoder (Sample SampleId (AttributoMap AttributoValue) File)
sampleDecoder =
    Decode.map4
        Sample
        (Decode.field "id" Decode.int)
        (Decode.field "name" Decode.string)
        (Decode.field "attributi" attributoMapDecoder)
        (Decode.field "files" (Decode.list fileDecoder))


encodeSample : Sample (Maybe Int) (AttributoMap AttributoValue) Int -> Encode.Value
encodeSample s =
    Encode.object <|
        [ ( "name", Encode.string s.name )
        , ( "attributi", encodeAttributoMap s.attributi )
        , ( "fileIds", Encode.list Encode.int s.files )
        ]
            ++ unwrap [] (\id -> [ ( "id", Encode.int id ) ]) s.id


httpGetSamples : (SamplesResponse -> msg) -> Cmd msg
httpGetSamples f =
    Http.get
        { url = "/api/samples"
        , expect =
            Http.expectJson f <|
                Decode.map2 (\samples attributi -> ( samples, attributi ))
                    (Decode.field "samples" <| Decode.list sampleDecoder)
                    (Decode.field "attributi" <| Decode.list (attributoDecoder attributoTypeDecoder))
        }


httpCreateSample : Sample (Maybe Int) (AttributoMap AttributoValue) Int -> Cmd Msg
httpCreateSample a =
    Http.post
        { url = "/api/samples"
        , expect = Http.expectJson EditSampleFinished (Decode.maybe (Decode.field "error" userErrorDecoder))
        , body = jsonBody (encodeSample a)
        }


httpEditSample : Sample (Maybe Int) (AttributoMap AttributoValue) Int -> Cmd Msg
httpEditSample a =
    httpPatch
        { url = "/api/samples"
        , expect = Http.expectJson EditSampleFinished (Decode.maybe (Decode.field "error" userErrorDecoder))
        , body = jsonBody (encodeSample a)
        }


httpDeleteSample : Int -> Cmd Msg
httpDeleteSample sampleId =
    httpDelete
        { url = "/api/samples"
        , body = jsonBody (Encode.object [ ( "id", Encode.int sampleId ) ])
        , expect = Http.expectJson SampleDeleteFinished (Decode.maybe (Decode.field "error" userErrorDecoder))
        }


init : () -> ( Model, Cmd Msg )
init _ =
    ( { samples = Loading
      , deleteModalOpen = Nothing
      , sampleDeleteRequest = NotAsked
      , modifyRequest = NotAsked
      , fileUploadRequest = NotAsked
      , editSample = Nothing
      , myTimeZone = Nothing
      , submitErrors = []
      , newFileUpload = { file = Nothing, description = "" }
      }
    , Task.perform TimeZoneReceived here
    )


viewAttributoForm : Attributo ( EditStatus, AttributoEditValue ) -> Html Msg
viewAttributoForm a =
    case second a.type_ of
        EditValueString s ->
            div [ class "mb-3" ] <|
                [ label [ for ("attributo-" ++ fromAttributoName a.name), class "form-label" ] [ text (fromAttributoName a.name) ]
                , input_
                    [ type_ "text"
                    , class "form-control"
                    , id ("attributo-" ++ fromAttributoName a.name)
                    , value s
                    , onInput (EditValueString >> SetValue >> EditSampleAttributo a.name)
                    ]
                ]
                    ++ [ if a.description /= "" then
                            div [ class "form-text" ] [ text a.description ]

                         else
                            text ""
                       ]

        EditValueInt s ->
            div [ class "mb-3" ] <|
                [ label [ for ("attributo-" ++ fromAttributoName a.name), class "form-label" ] [ text (fromAttributoName a.name) ]
                , div [ class "w-50" ]
                    [ input_
                        [ type_ "number"
                        , class "form-control"
                        , id ("attributo-" ++ fromAttributoName a.name)
                        , value s
                        , onInput (EditSampleAttributo a.name << SetValue << EditValueInt)
                        ]
                    ]
                ]
                    ++ [ if a.description /= "" then
                            div [ class "form-text" ] [ text a.description ]

                         else
                            text ""
                       ]

        EditValueList l ->
            div [ class "mb-3" ] <|
                [ label [ for ("attributo-" ++ fromAttributoName a.name), class "form-label" ] [ text (fromAttributoName a.name) ]
                , input_
                    [ type_ "text"
                    , class "form-control"
                    , id ("attributo-" ++ fromAttributoName a.name)
                    , value l.editValue
                    , onInput (\newInput -> EditSampleAttributo a.name <| SetValue <| EditValueList { l | editValue = newInput })
                    ]
                ]
                    ++ [ div [ class "form-text text-muted" ] [ strong [] [ text "Note on editing" ], text ": This is a list, but you can just insert the list elements, comma-separated in the text field." ] ]
                    ++ [ if a.description /= "" then
                            div [ class "form-text" ] [ strong [] [ text "Description: " ], text a.description ]

                         else
                            text ""
                       ]

        EditValueChoice { choiceValues, editValue } ->
            let
                makeOption choiceValue =
                    option
                        [ selected (editValue == choiceValue)
                        ]
                        [ text
                            (if choiceValue == "" then
                                "«no value»"

                             else
                                choiceValue
                            )
                        ]
            in
            div [ class "mb-3" ] <|
                [ label [ for ("attributo-" ++ fromAttributoName a.name), class "form-label" ] [ text (fromAttributoName a.name) ]
                , select
                    [ id ("attributo-" ++ fromAttributoName a.name)
                    , class "form-select"
                    , onInput (\newInput -> EditSampleAttributo a.name <| SetValue <| EditValueChoice { choiceValues = choiceValues, editValue = newInput })
                    ]
                    (List.map makeOption ("" :: choiceValues))
                ]

        EditValueNumber n ->
            let
                inputGroupText =
                    withDefault "" n.suffix
                        ++ (if n.range == emptyNumericRange then
                                ""

                            else
                                " ∈ " ++ numericRangeToString n.range
                           )
            in
            div [ class "mb-3" ] <|
                [ label [ for ("attributo-" ++ fromAttributoName a.name), class "form-label" ] [ text (fromAttributoName a.name) ]
                , div [ class "w-50 input-group" ]
                    [ input_
                        [ type_ "number"
                        , step "0.01"
                        , class "form-control"
                        , id ("attributo-" ++ fromAttributoName a.name)
                        , value n.editValue
                        , onInput (\newValue -> EditSampleAttributo a.name <| SetValue <| EditValueNumber { n | editValue = newValue })
                        ]
                    , if inputGroupText == "" then
                        text ""

                      else
                        span [ class "input-group-text" ] [ text inputGroupText ]
                    ]
                ]
                    ++ [ if a.description /= "" then
                            div [ class "form-text" ] [ text a.description ]

                         else
                            text ""
                       ]

        EditValueDateTime x ->
            div [ class "mb-3" ] <|
                [ label [ for ("attributo-" ++ fromAttributoName a.name), class "form-label" ] [ text (fromAttributoName a.name) ]
                , div [ class "w-50" ]
                    [ input_
                        [ type_ "datetime-local"
                        , class "form-control"
                        , id ("attributo-" ++ fromAttributoName a.name)
                        , value x
                        , onInput (EditSampleAttributo a.name << SetValue << EditValueDateTime)
                        ]
                    ]
                ]
                    ++ [ if a.description /= "" then
                            div [ class "form-text" ] [ text a.description ]

                         else
                            text ""
                       ]

        EditValueBoolean x ->
            div [ class "mb-3" ] <|
                [ div [ class "form-check" ] <|
                    [ input_
                        [ type_ "checkbox"
                        , class "form-check-input"
                        , id ("attributo-" ++ fromAttributoName a.name)
                        , checked x
                        , onInput (always <| EditSampleAttributo a.name <| SetValue <| EditValueBoolean (not x))
                        ]
                    , label [ for ("attributo-" ++ fromAttributoName a.name), class "form-check-label" ] [ text (fromAttributoName a.name) ]
                    ]
                        ++ [ if a.description /= "" then
                                div [ class "form-text" ] [ text a.description ]

                             else
                                text ""
                           ]
                ]

        _ ->
            text "type not supported to edit"


viewFiles : NewFileUpload -> List File -> List (Html Msg)
viewFiles newFile files =
    let
        viewFileRow : File -> Html Msg
        viewFileRow file =
            tr [ class "align-middle" ]
                [ td_ [ text (String.fromInt file.id) ]
                , td_ [ text file.fileName ]
                , td_ [ text file.type_ ]
                , td_ [ text file.description ]
                , td_ [ button [ class "btn btn-danger btn-sm", type_ "button", onClick (EditFileDelete file.id) ] [ icon { name = "trash" } ] ]
                ]

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

        uploadForm =
            [ div [ class "card mb-3" ]
                [ div [ class "card-header" ]
                    [ text "File upload" ]
                , div [ class "card-body" ]
                    [ div
                        [ class "input-group mb-3" ]
                        [ button [ type_ "button", class "btn btn-outline-secondary", onClick EditNewFileOpenSelector ] [ text "Choose file..." ]
                        , input [ type_ "text", disabled True, value (Maybe.unwrap "No file selected" ElmFile.name newFile.file), class "form-control" ] []
                        ]
                    , div [ class "mb-3" ]
                        [ label [ for "file-description", class "form-label" ] [ text "File Description" ]
                        , input_
                            [ type_ "text"
                            , class "form-control"
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
        filesTable ++ uploadForm


viewEditForm : List String -> NewFileUpload -> Sample (Maybe Int) EditableAttributiAndOriginal File -> Html Msg
viewEditForm submitErrorsList newFileUpload sample =
    let
        attributiFormEntries =
            List.map viewAttributoForm sample.attributi.editableAttributi

        submitErrors =
            case submitErrorsList of
                [] ->
                    [ text "" ]

                errors ->
                    [ p_ [ strongText "There were submission errors:" ]
                    , ul [ class "text-danger" ] <| List.map (\e -> li_ [ text e ]) errors
                    ]
    in
    form_ <|
        [ h4_
            [ text
                (if isNothing sample.id then
                    "Add new sample"

                 else
                    "Edit sample"
                )
            ]
        , div [ class "mb-3" ]
            [ label [ for "name", class "form-label" ] [ text "Name" ]
            , input_
                [ type_ "text"
                , class
                    ("form-control"
                        ++ (if sample.name == "" then
                                " is-invalid"

                            else
                                ""
                           )
                    )
                , id "name"
                , value sample.name
                , onInput EditSampleName
                ]
            , if sample.name /= "" then
                text ""

              else
                div [ class "invalid-feedback" ] [ text "Name is mandatory" ]
            ]
        ]
            ++ attributiFormEntries
            ++ viewFiles newFileUpload sample.files
            ++ submitErrors
            ++ [ button
                    [ class "btn btn-primary me-3 mb-3"
                    , onClick EditSampleSubmit
                    , type_ "button"
                    ]
                    [ icon { name = "plus-lg" }
                    , text
                        (if isNothing sample.id then
                            " Add new sample"

                         else
                            " Confirm edit"
                        )
                    ]
               , button
                    [ class "btn btn-secondary me-3 mb-3"
                    , onClick EditSampleCancel
                    , type_ "button"
                    ]
                    [ icon { name = "x-lg" }, text " Cancel" ]
               ]


viewAttributoValue : Zone -> AttributoType -> AttributoValue -> Html Msg
viewAttributoValue zone type_ value =
    case value of
        ValueBoolean bool ->
            if bool then
                icon { name = "check-lg" }

            else
                text ""

        ValueInt int ->
            text (fromInt int)

        ValueString string ->
            case type_ of
                DateTime ->
                    case toTime string of
                        Err _ ->
                            text <| "Error converting time string " ++ string

                        Ok v ->
                            text <| formatPosixHumanFriendly zone v

                _ ->
                    text string

        ValueList attributoValues ->
            case type_ of
                List { subType } ->
                    case subType of
                        Number _ ->
                            span_ <| [ text "(" ] ++ List.map (viewAttributoValue zone subType) attributoValues ++ [ text ")" ]

                        String ->
                            span_ <| intersperse (text ",") <| List.map (viewAttributoValue zone subType) attributoValues

                        _ ->
                            text "unsupported list element type"

                _ ->
                    text "unsupported list type"

        ValueNumber float ->
            text (fromFloat float)


typeToIcon : String -> Html Msg
typeToIcon type_ =
    icon
        { name =
            case split "/" type_ of
                "text" :: "x-shellscript" :: [] ->
                    "file-code"

                "application" :: "pdf" :: [] ->
                    "file-pdf"

                prefix :: _ ->
                    case prefix of
                        "image" ->
                            "file-image"

                        "text" ->
                            "file-text"

                        _ ->
                            "question-diamond"

                _ ->
                    "question-diamond"
        }


viewSampleRow : Zone -> List (Attributo AttributoType) -> Sample SampleId (AttributoMap AttributoValue) File -> Html Msg
viewSampleRow zone attributi sample =
    let
        makeAttributoCell { name, type_ } =
            td []
                [ case retrieveAttributoValue name sample.attributi of
                    Nothing ->
                        text ""

                    Just v ->
                        viewAttributoValue zone type_ v
                ]

        viewFile { id, type_, fileName, description } =
            li [ class "list-group-item" ]
                [ typeToIcon type_
                , text " "
                , span [ attribute "data-tooltip" description ] [ a [ href (makeFilesLink id), class "stretched-link" ] [ text fileName ] ]
                ]

        files =
            case sample.files of
                [] ->
                    text ""

                _ ->
                    ul [ class "list-group list-group-flush" ] (List.map viewFile sample.files)
    in
    tr_ <|
        [ td_ [ text (fromInt sample.id) ]
        , td_ [ text sample.name ]
        ]
            ++ List.map makeAttributoCell attributi
            ++ [ td_ [ files ]
               ]
            ++ [ td_
                    [ button [ class "btn btn-sm btn-danger me-1", onClick (AskDelete sample.name sample.id) ] [ icon { name = "trash" } ]
                    , button [ class "btn btn-sm btn-info", onClick (InitiateEdit sample) ] [ icon { name = "pencil-square" } ]
                    ]
               ]


viewSampleTable : Zone -> List (Sample SampleId (AttributoMap AttributoValue) File) -> List (Attributo AttributoType) -> Html Msg
viewSampleTable zone samples attributi =
    let
        mutedSubheader t =
            span [ class "text-muted fst-italic", style "font-size" "0.8rem" ] [ text t ]

        makeAttributoHeader : Attributo AttributoType -> List (Html msg)
        makeAttributoHeader a =
            case a.type_ of
                Number { suffix } ->
                    case suffix of
                        Just realSuffix ->
                            [ text (fromAttributoName a.name)
                            , br_
                            , mutedSubheader realSuffix
                            ]

                        _ ->
                            [ text (fromAttributoName a.name) ]

                _ ->
                    [ text (fromAttributoName a.name) ]

        attributiColumns : List (Html msg)
        attributiColumns =
            List.map (th_ << makeAttributoHeader) attributi
    in
    table [ class "table" ]
        [ thead_
            [ tr [ class "align-top" ] <|
                [ th_ [ text "ID" ]
                , th_ [ text "Name" ]
                ]
                    ++ attributiColumns
                    ++ [ th_ [ text "Files", br_, mutedSubheader "Hover to see description, click to download" ] ]
                    ++ [ th_ [ text "Actions" ]
                       ]
            ]
        , tbody_
            (List.map (viewSampleRow zone attributi) samples)
        ]


viewInner : Model -> List (Html Msg)
viewInner model =
    case ( model.samples, model.myTimeZone ) of
        ( NotAsked, _ ) ->
            singleton <| text ""

        ( Loading, _ ) ->
            singleton <| text "Loading..."

        ( Failure e, _ ) ->
            singleton <| makeAlert AlertDanger <| [ h4 [ class "alert-heading" ] [ text "Failed to retrieve samples" ] ] ++ showHttpError e

        ( Success { samples, attributi }, Nothing ) ->
            [ text "Waiting for time zone..." ]

        ( Success { samples, attributi }, Just zone ) ->
            let
                prefix =
                    case model.editSample of
                        Nothing ->
                            button [ class "btn btn-primary", onClick AddSample ] [ icon { name = "plus-lg" }, text " Add sample" ]

                        Just ea ->
                            viewEditForm model.submitErrors model.newFileUpload ea

                modifyRequestResult =
                    case model.modifyRequest of
                        NotAsked ->
                            text ""

                        Loading ->
                            p [] [ text "Request in progress..." ]

                        Failure e ->
                            div [] [ makeAlert AlertDanger (showHttpError e) ]

                        Success _ ->
                            div [ class "mt-3" ]
                                [ makeAlert AlertSuccess [ text "Request successful!" ]
                                ]

                deleteRequestResult =
                    case model.sampleDeleteRequest of
                        NotAsked ->
                            text ""

                        Loading ->
                            p [] [ text "Request in progress..." ]

                        Failure e ->
                            div [] [ makeAlert AlertDanger (showHttpError e) ]

                        Success _ ->
                            div [ class "mt-3" ]
                                [ makeAlert AlertSuccess [ text "Deletion successful!" ]
                                ]
            in
            prefix
                :: modifyRequestResult
                :: deleteRequestResult
                :: viewSampleTable zone samples attributi
                :: []


view : Model -> Html Msg
view model =
    let
        maybeDeleteModal =
            case model.deleteModalOpen of
                Nothing ->
                    []

                Just ( sampleName, sampleId ) ->
                    [ Dialog.view
                        (Just
                            { header = Nothing
                            , body = Just (span_ [ text "Really delete sample ", strongText sampleName, text "?" ])
                            , closeMessage = Just CancelDelete
                            , containerClass = Nothing
                            , footer = Just (button [ class "btn btn-danger", onClick (ConfirmDelete sampleId) ] [ text "Really delete!" ])
                            }
                        )
                    ]
    in
    div [ class "container" ] (maybeDeleteModal ++ viewInner model)


attributoValueToEditValue : Zone -> AttributoName -> List (Attributo AttributoType) -> AttributoValue -> Maybe AttributoEditValue
attributoValueToEditValue zone attributoName attributi value =
    let
        attributoFound : Maybe (Attributo AttributoType)
        attributoFound =
            List.find (\x -> x.name == attributoName) attributi

        --| This is used for the comma-separated "list of xy" input field
        attributoValueToString : AttributoValue -> String
        attributoValueToString x =
            case x of
                ValueBoolean bool ->
                    if bool then
                        "true"

                    else
                        "false"

                ValueInt int ->
                    String.fromInt int

                ValueString string ->
                    string

                ValueList attributoValues ->
                    join "," <| List.map attributoValueToString attributoValues

                ValueNumber float ->
                    String.fromFloat float

        convert : Attributo AttributoType -> Maybe AttributoEditValue
        convert a =
            case ( a.type_, value ) of
                ( Attributo.Int, ValueInt x ) ->
                    Just (EditValueInt (String.fromInt x))

                ( Attributo.String, ValueString x ) ->
                    Just (EditValueString x)

                ( Attributo.DateTime, ValueString x ) ->
                    case toTime x of
                        Err _ ->
                            Nothing

                        Ok posix ->
                            Just (EditValueDateTime (formatPosixDateTimeCompatible zone posix))

                ( Attributo.Choice { choiceValues }, ValueString x ) ->
                    Just (EditValueChoice { editValue = x, choiceValues = choiceValues })

                ( Attributo.Number { range, suffix, standardUnit }, ValueNumber x ) ->
                    Just (EditValueNumber { range = range, suffix = suffix, standardUnit = standardUnit, editValue = String.fromFloat x })

                ( Attributo.Number { range, suffix, standardUnit }, ValueInt x ) ->
                    Just (EditValueNumber { range = range, suffix = suffix, standardUnit = standardUnit, editValue = String.fromInt x })

                ( Attributo.List { minLength, maxLength, subType }, ValueList xs ) ->
                    Just
                        (EditValueList
                            { subType = subType
                            , minLength = minLength
                            , maxLength = maxLength
                            , editValue = join "," <| List.map attributoValueToString xs
                            }
                        )

                ( Attributo.Boolean, ValueBoolean x ) ->
                    Just (EditValueBoolean x)

                _ ->
                    Nothing

        -- Debug.log (Debug.toString x) Nothing
    in
    Maybe.andThen convert attributoFound


emptyEditValue : AttributoType -> AttributoEditValue
emptyEditValue a =
    case a of
        Attributo.Int ->
            EditValueInt ""

        Attributo.Boolean ->
            EditValueBoolean False

        Attributo.DateTime ->
            EditValueDateTime ""

        Attributo.SampleId ->
            EditValueSampleId

        Attributo.String ->
            EditValueString ""

        Attributo.List { subType, minLength, maxLength } ->
            EditValueList { subType = subType, minLength = minLength, maxLength = maxLength, editValue = "" }

        Attributo.Number { range, suffix, standardUnit } ->
            EditValueNumber { range = range, suffix = suffix, standardUnit = standardUnit, editValue = "" }

        Attributo.Choice { choiceValues } ->
            EditValueChoice { choiceValues = choiceValues, editValue = "" }


editSampleFromAttributiAndValues : Zone -> List (Attributo AttributoType) -> Sample (Maybe Int) (AttributoMap AttributoValue) a -> Sample (Maybe Int) EditableAttributiAndOriginal a
editSampleFromAttributiAndValues zone attributi s =
    -- two steps:
    -- 1. convert existing values into "best" manual values
    -- 2. add missing attributo and add as empty attributi, too
    let
        mapAttributi : AttributoMap AttributoValue -> EditableAttributiAndOriginal
        mapAttributi m =
            let
                -- Convert attributo metadata, as well as an attributo value, into an "editable attributo"
                convertToEditValues : String -> Attributo ( AttributoType, AttributoValue ) -> Dict String (Attributo ( EditStatus, AttributoEditValue )) -> Dict String (Attributo ( EditStatus, AttributoEditValue ))
                convertToEditValues attributoName a prev =
                    case attributoValueToEditValue zone (toAttributoName attributoName) attributi (second a.type_) of
                        Nothing ->
                            prev

                        Just finishedEditValue ->
                            Dict.insert attributoName (mapAttributo (always ( Unchanged, finishedEditValue )) a) prev

                existingAttributiMap : Dict String EditableAttributo
                existingAttributiMap =
                    Dict.foldr convertToEditValues Dict.empty (createAnnotatedAttributoMap attributi m)

                totalAttributoNames : Set.Set String
                totalAttributoNames =
                    Set.fromList (List.map (.name >> fromAttributoName) attributi)

                missingKeys : Set.Set String
                missingKeys =
                    Set.diff totalAttributoNames <| Set.fromList (Dict.keys existingAttributiMap)

                missingAttributi : List (Attributo AttributoType)
                missingAttributi =
                    List.filter (\x -> Set.member (fromAttributoName x.name) missingKeys) attributi

                missingAttributiMap : Dict String EditableAttributo
                missingAttributiMap =
                    Dict.fromList <| List.map (\a -> ( fromAttributoName a.name, mapAttributo (\type_ -> ( Unchanged, emptyEditValue type_ )) a )) <| missingAttributi
            in
            { originalAttributi = m, editableAttributi = Dict.values <| Dict.union existingAttributiMap missingAttributiMap }
    in
    sampleMapAttributi mapAttributi s


editValueToValue : AttributoEditValue -> Result String AttributoValue
editValueToValue x =
    case x of
        EditValueInt string ->
            Maybe.unwrap (Err "not an integer") (Ok << ValueInt) <| toInt string

        EditValueBoolean boolValue ->
            Ok (ValueBoolean boolValue)

        EditValueDateTime string ->
            Ok (ValueString string)

        EditValueSampleId ->
            Ok (ValueInt 0)

        EditValueString string ->
            Ok (ValueString string)

        EditValueList { minLength, maxLength, subType, editValue } ->
            let
                parts =
                    List.map trim <| split "," editValue

                noParts =
                    List.length parts
            in
            if Maybe.orElse (Maybe.map (\ml -> noParts < ml) minLength) (Maybe.map (\ml -> noParts > ml) maxLength) == Just True then
                Err "invalid range"

            else
                case subType of
                    String ->
                        Ok (ValueList <| List.map ValueString parts)

                    Number _ ->
                        Maybe.unwrap (Err "invalid number in list") Ok <| Maybe.map ValueList <| traverse (String.toFloat >> Maybe.map ValueNumber) parts

                    _ ->
                        Err "invalid list subtype"

        EditValueNumber { range, editValue } ->
            case String.toFloat editValue of
                Nothing ->
                    Err "invalid decimal number"

                Just inputNumeric ->
                    if valueInRange range inputNumeric then
                        Ok (ValueNumber inputNumeric)

                    else
                        Err "value not in range"

        EditValueChoice { choiceValues, editValue } ->
            let
                choiceValid =
                    editValue == "" || List.member editValue choiceValues
            in
            if choiceValid then
                Ok (ValueString editValue)

            else
                Err "invalid choice"


convertEditValues : EditableAttributiAndOriginal -> Result (List ( AttributoName, String )) (AttributoMap AttributoValue)
convertEditValues { originalAttributi, editableAttributi } =
    let
        -- first, filter for manually edited values (the other ones we don't care about here)
        manuallyEdited : List ( AttributoName, AttributoEditValue )
        manuallyEdited =
            List.foldr
                (\attributo prev ->
                    if first attributo.type_ == Edited then
                        ( attributo.name, second attributo.type_ ) :: prev

                    else
                        prev
                )
                []
                editableAttributi

        -- Convert the edited value to the real value (with optional error)
        convertSingle : ( AttributoName, AttributoEditValue ) -> Result ( AttributoName, String ) ( AttributoName, AttributoValue )
        convertSingle ( name, v ) =
            case editValueToValue v of
                Err e ->
                    -- add attributo name to error for better display later
                    Err ( name, e )

                Ok value ->
                    Ok ( name, value )

        -- Convert _all_ edited values, optionally failing
        converted : Result (List ( AttributoName, String )) (List ( AttributoName, AttributoValue ))
        converted =
            collectResults (List.map convertSingle manuallyEdited)

        -- Combine result of editing with original map
        combineWithOriginal : List ( AttributoName, AttributoValue ) -> AttributoMap AttributoValue
        combineWithOriginal =
            List.foldr (\( name, value ) -> updateAttributoMap name value) originalAttributi
    in
    Result.map combineWithOriginal converted


emptySample : Sample (Maybe Int) (AttributoMap a) b
emptySample =
    { id = Nothing, name = "", attributi = emptyAttributoMap, files = [] }


emptyNewFileUpload =
    { description = "", file = Nothing }


update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        SamplesReceived response ->
            ( { model | samples = fromResult <| Result.map (\( samples, attributi ) -> { samples = samples, attributi = attributi }) <| response }, Cmd.none )

        -- The user pressed "yes, really delete!" in the modal
        ConfirmDelete sampleId ->
            ( { model | sampleDeleteRequest = Loading, deleteModalOpen = Nothing }, httpDeleteSample sampleId )

        -- The user closed the "Really delete?" modal
        CancelDelete ->
            ( { model | deleteModalOpen = Nothing }, Cmd.none )

        -- The deletion request for an object finished
        SampleDeleteFinished result ->
            case result of
                Err e ->
                    ( { model | sampleDeleteRequest = Failure e }, Cmd.none )

                Ok (Just userError) ->
                    ( { model | sampleDeleteRequest = Failure (Http.BadBody userError.title) }, Cmd.none )

                Ok Nothing ->
                    ( { model | sampleDeleteRequest = Success () }, httpGetSamples SamplesReceived )

        -- The user pressed the "Add new object" button
        AddSample ->
            case ( model.samples, model.myTimeZone ) of
                ( Success { attributi }, Just zone ) ->
                    ( { model | editSample = Just (editSampleFromAttributiAndValues zone attributi emptySample) }, Cmd.none )

                _ ->
                    ( model, Cmd.none )

        -- The name of the object changed
        EditSampleName newName ->
            case model.editSample of
                Nothing ->
                    ( model, Cmd.none )

                Just editSample ->
                    ( { model | editSample = Just { editSample | name = newName } }, Cmd.none )

        -- The user pressed the submit change button (either creating or editing an object)
        EditSampleSubmit ->
            case model.editSample of
                Nothing ->
                    ( model, Cmd.none )

                Just editSample ->
                    let
                        -- either edit or create, depending on the ID
                        operation =
                            Maybe.unwrap httpCreateSample (always httpEditSample) editSample.id
                    in
                    if model.newFileUpload.description /= "" || isJust model.newFileUpload.file then
                        ( { model | submitErrors = [ "There is still a file in the upload form. Submit or clear the form!" ] }, Cmd.none )

                    else
                        case convertEditValues editSample.attributi of
                            Err errorList ->
                                ( { model | submitErrors = List.map (\( name, errorMessage ) -> fromAttributoName name ++ ": " ++ errorMessage) errorList }, Cmd.none )

                            Ok editedAttributi ->
                                let
                                    sampleToSend =
                                        { id = editSample.id
                                        , name = editSample.name
                                        , attributi = editedAttributi
                                        , files = List.map .id editSample.files
                                        }
                                in
                                ( { model | modifyRequest = Loading }, operation sampleToSend )

        EditSampleCancel ->
            ( { model | editSample = Nothing, newFileUpload = emptyNewFileUpload }, Cmd.none )

        EditSampleFinished result ->
            case result of
                Err e ->
                    ( { model | modifyRequest = Failure e }, Cmd.none )

                Ok (Just userError) ->
                    ( { model | modifyRequest = Failure (Http.BadBody userError.title) }, Cmd.none )

                Ok Nothing ->
                    ( { model | modifyRequest = Success (), editSample = Nothing }, httpGetSamples SamplesReceived )

        EditSampleAttributo attributoName attributoValue ->
            case ( model.editSample, attributoValue ) of
                -- This is the unlikely case that we have an "attributo was edited" message, but sample is edited
                ( Nothing, _ ) ->
                    ( model, Cmd.none )

                -- An attributo was changed, but the change should be ignored
                ( _, IgnoreValue ) ->
                    ( model, Cmd.none )

                -- An attributo was changed, and the change is "relevant"
                -- Note that "editSample" is a sample with the pair "original attributi, editable attributi" as attributi
                -- attribute (damn this is confusing!)
                ( Just editSample, SetValue newValue ) ->
                    let
                        -- Update the value if it exists (if it doesn't, this is weird!), and set the state to "edited"
                        -- so when the object is stored, it's added to the manually edited attributes list
                        updateValue : Attributo ( EditStatus, AttributoEditValue ) -> Attributo ( EditStatus, AttributoEditValue )
                        updateValue x =
                            if x.name == attributoName then
                                mapAttributo (always ( Edited, newValue )) x

                            else
                                x

                        newEditable =
                            List.map updateValue editSample.attributi.editableAttributi

                        newSample =
                            { editSample | attributi = { originalAttributi = editSample.attributi.originalAttributi, editableAttributi = newEditable } }
                    in
                    ( { model | editSample = Just newSample }, Cmd.none )

        AskDelete sampleName sampleId ->
            ( { model | deleteModalOpen = Just ( sampleName, sampleId ) }, Cmd.none )

        InitiateEdit sample ->
            case ( model.samples, model.myTimeZone ) of
                ( Success { attributi }, Just zone ) ->
                    ( { model | editSample = Just (editSampleFromAttributiAndValues zone attributi (sampleMapId Just sample)) }, Cmd.none )

                _ ->
                    ( model, Cmd.none )

        TimeZoneReceived zone ->
            ( { model | myTimeZone = Just zone }, httpGetSamples SamplesReceived )

        EditNewFileDescription newDescription ->
            let
                oldFileUpload =
                    model.newFileUpload
            in
            ( { model | newFileUpload = { oldFileUpload | description = newDescription } }, Cmd.none )

        EditNewFileFile newFile ->
            let
                oldFileUpload =
                    model.newFileUpload
            in
            ( { model | newFileUpload = { oldFileUpload | file = Just newFile } }, Cmd.none )

        EditFileUpload ->
            case model.newFileUpload.file of
                Nothing ->
                    ( model, Cmd.none )

                Just fileToUpload ->
                    ( { model | fileUploadRequest = Loading }, httpCreateFile EditFileUploadFinished model.newFileUpload.description fileToUpload )

        EditFileUploadFinished result ->
            case result of
                Err e ->
                    ( { model | fileUploadRequest = Failure e }, Cmd.none )

                Ok file ->
                    let
                        newEditSample =
                            case model.editSample of
                                Nothing ->
                                    Nothing

                                Just editSample ->
                                    Just { editSample | files = file :: editSample.files }
                    in
                    ( { model | fileUploadRequest = Success (), newFileUpload = emptyNewFileUpload, editSample = newEditSample }, Cmd.none )

        EditFileDelete toDeleteId ->
            case model.editSample of
                Nothing ->
                    ( model, Cmd.none )

                Just editSample ->
                    let
                        newEditSample =
                            Just { editSample | files = List.filter (\x -> x.id /= toDeleteId) editSample.files }
                    in
                    ( { model | editSample = newEditSample }, Cmd.none )

        EditResetNewFileUpload ->
            ( { model | newFileUpload = emptyNewFileUpload }, Cmd.none )

        EditNewFileOpenSelector ->
            ( model, File.Select.file [] EditNewFileFile )
