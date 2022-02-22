module Amarcord.API.Requests exposing (ExperimentType, httpCreateExperimentType, httpDeleteExperimentType, httpGetExperimentTypes)

import Amarcord.UserError exposing (UserError, userErrorDecoder)
import Amarcord.Util exposing (httpDelete)
import Http exposing (jsonBody)
import Json.Decode as Decode
import Json.Encode as Encode


userErrorToHttpError : { a | title : String } -> Http.Error
userErrorToHttpError =
    Http.BadBody << .title


userErrorToError : UserError -> Result Http.Error a
userErrorToError =
    Err << userErrorToHttpError


maybeErrorToResult : Result Http.Error (Maybe UserError) -> Result Http.Error ()
maybeErrorToResult x =
    case x of
        Ok (Just e) ->
            userErrorToError e

        Ok Nothing ->
            Ok ()

        Err error ->
            Err error


type alias ExperimentType =
    { name : String
    , attributeNames : List String
    }


errorDecoder : Decode.Decoder UserError
errorDecoder =
    Decode.field "error" userErrorDecoder


maybeErrorDecoder : Decode.Decoder (Maybe UserError)
maybeErrorDecoder =
    Decode.maybe errorDecoder


experimentTypeDecoder : Decode.Decoder ExperimentType
experimentTypeDecoder =
    Decode.map2 ExperimentType (Decode.field "name" Decode.string) (Decode.field "attributo-names" <| Decode.list Decode.string)


httpCreateExperimentType : (Result Http.Error () -> msg) -> String -> List String -> Cmd msg
httpCreateExperimentType f name attributiNames =
    Http.post
        { url = "/api/experiment-types"
        , expect =
            Http.expectJson (f << maybeErrorToResult) maybeErrorDecoder
        , body =
            jsonBody
                (Encode.object
                    [ ( "name", Encode.string name )
                    , ( "attributi-names", Encode.list Encode.string attributiNames )
                    ]
                )
        }


httpDeleteExperimentType : (Result Http.Error () -> msg) -> String -> Cmd msg
httpDeleteExperimentType f experimentTypeName =
    httpDelete
        { url = "/api/experiment-types"
        , body = jsonBody (Encode.object [ ( "name", Encode.string experimentTypeName ) ])
        , expect = Http.expectJson (f << maybeErrorToResult) maybeErrorDecoder
        }


valueOrError : Decode.Decoder value -> Decode.Decoder (Result UserError value)
valueOrError valueDecoder =
    Decode.oneOf [ Decode.map Err errorDecoder, Decode.map Ok valueDecoder ]


userErrorOrHttpError : Result Http.Error (Result UserError value) -> Result Http.Error value
userErrorOrHttpError x =
    case x of
        Err y ->
            Err y

        Ok (Err y) ->
            Err (userErrorToHttpError y)

        Ok (Ok y) ->
            Ok y


httpGetExperimentTypes : (Result Http.Error (List ExperimentType) -> msg) -> Cmd msg
httpGetExperimentTypes f =
    Http.get
        { url = "/api/experiment-types"
        , expect = Http.expectJson (f << userErrorOrHttpError) (valueOrError <| Decode.field "experiment-types" <| Decode.list experimentTypeDecoder)
        }
