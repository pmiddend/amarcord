module Amarcord.File exposing (..)

import File as ElmFile
import Http exposing (filePart, multipartBody, stringPart)
import Json.Decode as Decode
import Json.Encode as Encode


type alias File =
    { id : Int
    , type_ : String
    , fileName : String
    , description : String
    }


fileDecoder : Decode.Decoder File
fileDecoder =
    Decode.map4
        File
        (Decode.field "id" Decode.int)
        (Decode.field "type_" Decode.string)
        (Decode.field "fileName" Decode.string)
        (Decode.field "description" Decode.string)


httpCreateFile : (Result Http.Error File -> msg) -> String -> ElmFile.File -> Cmd msg
httpCreateFile f description file =
    Http.post
        { url = "/api/files"
        , expect = Http.expectJson f fileDecoder
        , body =
            multipartBody
                [ filePart "file" file
                , stringPart "metadata" (Encode.encode 0 <| Encode.object [ ( "description", Encode.string description ) ])
                ]
        }
