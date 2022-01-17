module Amarcord.UserError exposing (..)

import Json.Decode as Decode


type alias UserError =
    { title : String, code : Maybe Int, description : Maybe String }


userErrorDecoder : Decode.Decoder UserError
userErrorDecoder =
    Decode.map3
        (\title code description -> { title = title, code = code, description = description })
        (Decode.field "title" Decode.string)
        (Decode.maybe (Decode.field "code" Decode.int))
        (Decode.maybe (Decode.field "title" Decode.string))
