module Amarcord.UserError exposing (..)

import Json.Decode as Decode


type alias CustomError =
    { title : String, code : Maybe Int, description : Maybe String }


customErrorDecoder : Decode.Decoder CustomError
customErrorDecoder =
    Decode.map3
        (\title code description -> { title = title, code = code, description = description })
        (Decode.field "title" Decode.string)
        (Decode.maybe (Decode.field "code" Decode.int))
        (Decode.maybe (Decode.field "description" Decode.string))
