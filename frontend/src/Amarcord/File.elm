module Amarcord.File exposing (..)


type alias File =
    { id : Int
    , type_ : String
    , fileName : String
    , description : String
    , sizeInBytes : Int
    , originalPath : Maybe String
    }
