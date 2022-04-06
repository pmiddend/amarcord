module Amarcord.LocalStorage exposing (..)

import Json.Decode as Decode
import Json.Encode as Encode
import Result exposing (toMaybe)


type alias LocalStorageColumn =
    { attributoName : String
    , isOn : Bool
    }


type alias LocalStorage =
    { columns : List LocalStorageColumn
    }


decodeLocalStorage : String -> Maybe LocalStorage
decodeLocalStorage =
    toMaybe
        << Decode.decodeString
            (Decode.map
                LocalStorage
                (Decode.field "columns"
                    (Decode.list (Decode.map2 LocalStorageColumn (Decode.field "name" Decode.string) (Decode.field "is-on" Decode.bool)))
                )
            )


encodeLocalStorage : LocalStorage -> String
encodeLocalStorage { columns } =
    Encode.encode 0 <|
        Encode.object
            [ ( "columns"
              , Encode.list
                    (\v ->
                        Encode.object
                            [ ( "name"
                              , Encode.string v.attributoName
                              )
                            , ( "is-on", Encode.bool v.isOn )
                            ]
                    )
                    columns
              )
            ]
