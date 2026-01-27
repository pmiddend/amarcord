module Amarcord.GeometryMetadata exposing (GeometryId(..), GeometryMetadata, fromJson, geometryIdToInt, geometryIdToString)

import Api.Data exposing (JsonGeometryMetadata)
import Api.Time exposing (Posix)
import Time exposing (millisToPosix)


type GeometryId
    = GeometryId Int


fromJson : JsonGeometryMetadata -> GeometryMetadata
fromJson { id, name, createdLocal } =
    { id = GeometryId id
    , name = name
    , createdLocal = millisToPosix createdLocal
    }


geometryIdToString : GeometryId -> String
geometryIdToString (GeometryId gid) =
    String.fromInt gid


geometryIdToInt : GeometryId -> Int
geometryIdToInt (GeometryId gid) =
    gid


type alias GeometryMetadata =
    { id : GeometryId
    , name : String
    , createdLocal : Posix
    }
