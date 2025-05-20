module Amarcord.GeometryMetadata exposing (GeometryId(..), GeometryMetadata, fromJson, geometryIdToInt, geometryIdToString)

import Api.Data exposing (JsonGeometryMetadata)


type GeometryId
    = GeometryId Int


fromJson : JsonGeometryMetadata -> GeometryMetadata
fromJson { id, name } =
    { id = GeometryId id, name = name }


geometryIdToString : GeometryId -> String
geometryIdToString (GeometryId gid) =
    String.fromInt gid


geometryIdToInt : GeometryId -> Int
geometryIdToInt (GeometryId gid) =
    gid


type alias GeometryMetadata =
    { id : GeometryId
    , name : String
    }
