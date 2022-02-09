module Amarcord.Route exposing (..)

import Url
import Url.Parser exposing (Parser, map, oneOf, parse, s, top)


type Route
    = Root
    | Samples
    | RunOverview
    | Attributi
    | Analysis


parseUrl : Url.Url -> Route
parseUrl url =
    case parse matchRoute url of
        Just route ->
            route

        Nothing ->
            Root


makeLink : Route -> String
makeLink x =
    case x of
        Root ->
            "/"

        Attributi ->
            "/attributi"

        RunOverview ->
            "/runoverview"

        Samples ->
            "/samples"

        Analysis ->
            "/analysis"


makeFilesLink : Int -> String
makeFilesLink id =
    "/api/files/" ++ String.fromInt id


matchRoute : Parser (Route -> a) a
matchRoute =
    oneOf
        [ map Root top
        , map Attributi (s "attributi")
        , map Samples (s "samples")
        , map RunOverview (s "runoverview")
        , map Analysis (s "analysis")
        ]
