module Amarcord.Route exposing (..)

import Url
import Url.Parser exposing (Parser, map, oneOf, parse, s, top)


type Route
    = Root
    | Chemicals
    | DataSets
    | Schedule
    | ExperimentTypes
    | RunOverview
    | Attributi
    | AdvancedControls
    | Analysis
    | RunAnalysis



-- This is the main entrypoint for parsing the current URL. It looks a bit weird - why parse the
-- fragment "recursively"? If we parse the URL directly, we have URLs like this:
-- http://localhost/run_overview
-- Which needs extra work on the web server side. It basically needs to reroute every request it doesn't know
-- to the index.html page somehow? Or we need specific rewrite rules.
--
-- Instead, we "nest" the real URL in the fragment part, as in: index.html#/foo/bar?baz=qux.
-- So here, we first check if we have a fragment, and if so, parse it like it's the real path + query string.
-- This needs minimal work on the web server - it just has to redirect "/" to "/index.html"


parseUrlFragment : Url.Url -> Route
parseUrlFragment url =
    case Maybe.andThen (\fragment -> Url.fromString ("http://localhost" ++ fragment)) url.fragment of
        Just subUrl ->
            parseUrl subUrl

        Nothing ->
            Root


parseUrl : Url.Url -> Route
parseUrl url =
    case parse matchRoute url of
        Just route ->
            route

        Nothing ->
            Root


routePrefix : String
routePrefix =
    "index.html#"


makeLink : Route -> String
makeLink x =
    case x of
        Root ->
            routePrefix ++ "/"

        Attributi ->
            routePrefix ++ "/attributi"

        RunOverview ->
            routePrefix ++ "/runoverview"

        AdvancedControls ->
            routePrefix ++ "/advancedcontrols"

        Chemicals ->
            routePrefix ++ "/chemicals"

        Analysis ->
            routePrefix ++ "/analysis"

        RunAnalysis ->
            routePrefix ++ "/runanalysis"

        DataSets ->
            routePrefix ++ "/datasets"

        ExperimentTypes ->
            routePrefix ++ "/experimenttypes"

        Schedule ->
            routePrefix ++ "/schedule"


makeFilesLink : Int -> String
makeFilesLink id =
    "api/files/" ++ String.fromInt id


matchRoute : Parser (Route -> a) a
matchRoute =
    oneOf
        [ map Root top
        , map Attributi (s "attributi")
        , map Chemicals (s "chemicals")
        , map RunOverview (s "runoverview")
        , map Schedule (s "schedule")
        , map AdvancedControls (s "advancedcontrols")
        , map Analysis (s "analysis")
        , map RunAnalysis (s "runanalysis")
        , map DataSets (s "datasets")
        , map ExperimentTypes (s "experimenttypes")
        ]
