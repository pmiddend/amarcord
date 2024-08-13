module Amarcord.Route exposing (..)

import Amarcord.API.Requests exposing (BeamtimeId, ExperimentTypeId, beamtimeIdToString)
import Url
import Url.Parser exposing ((</>), Parser, int, map, oneOf, parse, s, top)


type Route
    = BeamtimeSelection
    | Root BeamtimeId
    | Chemicals BeamtimeId
    | DataSets BeamtimeId
    | Schedule BeamtimeId
    | ExperimentTypes BeamtimeId
    | RunOverview BeamtimeId
    | Attributi BeamtimeId
    | AdvancedControls BeamtimeId
    | Analysis (Maybe ExperimentTypeId) BeamtimeId
    | RunAnalysis BeamtimeId
    | EventLog BeamtimeId


beamtimeIdInRoute : Route -> Maybe BeamtimeId
beamtimeIdInRoute x =
    case x of
        BeamtimeSelection ->
            Nothing

        Root btid ->
            Just btid

        Chemicals btid ->
            Just btid

        DataSets btid ->
            Just btid

        Schedule btid ->
            Just btid

        ExperimentTypes btid ->
            Just btid

        RunOverview btid ->
            Just btid

        Attributi btid ->
            Just btid

        AdvancedControls btid ->
            Just btid

        Analysis _ btid ->
            Just btid

        RunAnalysis btid ->
            Just btid

        EventLog btid ->
            Just btid



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
            BeamtimeSelection


parseUrl : Url.Url -> Route
parseUrl url =
    case parse matchRoute url of
        Just route ->
            route

        Nothing ->
            BeamtimeSelection


routePrefix : String
routePrefix =
    "index.html#"


makeLink : Route -> String
makeLink x =
    case x of
        BeamtimeSelection ->
            routePrefix ++ "/"

        Root beamtimeId ->
            routePrefix ++ "/" ++ beamtimeIdToString beamtimeId

        Attributi beamtimeId ->
            routePrefix ++ "/attributi/" ++ beamtimeIdToString beamtimeId

        RunOverview beamtimeId ->
            routePrefix ++ "/runoverview/" ++ beamtimeIdToString beamtimeId

        AdvancedControls beamtimeId ->
            routePrefix ++ "/advancedcontrols/" ++ beamtimeIdToString beamtimeId

        Chemicals beamtimeId ->
            routePrefix ++ "/chemicals/" ++ beamtimeIdToString beamtimeId

        Analysis experimentTypeIdMaybe beamtimeId ->
            let
                prefix =
                    routePrefix ++ "/analysis/" ++ beamtimeIdToString beamtimeId
            in
            case experimentTypeIdMaybe of
                Nothing ->
                    prefix

                Just experimentTypeId ->
                    prefix ++ "/" ++ String.fromInt experimentTypeId

        RunAnalysis beamtimeId ->
            routePrefix ++ "/runanalysis/" ++ beamtimeIdToString beamtimeId

        DataSets beamtimeId ->
            routePrefix ++ "/datasets/" ++ beamtimeIdToString beamtimeId

        ExperimentTypes beamtimeId ->
            routePrefix ++ "/experimenttypes/" ++ beamtimeIdToString beamtimeId

        Schedule beamtimeId ->
            routePrefix ++ "/schedule/" ++ beamtimeIdToString beamtimeId

        EventLog beamtimeId ->
            routePrefix ++ "/event-log/" ++ beamtimeIdToString beamtimeId


makeFilesLink : Int -> String
makeFilesLink id =
    "api/files/" ++ String.fromInt id


makeIndexingIdLogLink : Int -> String
makeIndexingIdLogLink id =
    "api/indexing/" ++ String.fromInt id ++ "/log"


makeIndexingIdErrorLogLink : Int -> String
makeIndexingIdErrorLogLink id =
    "api/indexing/" ++ String.fromInt id ++ "/errorlog"


matchRoute : Parser (Route -> a) a
matchRoute =
    oneOf
        [ map BeamtimeSelection top

        -- , map Root int
        , map Attributi (s "attributi" </> int)
        , map Chemicals (s "chemicals" </> int)
        , map RunOverview (s "runoverview" </> int)
        , map Schedule (s "schedule" </> int)
        , map EventLog (s "event-log" </> int)
        , map AdvancedControls (s "advancedcontrols" </> int)
        , map (\btId etId -> Analysis (Just etId) btId) (s "analysis" </> int </> int)
        , map (Analysis Nothing) (s "analysis" </> int)
        , map RunAnalysis (s "runanalysis" </> int)
        , map DataSets (s "datasets" </> int)
        , map ExperimentTypes (s "experimenttypes" </> int)
        ]
