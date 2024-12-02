module Amarcord.Route exposing (..)

import Amarcord.API.DataSet exposing (DataSetId)
import Amarcord.API.Requests exposing (BeamtimeId, ExperimentTypeId, MergeResultId, beamtimeIdToString)
import Amarcord.Attributo exposing (AttributoId, AttributoValue(..))
import Dict
import Time exposing (millisToPosix, posixToMillis)
import Url
import Url.Parser exposing ((</>), (<?>), Parser, int, map, oneOf, parse, s, top)
import Url.Parser.Query as Query


type alias AnalysisFilter =
    { id : AttributoId
    , value : AttributoValue
    }


type MergeFilter
    = Merged
    | Unmerged
    | Both


type Route
    = BeamtimeSelection
    | Root BeamtimeId
    | Chemicals BeamtimeId
    | DataSets BeamtimeId
    | Schedule BeamtimeId
    | ExperimentTypes BeamtimeId
    | Runs BeamtimeId
    | RunOverview BeamtimeId
    | Attributi BeamtimeId
    | AdvancedControls BeamtimeId
    | AnalysisOverview BeamtimeId (List AnalysisFilter) Bool MergeFilter
    | AnalysisDataSet BeamtimeId Int
    | MergeResult BeamtimeId ExperimentTypeId DataSetId MergeResultId
    | RunAnalysis BeamtimeId
    | EventLog BeamtimeId


beamtimeIdInRoute : Route -> Maybe BeamtimeId
beamtimeIdInRoute x =
    case x of
        BeamtimeSelection ->
            Nothing

        Root btid ->
            Just btid

        MergeResult btid _ _ _ ->
            Just btid

        Chemicals btid ->
            Just btid

        DataSets btid ->
            Just btid

        Schedule btid ->
            Just btid

        ExperimentTypes btid ->
            Just btid

        Runs btid ->
            Just btid

        RunOverview btid ->
            Just btid

        Attributi btid ->
            Just btid

        AdvancedControls btid ->
            Just btid

        AnalysisOverview btId _ _ _ ->
            Just btId

        AnalysisDataSet btid _ ->
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


addQuery : List String -> String
addQuery xs =
    case xs of
        [] ->
            ""

        xss ->
            "?" ++ String.join "&" xss


mergeFilterToString : MergeFilter -> String
mergeFilterToString x =
    case x of
        Both ->
            "both"

        Merged ->
            "merged"

        Unmerged ->
            "unmerged"


makeLink : Route -> String
makeLink x =
    case x of
        BeamtimeSelection ->
            routePrefix ++ "/"

        Root beamtimeId ->
            routePrefix ++ "/" ++ beamtimeIdToString beamtimeId

        Attributi beamtimeId ->
            routePrefix ++ "/attributi/" ++ beamtimeIdToString beamtimeId

        Runs beamtimeId ->
            routePrefix ++ "/runs/" ++ beamtimeIdToString beamtimeId

        RunOverview beamtimeId ->
            routePrefix ++ "/runoverview/" ++ beamtimeIdToString beamtimeId

        AdvancedControls beamtimeId ->
            routePrefix ++ "/advancedcontrols/" ++ beamtimeIdToString beamtimeId

        Chemicals beamtimeId ->
            routePrefix ++ "/chemicals/" ++ beamtimeIdToString beamtimeId

        AnalysisOverview beamtimeId filters acrossBeamtimes mergeFilter ->
            routePrefix
                ++ "/analysis/"
                ++ beamtimeIdToString beamtimeId
                ++ addQuery
                    (("across="
                        ++ (if acrossBeamtimes then
                                "1"

                            else
                                "0"
                           )
                     )
                        :: ("merge=" ++ mergeFilterToString mergeFilter)
                        :: List.map (\filterString -> "filter=" ++ filterString) (filtersSerializer filters)
                    )

        AnalysisDataSet beamtimeId dsId ->
            routePrefix ++ "/data-set/" ++ beamtimeIdToString beamtimeId ++ "/" ++ String.fromInt dsId

        RunAnalysis beamtimeId ->
            routePrefix ++ "/runanalysis/" ++ beamtimeIdToString beamtimeId

        DataSets beamtimeId ->
            routePrefix ++ "/datasets/" ++ beamtimeIdToString beamtimeId

        MergeResult beamtimeId etId dsId mergeResultId ->
            routePrefix
                ++ "/mergeresult/"
                ++ beamtimeIdToString beamtimeId
                ++ "/"
                ++ String.fromInt etId
                ++ "/"
                ++ String.fromInt dsId
                ++ "/"
                ++ String.fromInt mergeResultId

        ExperimentTypes beamtimeId ->
            routePrefix ++ "/experimenttypes/" ++ beamtimeIdToString beamtimeId

        Schedule beamtimeId ->
            routePrefix ++ "/schedule/" ++ beamtimeIdToString beamtimeId

        EventLog beamtimeId ->
            routePrefix ++ "/event-log/" ++ beamtimeIdToString beamtimeId


makeFilesLink : Int -> Maybe String -> String
makeFilesLink id suggestedNameMaybe =
    case suggestedNameMaybe of
        Nothing ->
            "api/files/" ++ String.fromInt id

        Just suggestedName ->
            "api/files/" ++ String.fromInt id ++ "?suggested_name=" ++ suggestedName


makeIndexingIdLogLink : Int -> String
makeIndexingIdLogLink id =
    "api/indexing/" ++ String.fromInt id ++ "/log"


makeIndexingIdErrorLogLink : Int -> String
makeIndexingIdErrorLogLink id =
    "api/indexing/" ++ String.fromInt id ++ "/errorlog"


filtersParser : List String -> List AnalysisFilter
filtersParser strings =
    let
        valueParser : String -> String -> Maybe AttributoValue
        valueParser type_ valueStr =
            case type_ of
                "i" ->
                    Maybe.map ValueInt (String.toInt valueStr)

                "c" ->
                    Maybe.map ValueChemical (String.toInt valueStr)

                "dt" ->
                    Maybe.map (ValueDateTime << millisToPosix) (String.toInt valueStr)

                "s" ->
                    Just (ValueString valueStr)

                -- lists not supported for now (too lazy)
                "l" ->
                    Nothing

                "n" ->
                    Maybe.map ValueNumber (String.toFloat valueStr)

                "b" ->
                    Just (ValueBoolean (valueStr == "true"))

                "x" ->
                    Just ValueNone

                _ ->
                    Nothing

        filterParser : String -> Maybe AnalysisFilter
        filterParser s =
            case String.split "," s of
                [ idStr, valueType, valueStr ] ->
                    String.toInt idStr
                        |> Maybe.andThen (\id -> valueParser valueType valueStr |> Maybe.map (\value -> { id = id, value = value }))

                _ ->
                    Nothing
    in
    List.filterMap filterParser strings


filtersSerializer : List AnalysisFilter -> List String
filtersSerializer filters =
    let
        filterSerializer : AnalysisFilter -> String
        filterSerializer { id, value } =
            case value of
                ValueInt n ->
                    String.join "," [ String.fromInt id, "i", String.fromInt n ]

                ValueChemical n ->
                    String.join "," [ String.fromInt id, "c", String.fromInt n ]

                ValueDateTime posix ->
                    String.join "," [ String.fromInt id, "dt", String.fromInt (posixToMillis posix) ]

                ValueString s ->
                    String.join "," [ String.fromInt id, "s", s ]

                ValueList _ ->
                    String.join "," [ String.fromInt id, "l", "" ]

                ValueNumber n ->
                    String.join "," [ String.fromInt id, "n", String.fromFloat n ]

                ValueBoolean b ->
                    String.join ","
                        [ String.fromInt id
                        , "n"
                        , if b then
                            "true"

                          else
                            "false"
                        ]

                ValueNone ->
                    String.join ","
                        [ String.fromInt id
                        , "x"
                        , ""
                        ]
    in
    List.map filterSerializer filters


matchRoute : Parser (Route -> a) a
matchRoute =
    oneOf
        [ map BeamtimeSelection top
        , map Attributi (s "attributi" </> int)
        , map Chemicals (s "chemicals" </> int)
        , map RunOverview (s "runoverview" </> int)
        , map Runs (s "runs" </> int)
        , map Schedule (s "schedule" </> int)
        , map EventLog (s "event-log" </> int)
        , map AdvancedControls (s "advancedcontrols" </> int)
        , map
            AnalysisOverview
            ((s "analysis" </> int <?> Query.custom "filter" filtersParser)
                <?> Query.map (\x -> x == Just 1) (Query.int "across")
                <?> Query.map (Maybe.withDefault Both)
                        (Query.enum "merge"
                            (Dict.fromList
                                [ ( "both", Both )
                                , ( "merged", Merged )
                                , ( "unmerged", Unmerged )
                                ]
                            )
                        )
            )
        , map AnalysisDataSet (s "data-set" </> int </> int)
        , map MergeResult (s "mergeresult" </> int </> int </> int </> int)
        , map RunAnalysis (s "runanalysis" </> int)
        , map DataSets (s "datasets" </> int)
        , map ExperimentTypes (s "experimenttypes" </> int)
        ]
