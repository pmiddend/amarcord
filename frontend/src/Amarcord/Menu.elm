module Amarcord.Menu exposing (viewMenu)

import Amarcord.API.Requests exposing (BeamtimeId)
import Amarcord.Bootstrap exposing (icon)
import Amarcord.Route as Route exposing (MergeFilter(..), Route)
import Html exposing (Html, a, div, h3, li, text, ul)
import Html.Attributes exposing (attribute, class, href, target)
import Html.Attributes.Extra exposing (role)
import List exposing (any)


type alias MenuLeaf =
    { route : Route
    , description : String
    , iconName : String
    }


type MenuNode
    = Dropdown String String (List MenuLeaf)
    | Leaf MenuLeaf
    | VerticalRule


viewMenuNode : Route -> MenuNode -> Html msg
viewMenuNode modelRoute x =
    case x of
        Leaf leaf ->
            viewMenuLeaf modelRoute leaf

        VerticalRule ->
            div [ class "vr" ] []

        Dropdown description iconName leaves ->
            li [ class "nav-item dropdown" ]
                [ a
                    [ class
                        ("nav-link dropdown-toggle"
                            ++ (if any (\leaf -> leaf.route == modelRoute) leaves then
                                    " active"

                                else
                                    ""
                               )
                        )
                    , attribute "data-bs-toggle" "dropdown"
                    , role "button"
                    , target "_self"
                    ]
                    [ icon { name = iconName }, text <| " " ++ description ]
                , div [ class "dropdown-menu" ] (List.map (viewMenuLeaf modelRoute) leaves)
                ]


menu : BeamtimeId -> List MenuNode
menu bt =
    [ Dropdown "Runs"
        "card-list"
        [ { route = Route.RunOverview bt, description = "Current Run", iconName = "caret-right" }
        , { route = Route.Runs bt [], description = "All Runs", iconName = "folder2" }
        ]
    , Dropdown "Library"
        "collection"
        [ { route = Route.Chemicals bt, description = "Chemicals", iconName = "gem" }
        , { route = Route.DataSets bt, description = "Data Sets", iconName = "folder2" }
        , { route = Route.Geometries bt, description = "Geometries", iconName = "border-all" }
        ]
    , Dropdown "Analysis"
        "bar-chart-steps"
        [ { route = Route.AnalysisOverview bt [] False Both, description = "By Experiment Type", iconName = "clipboard-check" }
        , { route = Route.RunAnalysis bt, description = "By Run", iconName = "zoom-in" }
        , { route = Route.Geometry bt, description = "Geometry", iconName = "border" }
        ]
    , Dropdown "Admin"
        "gear-fill"
        [ { route = Route.ExperimentTypes bt, description = "Experiment Types", iconName = "clipboard-check" }
        , { route = Route.Attributi bt Nothing, description = "Attributi", iconName = "card-list" }
        , { route = Route.AdvancedControls bt, description = "Advanced", iconName = "speedometer" }
        , { route = Route.Schedule bt, description = "Schedule", iconName = "calendar-week" }
        , { route = Route.EventLog bt, description = "Events", iconName = "book" }
        , { route = Route.Import bt Route.ImportAttributi, description = "Import", iconName = "upload" }
        ]
    , VerticalRule
    , Leaf { route = Route.Root bt, description = "All Beamtimes", iconName = "globe" }
    ]


viewMenu : Route -> Html msg
viewMenu modelRoute =
    case Route.beamtimeIdInRoute modelRoute of
        Nothing ->
            h3 [] [ text "Select a beam time" ]

        Just btid ->
            ul [ class "nav nav-pills" ] (List.map (viewMenuNode modelRoute) (menu btid))


viewMenuLeaf : Route -> MenuLeaf -> Html msg
viewMenuLeaf modelRoute leaf =
    li [ class "nav-item" ]
        [ a
            [ href (Route.makeLink leaf.route)
            , class
                ((if modelRoute == leaf.route then
                    "active "

                  else
                    ""
                 )
                    ++ "nav-link text-nowrap"
                )
            ]
            [ icon { name = leaf.iconName }, text <| " " ++ leaf.description ]
        ]
