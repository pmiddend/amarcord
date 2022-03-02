module Amarcord.Menu exposing (viewMenu)

import Amarcord.Bootstrap exposing (icon)
import Amarcord.Route as Route exposing (Route)
import Html exposing (Html, a, div, li, text, ul)
import Html.Attributes exposing (attribute, class, href, target)
import List exposing (any)


type alias MenuLeaf =
    { route : Route
    , description : String
    , iconName : String
    }


type MenuNode
    = Dropdown String String (List MenuLeaf)
    | Leaf MenuLeaf


type alias Menu =
    List MenuNode


viewMenuNode : Route -> MenuNode -> Html msg
viewMenuNode modelRoute x =
    case x of
        Leaf leaf ->
            viewMenuLeaf modelRoute leaf

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
                    , target "_self"
                    ]
                    [ icon { name = iconName }, text <| " " ++ description ]
                , div [ class "dropdown-menu" ] (List.map (viewMenuLeaf modelRoute) leaves)
                ]


menu =
    [ Leaf { route = Route.RunOverview, description = "Overview", iconName = "card-list" }
    , Dropdown "Setup"
        "cart"
        [ { route = Route.Samples, description = "Samples", iconName = "gem" }
        , { route = Route.DataSets, description = "Data Sets", iconName = "folder2" }
        ]
    , Leaf { route = Route.Analysis, description = "Analysis", iconName = "bar-chart-steps" }
    , Dropdown "Admin"
        "gear-fill"
        [ { route = Route.ExperimentTypes, description = "Experiment Types", iconName = "clipboard-check" }
        , { route = Route.Attributi, description = "Attributi", iconName = "card-list" }
        ]
    ]


viewMenu : Route -> Html msg
viewMenu modelRoute =
    ul [ class "nav nav-pills" ]
        (List.map (viewMenuNode modelRoute) menu)


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
                    ++ "nav-link"
                )
            ]
            [ icon { name = leaf.iconName }, text <| " " ++ leaf.description ]
        ]
