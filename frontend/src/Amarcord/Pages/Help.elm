module Amarcord.Pages.Help exposing (..)

import Amarcord.Html exposing (h1_, h2_, h3_, img_, p_, strongText)
import Html exposing (div, text)
import Html.Attributes exposing (class, src)


view : Html.Html msg
view =
    div [ class "container" ]
        [ h1_ [ text "Welcome to AMARCORD!" ]
        , h2_ [ text "Overview" ]
        , p_ [ text "There are a few concepts you need to learn about to start working with AMARCORD. Check out the following diagram:" ]
        , img_ [ src "overview.svg" ]
        , p_
            [ text "As you can see, the concept of "
            , strongText "attributi"
            , text " is key to understanding how to customize AMARCORD. Let's explain this using a concrete example."
            ]
        , h3_ [ text "Toy Project: Screening Lyzozyme at EuXFEL" ]
        , p_ [ text "" ]
        ]
