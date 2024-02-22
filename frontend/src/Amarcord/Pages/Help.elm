module Amarcord.Pages.Help exposing (..)

import Amarcord.Html exposing (h1_, h2_, p_)
import Html exposing (div, text)
import Html.Attributes exposing (class)


view : Html.Html msg
view =
    div [ class "container" ]
        [ h1_ [ text "Welcome to AMARCORD!" ]
        , h2_ [ text "Overview" ]
        , p_ [ text "There are a few concepts you need to learn about to start working with AMARCORD." ]
        ]
