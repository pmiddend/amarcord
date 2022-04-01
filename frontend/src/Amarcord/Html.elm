module Amarcord.Html exposing (..)

import Html exposing (div, em, form, h1, h2, h3, h4, h5, hr, img, input, li, nav, p, span, strong, sup, tbody, td, text, th, thead, tr, ul)


br_ : Html.Html msg
br_ =
    Html.br [] []


th_ x =
    th [] x


ul_ x =
    ul [] x


nav_ x =
    nav [] x


sup_ x =
    sup [] x


img_ x =
    img x []


hr_ =
    hr [] []


strongText x =
    strong [] [ text x ]


span_ x =
    span [] x


p_ x =
    p [] x


li_ x =
    li [] x


tr_ x =
    tr [] x


td_ x =
    td [] x


thead_ x =
    thead [] x


tbody_ x =
    tbody [] x


div_ x =
    div [] x


form_ x =
    form [] x


h1_ x =
    h1 [] x


h2_ x =
    h2 [] x


h3_ x =
    h3 [] x


h4_ x =
    h4 [] x


h5_ x =
    h5 [] x


input_ x =
    input x []


em_ =
    em []
