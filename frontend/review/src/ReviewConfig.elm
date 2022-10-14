module ReviewConfig exposing (config)

{-| Do not rename the ReviewConfig module or the config function, because
`elm-review` will look for these.

To add packages that contain rules, add them to this review project using

    `elm install author/packagename`

when inside the directory containing this file.

-}

import NoDeprecated
import NoEtaReducibleLambdas
import NoMissingTypeAnnotation
import NoPrematureLetComputation
import NoRedundantConcat
import NoRedundantCons
import NoSinglePatternCase
import NoUnused.Dependencies
import Review.Rule exposing (Rule)
import Simplify


config : List Rule
config =
    [ NoMissingTypeAnnotation.rule
    , NoDeprecated.rule NoDeprecated.defaults
    , NoPrematureLetComputation.rule
    --, NoRedundantConcat.rule
    --, NoRedundantCons.rule
    , NoSinglePatternCase.rule NoSinglePatternCase.fixInArgument
    , NoUnused.Dependencies.rule
    , NoEtaReducibleLambdas.rule {
          lambdaReduceStrategy = NoEtaReducibleLambdas.AlwaysRemoveLambdaWhenPossible
        , argumentNamePredicate = always True
      }
    , Simplify.rule Simplify.defaults
    ]
