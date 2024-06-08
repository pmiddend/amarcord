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
import NoSinglePatternCase
import NoUnsafeDivision
import NoUnused.CustomTypeConstructors
import NoUnused.Dependencies
import NoUnused.Exports
import NoUnused.Parameters
import NoUnused.Patterns
import NoUnused.Variables
import Review.Rule as Rule exposing (Rule)
import Simplify


config : List Rule
config =
    List.map
        (Rule.ignoreErrorsForDirectories
            [ "third-party", "generated" ]
        )
        [ NoMissingTypeAnnotation.rule
        , NoDeprecated.rule NoDeprecated.defaults
        , NoPrematureLetComputation.rule
        , NoSinglePatternCase.rule NoSinglePatternCase.fixInArgument
        , NoUnused.Dependencies.rule
        , NoUnsafeDivision.rule
        , NoUnused.Variables.rule
        , NoUnused.CustomTypeConstructors.rule []
        , NoUnused.Exports.rule
        , NoUnused.Parameters.rule
        , NoUnused.Patterns.rule
        , NoEtaReducibleLambdas.rule
            { lambdaReduceStrategy = NoEtaReducibleLambdas.AlwaysRemoveLambdaWhenPossible
            , argumentNamePredicate = always True
            }
        , Simplify.rule Simplify.defaults
        ]
