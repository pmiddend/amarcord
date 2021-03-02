{ name = "halogen-project"
, dependencies =
  [ "aff-promise"
  , "console"
  , "effect"
  , "halogen"
  , "halogen-css"
  , "psci-support"
  , "purescript-graphql-client"
  , "remotedata"
  , "routing"
  , "routing-duplex"
  ]
, packages = ./packages.dhall
, sources = [ "src/**/*.purs", "test/**/*.purs" ]
}
