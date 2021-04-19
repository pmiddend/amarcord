{ name = "halogen-project"
, dependencies =
  [ "aff-promise"
  , "affjax"
  , "argonaut"
  , "console"
  , "debug"
  , "effect"
  , "formatters"
  , "halogen"
  , "halogen-css"
  , "numbers"
  , "psci-support"
  , "remotedata"
  , "routing"
  , "routing-duplex"
  ]
, packages = ./packages.dhall
, sources = [ "src/**/*.purs", "test/**/*.purs" ]
}
