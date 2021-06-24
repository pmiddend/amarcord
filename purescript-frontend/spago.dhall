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
  , "js-timers"
  , "numbers"
  , "psci-support"
  , "remotedata"
  , "routing"
  , "routing-duplex"
  , "uri"
  ]
, packages = ./packages.dhall
, sources = [ "src/**/*.purs", "test/**/*.purs" ]
}
