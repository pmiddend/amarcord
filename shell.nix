let default = (import ./default.nix);
in default.pythonEnv.env.overrideAttrs (oldAttrs: {
  buildInputs = [ default.pkgs.poetry ];
})
