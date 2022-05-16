let
  pkgs = import (import ./nix/sources.nix { }).nixpkgs { };
  frontend = pkgs.callPackage (import ./frontend/default.nix) { };
  poetryOverrides = pkgs.poetry2nix.overrides.withDefaults (self: super: {
      # ModuleNotFoundError: No module named 'flit_core'
      # see https://github.com/nix-community/poetry2nix/issues/218
      pyparsing = super.pyparsing.overrideAttrs (old: { buildInputs = (old.buildInputs or [ ]) ++ [ self.flit-core ]; });

      quart = super.quart.overrideAttrs (old: { buildInputs = (old.buildInputs or [ ]) ++ [ self.poetry ]; });

      # ModuleNotFoundError: No module named 'poetry'
      msgpack-types = super.msgpack-types.overrideAttrs (old: { buildInputs = (old.buildInputs or [ ]) ++ [ self.poetry ]; });

      # Cannot find hatchling otherwise
      platformdirs = super.platformdirs.overrideAttrs (old: {
        buildInputs = (old.buildInputs or [ ]) ++ [ self.hatchling self.hatch-vcs ];
      });

    });
  pythonPackage = pkgs.poetry2nix.mkPoetryApplication {
    projectDir = ./.;
    postInstall = ''
      wrapProgram $out/bin/amarcord-webserver --set AMARCORD_STATIC_FOLDER ${frontend}/
    '';
    overrides = poetryOverrides;
  };
  pythonEnv = pkgs.poetry2nix.mkPoetryEnv {
    projectDir = ./.;
    overrides = poetryOverrides;
  };
in
{
  pythonPackages = pkgs.python3Packages;
  inherit pythonPackage;
  inherit frontend;
  inherit pkgs;
  inherit pythonEnv;
  dockerImage = pkgs.dockerTools.buildImage {
    name = "amarcord";
    tag = "latest";

    contents = [ pythonPackage frontend ];

    config = {
      Env = [ "AMARCORD_STATIC_FOLDER=${frontend}" ];
    };
  };
  skopeoShell = pkgs.mkShell {
    packages = [ pkgs.skopeo ];
  };
}
