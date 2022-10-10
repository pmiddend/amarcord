let
  pkgs = import (import ./nix/sources.nix { }).nixpkgs { };
  frontend = pkgs.callPackage (import ./frontend/default.nix) { };
  poetry2nix-latest-src = pkgs.fetchFromGitHub {
    owner = "nix-community";
    repo = "poetry2nix";
    rev = "1.34.1";
    hash = "sha256-aEhdKq5PTG20cN+4OHhRHnp5tE8mbwpKyO48bbDAt40=";
  };
  poetry2nix = (import poetry2nix-latest-src { inherit pkgs; poetry = pkgs.poetry; });
  poetryOverrides = poetry2nix.overrides.withDefaults (self: super: {
      # ModuleNotFoundError: No module named 'flit_core'
      # see https://github.com/nix-community/poetry2nix/issues/218
      pyparsing = super.pyparsing.overrideAttrs (old: { buildInputs = (old.buildInputs or [ ]) ++ [ self.flit-core ]; });

      quart = super.quart.overrideAttrs (old: { buildInputs = (old.buildInputs or [ ]) ++ [ self.poetry ]; });
      
      cfel-pylint-checkers = super.cfel-pylint-checkers.overrideAttrs (old: { buildInputs = (old.buildInputs or [ ]) ++ [ self.poetry ]; });

      # ModuleNotFoundError: No module named 'poetry'
      msgpack-types = super.msgpack-types.overrideAttrs (old: { buildInputs = (old.buildInputs or [ ]) ++ [ self.poetry ]; });

      # Cannot find hatchling otherwise
      platformdirs = super.platformdirs.overrideAttrs (old: {
        buildInputs = (old.buildInputs or [ ]) ++ [ self.hatchling self.hatch-vcs ];
      });

      perflint = super.perflint.overrideAttrs (old: {
        buildInputs = (old.buildInputs or [ ]) ++ [ self.flit-core ];
      });
    });
  pythonPackage = poetry2nix.mkPoetryApplication {
    projectDir = ./.;
    postInstall = ''
      wrapProgram $out/bin/amarcord-webserver --set AMARCORD_STATIC_FOLDER ${frontend}/
    '';
    overrides = poetryOverrides;
  };
  pythonEnv = poetry2nix.mkPoetryEnv {
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

    copyToRoot = pkgs.buildEnv {
      name = "image-root";
      paths = [ pythonPackage frontend ];
      pathsToLink = [ "/bin" ];
    };

    config = {
      Env = [ "AMARCORD_STATIC_FOLDER=${frontend}" ];
    };
  };
  skopeoShell = pkgs.mkShell {
    packages = [ pkgs.skopeo ];
  };
}
