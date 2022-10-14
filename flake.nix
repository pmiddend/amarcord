{
  description = "Flake for AMARCORD - a web server, frontend tools for storing metadata for serial crystallography";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.poetry2nix = {
    url = "github:nix-community/poetry2nix";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, poetry2nix }:
    let
      system = "x86_64-linux";
    in
    rec {
      # Nixpkgs overlay providing the application
      overlay = nixpkgs.lib.composeManyExtensions [
        poetry2nix.overlay
        (final: prev:
          let
            poetryOverrides = prev.poetry2nix.overrides.withDefaults (self: super: {
              # ModuleNotFoundError: No module named 'flit_core'
              # see https://github.com/nix-community/poetry2nix/issues/218
              pyparsing = super.pyparsing.overrideAttrs (old: { buildInputs = (old.buildInputs or [ ]) ++ [ self.flit-core ]; });

              quart = super.quart.overrideAttrs (old: { buildInputs = (old.buildInputs or [ ]) ++ [ self.poetry ]; });

              openpyxl-stubs = super.openpyxl-stubs.overrideAttrs (old: { buildInputs = (old.buildInputs or [ ]) ++ [ self.setuptools ]; });

              typed-argument-parser = super.typed-argument-parser.overrideAttrs (old: { buildInputs = (old.buildInputs or [ ]) ++ [ self.setuptools ]; });

              types-python-dateutil = super.types-python-dateutil.overrideAttrs (old: { buildInputs = (old.buildInputs or [ ]) ++ [ self.setuptools ]; });

              quart-cors = super.quart-cors.overrideAttrs (old: { buildInputs = (old.buildInputs or [ ]) ++ [ self.setuptools ]; });

              sqlalchemy2-stubs = super.sqlalchemy2-stubs.overrideAttrs (old: { buildInputs = (old.buildInputs or [ ]) ++ [ self.setuptools ]; });

              randomname = super.randomname.overrideAttrs (old: { buildInputs = (old.buildInputs or [ ]) ++ [ self.setuptools ]; });

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
          in
          rec {
            # The application
            amarcord-python-package =
              prev.poetry2nix.mkPoetryApplication {
                projectDir = ./.;
                postPatch = ''
                  sed -e 's#^hardcoded_static_folder.*#hardcoded_static_folder = "${amarcord-frontend}"#' -i   amarcord/cli/webserver.py
                '';
                overrides = poetryOverrides;
              };
            amarcord-python-env = prev.poetry2nix.mkPoetryEnv {
              projectDir = ./.;
              overrides = poetryOverrides;
              editablePackageSources = {
                amarcord = ./amarcord;
              };
            };

            amarcord-frontend = prev.pkgs.callPackage ./frontend/default.nix { };
          })
      ];

      packages.${system} =
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ overlay ];
          };
        in
        {
          amarcord-python-package = pkgs.amarcord-python-package;
          amarcord-docker-image = pkgs.dockerTools.buildImage {
            name = "amarcord";
            tag = "latest";

            copyToRoot = pkgs.buildEnv {
              name = "image-root";
              paths = [ pkgs.amarcord-python-package ];
              pathsToLink = [ "/bin" ];
            };
          };
        };

      devShells.${system} =
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ overlay ];
          };
        in {
        default = pkgs.amarcord-python-env.env.overrideAttrs (oldAttrs: {
          buildInputs = [ pkgs.poetry pkgs.skopeo pkgs.shellcheck ];
        });

        frontend = pkgs.mkShell {
          buildInputs = [ pkgs.elmPackages.elm pkgs.elmPackages.elm-review pkgs.nodejs pkgs.elm2nix ];
        };
      };
    };

}
