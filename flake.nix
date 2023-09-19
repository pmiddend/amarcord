{
  description = "Flake for AMARCORD - a web server, frontend tools for storing metadata for serial crystallography";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs?rev=5df4d78d54f7a34e9ea1f84a22b4fd9baebc68d0";
  inputs.poetry2nix = {
    url = "github:nix-community/poetry2nix?rev=1d7eda9336f336392d24e9602be5cb9be7ae405c";
    inputs.nixpkgs.follows = "nixpkgs";
  };
  inputs.uglymol.url = "git+https://gitlab.desy.de/cfel-sc-public/uglymol.git";
  inputs.mkElmDerivation = {
    url = "github:jeslie0/mkElmDerivation";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, poetry2nix, uglymol, mkElmDerivation }:
    let
      system = "x86_64-linux";
      pypkgs-build-requirements = {
        structlog-overtime = [ "setuptools" ];
        openpyxl-stubs = [ "setuptools" ];
        fawltydeps = [ "poetry" ];
        randomname = [ "setuptools" ];
        quart-cors = [ "setuptools" ];
        quart = [ "poetry" ];
      };
      p2n-overrides = final: prev: prev.poetry2nix.defaultPoetryOverrides.extend (self: super:
        builtins.mapAttrs
          (package: build-requirements:
            (builtins.getAttr package super).overridePythonAttrs (old: {
              buildInputs = (old.buildInputs or [ ]) ++ (builtins.map (pkg: if builtins.isString pkg then builtins.getAttr pkg super else pkg) build-requirements);
            })
          )
          pypkgs-build-requirements
      );
    in
    rec {
      # Nixpkgs overlay providing the application
      overlay = nixpkgs.lib.composeManyExtensions [
        poetry2nix.overlay
        (final: prev:
          let
            poetryOverrides = p2n-overrides final prev;
          in
          rec {
            # The application
            amarcord-python-package = frontend:
              prev.poetry2nix.mkPoetryApplication {
                projectDir = ./.;
                postPatch = ''
                  sed -e 's#^hardcoded_static_folder.*#hardcoded_static_folder = "${frontend}"#' -i   amarcord/cli/webserver.py
                '';
                overrides = poetryOverrides;
              };
            amarcord-production-webserver = frontend: prev.writeShellScriptBin "amarcord-production-webserver" ''
              ${(amarcord-python-package frontend).dependencyEnv}/bin/hypercorn amarcord.cli.webserver:app "$@"
            '';
            amarcord-python-env = prev.poetry2nix.mkPoetryEnv {
              projectDir = ./.;
              overrides = poetryOverrides;
              editablePackageSources = {
                amarcord = ./amarcord;
              };
            };
          })
      ];

      packages.${system} =
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [
              overlay
              mkElmDerivation.overlays.mkElmDerivation
            ];
          };

          amarcord-frontend =
            let
              corePackage = pkgs.mkElmDerivation {
                pname = "amarcord-frontend-core";
                version = "1.0.0";
                src = frontend/.;
                outputJavaScript = true;
                targets = [ "src/Main.elm" ];
              };
              mtzJs = pkgs.fetchurl {
                url = "https://raw.githubusercontent.com/uglymol/uglymol.github.io/master/wasm/mtz.js";
                hash = "sha256-Ut9ZJnGu+hbJBWD+XW14mosJ1Lr3tcRx+pHOP+q+awo=";
              };
              mtzWasm = pkgs.fetchurl {
                url = "https://raw.githubusercontent.com/uglymol/uglymol.github.io/master/wasm/mtz.wasm";
                hash = "sha256-B71/bdEMs/yLMbHzmDiWeQNeoR80pUgNJmnzR/7Pabk=";
              };

            in
            pkgs.stdenv.mkDerivation {
              src = frontend/.;
              pname = "amarcord-frontend";
              version = "1.0.0";
              phases = "unpackPhase installPhase";

              installPhase = ''
                mkdir -p $out
                cp ${corePackage}/Main.min.js $out/main.js
                cp ${mtzJs} $out/mtz.js
                cp ${mtzWasm} $out/mtz.wasm
                cp uglymol-custom-element.js $out/
                echo ${uglymol}
                cp -R src/index.html ${uglymol.packages.${system}.default}/uglymol.min.js ./*.js ./*.svg ./*.css ./*.png ./*.jpg fonts/ $out/
              '';
            };

        in
        {
          amarcord-python-package = pkgs.amarcord-python-package amarcord-frontend;
          amarcord-production-webserver = pkgs.amarcord-production-webserver amarcord-frontend;
          inherit amarcord-frontend;
          amarcord-docker-image = pkgs.dockerTools.streamLayeredImage {
            name = "amarcord";
            tag = "latest";

            contents = [ (pkgs.amarcord-production-webserver amarcord-frontend) ];

          };
        };

      devShells.${system} =
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ overlay ];
          };

          # External as in "not provided by poetry"
          externalDependencies = [
            pkgs.poetry
            pkgs.skopeo
            pkgs.shellcheck
            pkgs.nodePackages.pyright
          ];

        in
        {
          default = pkgs.amarcord-python-env.env.overrideAttrs (oldAttrs: {
            buildInputs = externalDependencies;
            PYTHONPATH = "./";
          });

          frontend = pkgs.mkShell {
            buildInputs = [
              pkgs.elmPackages.elm
              pkgs.elmPackages.elm-review
              pkgs.elmPackages.elm-format
              pkgs.elmPackages.elm-json
              pkgs.nodejs
              pkgs.elm2nix
            ];
          };
        };
    };

}
