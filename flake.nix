{
  description = "Flake for AMARCORD - a web server, frontend tools for storing metadata for serial crystallography";

  inputs.nixpkgs.url = "nixpkgs/nixos-24.11";
  inputs.poetry2nix = {
    url = "github:nix-community/poetry2nix";
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
        fawltydeps = [ "poetry" ];
        pyprojroot = [ "setuptools" ];
        autoimport = [ "pdm-pep517" "pdm-backend" ];
        types-openpyxl = [ "setuptools" ];
        randomname = [ "setuptools" ];
        alabaster = [ "flit-core" ];
        sphinx-autobuild = [ "flit-core" ];
        pyyaml = [ "setuptools" ];
        sphinxcontrib-mermaid = [ "setuptools" ];
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
        poetry2nix.overlays.default
        (final: prev:
          let
            poetryOverrides = p2n-overrides final prev;
            # Fix taken from https://github.com/fpletz/authentik-nix/blob/24907f67ee4850179e46c19ce89334568d2b05c6/components/pythonEnv.nix
            # issue is
            # https://github.com/NixOS/nixpkgs/pull/361930
            python = prev.python312.override {
              self = python;
              packageOverrides = finalRec: prevRec: {
                wheel = prevRec.wheel.overridePythonAttrs (oA: rec {
                  version = "0.45.0";
                  src = oA.src.override (oA: {
                    rev = "refs/tags/${version}";
                    hash = "sha256-SkviTE0tRB++JJoJpl+CWhi1kEss0u8iwyShFArV+vw=";
                  });
                });
              };
            };
          in
          rec {
            # The application
            amarcord-python-package = frontend:
              prev.poetry2nix.mkPoetryApplication {
                projectDir = ./.;
                inherit python;
                postPatch = ''
                  sed -e 's#^hardcoded_static_folder.*#hardcoded_static_folder = "${frontend}"#' -i   amarcord/cli/webserver.py
                '';
                overrides = poetryOverrides;
              };
            # super annoying: .dependencyEnv gives us an env with the "python" and "uvicorn" executables, which then search upwards and sidewards
            # to find dependencies. uvicorn, however, still uses the current working directory as a search path. However, the current working directory
            # is the wrong directory, since the hardcoded_static_folder isn't replaced yet. This is breaking isolation, so we override the behavior with
            # the --app dir explicitly
            build-amarcord-production-webserver = frontend: prev.writeShellScriptBin "amarcord-production-webserver" ''
              ${(amarcord-python-package frontend).dependencyEnv}/bin/gunicorn amarcord.cli.webserver:app  --worker-class uvicorn.workers.UvicornWorker "$@"
            '';
            amarcord-python-env = prev.poetry2nix.mkPoetryEnv {
              inherit python;
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
          amarcord-production-webserver = pkgs.build-amarcord-production-webserver amarcord-frontend;
          inherit amarcord-frontend;
          amarcord-docker-image-no-stream = pkgs.dockerTools.buildImage {
            name = "amarcord";
            tag = "latest";

            copyToRoot = pkgs.buildEnv {
              name = "amarcord-docker-root";
              paths = [ (pkgs.build-amarcord-production-webserver amarcord-frontend) (pkgs.amarcord-python-package amarcord-frontend) ];
              pathsToLink = [ "/bin" ];
            };

          };
          amarcord-docker-image = pkgs.dockerTools.streamLayeredImage {
            name = "amarcord";
            tag = "latest";

            contents = [
              (pkgs.build-amarcord-production-webserver amarcord-frontend)
              (pkgs.amarcord-python-package amarcord-frontend)
            ];
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
            pkgs.basedpyright
            # for docs
            pkgs.glibcLocales
            pkgs.mermaid-cli
            pkgs.gnumake
            # For generating Elm code
            pkgs.openapi-generator-cli
            # To generate the DB diagrams
            pkgs.schemacrawler
          ];

        in
        {
          default = pkgs.amarcord-python-env.env.overrideAttrs
            (oldAttrs: {
              buildInputs = externalDependencies;
              PYTHONPATH = "./";
              # sphinxcontrib-spelling wants enchant and uses dlopen, so for now: hack
              LD_LIBRARY_PATH = "${pkgs.enchant}/lib";
              # This is also more or less a hack, see
              # https://discourse.nixos.org/t/aspell-dictionaries-are-not-available-to-enchant/39254
              ASPELL_CONF = "dict-dir ${(pkgs.aspellWithDicts (ps: with ps; [ en ]))}/lib/aspell";
            });

          frontend =
            let
              elm-language-server = (import ./frontend/elm-language-server { inherit pkgs; })."@elm-tooling/elm-language-server";
            in
            pkgs.mkShell {
              buildInputs = [
                pkgs.elmPackages.elm
                pkgs.elmPackages.elm-review
                pkgs.elmPackages.elm-format
                pkgs.elmPackages.elm-json
                pkgs.elmPackages.elm-test
                elm-language-server
                pkgs.nodejs
                pkgs.elm2nix
              ];
            };
        };
    };

}
