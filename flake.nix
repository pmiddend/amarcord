{
  description = "Flake for AMARCORD - a web server, frontend tools for storing metadata for serial crystallography";

  inputs.nixpkgs.url = "nixpkgs/nixos-24.11";
  inputs.uglymol.url = "git+https://gitlab.desy.de/cfel-sc-public/uglymol.git";
  inputs.mkElmDerivation = {
    url = "github:jeslie0/mkElmDerivation";
    inputs.nixpkgs.follows = "nixpkgs";
  };
  inputs.pyproject-nix = {
    url = "github:pyproject-nix/pyproject.nix";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  inputs.uv2nix = {
    url = "github:pyproject-nix/uv2nix";
    inputs.pyproject-nix.follows = "pyproject-nix";
    inputs.nixpkgs.follows = "nixpkgs";
  };

  inputs.pyproject-build-systems = {
    url = "github:pyproject-nix/build-system-pkgs";
    inputs.pyproject-nix.follows = "pyproject-nix";
    inputs.uv2nix.follows = "uv2nix";
    inputs.nixpkgs.follows = "nixpkgs";
  };


  outputs = { self, nixpkgs, uv2nix, pyproject-nix, pyproject-build-systems, uglymol, mkElmDerivation }:
    let
      system = "x86_64-linux";
      inherit (nixpkgs) lib;

      # Load a uv workspace from a workspace root.
      # Uv2nix treats all uv projects as workspace projects.
      workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./.; };

      # Create package overlay from workspace.
      overlay = workspace.mkPyprojectOverlay {
        # Prefer prebuilt binary wheels as a package source.
        # Sdists are less likely to "just work" because of the metadata missing from uv.lock.
        # Binary wheels are more likely to, but may still require overrides for library dependencies.
        sourcePreference = "wheel"; # or sourcePreference = "sdist";
        # Optionally customise PEP 508 environment
        # environ = {
        #   platform_release = "5.10.65";
        # };
      };

      # Extend generated overlay with build fixups
      #
      # Uv2nix can only work with what it has, and uv.lock is missing essential metadata to perform some builds.
      # This is an additional overlay implementing build fixups.
      # See:
      # - https://pyproject-nix.github.io/uv2nix/FAQ.html
      pyprojectOverrides = final: prev: {
        python-magic = prev.python-magic.overrideAttrs (old:
          let
            libPath = "${lib.getLib pkgs.file}/lib/libmagic${pkgs.stdenv.hostPlatform.extensions.sharedLibrary}";
            fixupScriptText = ''
              substituteInPlace magic/loader.py \
                --replace-warn "find_library('magic')" "'${libPath}'"
            '';
            isWheel = old.src.isWheel or false;
          in
          {
            buildInputs = [ prev.setuptools ];
            postPatch = lib.optionalString (!isWheel) fixupScriptText;
            postFixup = lib.optionalString isWheel ''
              cd $out/${final.python.sitePackages}
              ${fixupScriptText}
            '';
            pythonImportsCheck = old.pythonImportsCheck or [ ] ++ [ "magic" ];
          }
        );

        # pyenchant = prev.pyenchant.overrideAttrs (old: {
        #   buildInputs = [ prev.setuptools ];
        # });
      };


      # let
      #   inherit (final) resolveBuildSystem;
      #   inherit (builtins) mapAttrs;
      #   buildSystemOverrides = { python-magic.setuptools = [ ]; };
      # in
      # mapAttrs (name: spec: prev.${name}.overrideAttrs (old: { nativeBuildInputs = old.nativeBuildInputs ++ resolveBuildSystem spec; })) buildSystemOverrides

      # This example is only using x86_64-linux
      pkgs = import nixpkgs {
        inherit system;
        overlays = [
          # overlay
          mkElmDerivation.overlays.mkElmDerivation
        ];
      };

      # Use Python 3.12 from nixpkgs
      python = pkgs.python312;

      # Construct package set
      pythonSet =
        # Use base package set from pyproject.nix builders
        (pkgs.callPackage pyproject-nix.build.packages {
          inherit python;
        }).overrideScope
          (
            lib.composeManyExtensions [
              pyproject-build-systems.overlays.default
              overlay
              pyprojectOverrides
            ]
          );
    in
    {
      packages.${system} =
        let
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
        rec {
          inherit amarcord-frontend;

          amarcord-python-package = pythonSet.mkVirtualEnv "amarcord-serial-env" workspace.deps.default;

          amarcord-production-webserver = pkgs.writeShellScriptBin "amarcord-production-webserver" ''
            export AMARCORD_STATIC_PATH="${amarcord-frontend}"
            ${amarcord-python-package}/bin/gunicorn amarcord.cli.webserver:app  --worker-class uvicorn.workers.UvicornWorker "$@"
          '';

          amarcord-docker-image-no-stream = pkgs.dockerTools.buildImage {
            name = "amarcord";
            tag = "latest";

            copyToRoot = pkgs.buildEnv {
              name = "amarcord-docker-root";
              paths = [ amarcord-production-webserver ];
              pathsToLink = [ "/bin" ];
            };

          };
          amarcord-docker-image = pkgs.dockerTools.streamLayeredImage {
            name = "amarcord";
            tag = "latest";

            contents = [
              amarcord-production-webserver
            ];
          };
        };

      devShells.${system} = {
        # It is of course perfectly OK to keep using an impure virtualenv workflow and only use uv2nix to build packages.
        # This devShell simply adds Python and undoes the dependency leakage done by Nixpkgs Python infrastructure.
        impure = pkgs.mkShell {
          packages = [
            python
            pkgs.uv
          ];
          shellHook = ''
            unset PYTHONPATH
            export UV_PYTHON_DOWNLOADS=never
            # sphinxcontrib-spelling wants enchant and uses dlopen, so for now: hack
            export PYENCHANT_LIBRARY_PATH="${pkgs.enchant}/lib/libenchant-2.so";
            # This is also more or less a hack, see
            # https://discourse.nixos.org/t/aspell-dictionaries-are-not-available-to-enchant/39254
            export ASPELL_CONF="dict-dir ${(pkgs.aspellWithDicts (ps: with ps; [ en ]))}/lib/aspell";            
          '';
        };

        # This devShell uses uv2nix to construct a virtual environment purely from Nix, using the same dependency specification as the application.
        # The notable difference is that we also apply another overlay here enabling editable mode ( https://setuptools.pypa.io/en/latest/userguide/development_mode.html ).
        #
        # This means that any changes done to your local files do not require a rebuild.
        default =
          let
            # Create an overlay enabling editable mode for all local dependencies.
            editableOverlay = workspace.mkEditablePyprojectOverlay {
              # Use environment variable
              root = "$REPO_ROOT";
              # Optional: Only enable editable for these packages
              # members = [ "hello-world" ];
            };

            # Override previous set with our overrideable overlay.
            editablePythonSet = pythonSet.overrideScope editableOverlay;

            # Build virtual environment, with local packages being editable.
            #
            # Enable all optional dependencies for development.
            virtualenv = editablePythonSet.mkVirtualEnv "amarcord-serial-dev-env" workspace.deps.all;

          in
          pkgs.mkShell {
            packages = [
              virtualenv
              pkgs.uv
              pkgs.skopeo
              pkgs.basedpyright
              # for docs
              pkgs.glibcLocales
              pkgs.mermaid-cli
              pkgs.gnumake
              # For generating Elm code
              pkgs.openapi-generator-cli
              # To generate the DB diagrams
              pkgs.schemacrawler
              pkgs.shellcheck
            ];
            shellHook = ''
              # Undo dependency propagation by nixpkgs.
              unset PYTHONPATH

              # Don't create venv using uv
              export UV_NO_SYNC=1

              # Prevent uv from downloading managed Python's
              export UV_PYTHON_DOWNLOADS=never

              # Get repository root using git. This is expanded at runtime by the editable `.pth` machinery.
              export REPO_ROOT=$(git rev-parse --show-toplevel)

              # sphinxcontrib-spelling wants enchant and uses dlopen, so for now: hack
              export PYENCHANT_LIBRARY_PATH="${pkgs.enchant}/lib/libenchant-2.so";
              # This is also more or less a hack, see
              # https://discourse.nixos.org/t/aspell-dictionaries-are-not-available-to-enchant/39254
              export ASPELL_CONF="dict-dir ${(pkgs.aspellWithDicts (ps: with ps; [ en ]))}/lib/aspell";            
            '';
          };

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
