{ stdenv, elmPackages, lib, nodePackages, uglymol, fetchurl }:

let
  mkDerivation =
    { srcs ? ./elm-srcs.nix
    , src
    , name
    , srcdir ? "./src"
    , targets ? [ ]
    , registryDat ? ./registry.dat
    , outputJavaScript ? false
    }:
    stdenv.mkDerivation {
      inherit name src;

      buildInputs = [ elmPackages.elm ]
        ++ lib.optional outputJavaScript nodePackages.uglify-js;

      buildPhase = elmPackages.fetchElmDeps {
        elmPackages = import srcs;
        elmVersion = "0.19.1";
        inherit registryDat;
      };

      installPhase =
        let
          elmfile = module: "${srcdir}/${builtins.replaceStrings ["."] ["/"] module}.elm";
          extension = if outputJavaScript then "js" else "html";
        in
        ''
          mkdir -p $out/share/doc
          ${lib.concatStrings (map (module: ''
            echo "compiling ${elmfile module}"
            elm make ${elmfile module} --output $out/${module}.${extension} --docs $out/share/doc/${module}.json
            ${lib.optionalString outputJavaScript ''
              echo "minifying ${elmfile module}"
              uglifyjs $out/${module}.${extension} --compress 'pure_funcs="F2,F3,F4,F5,F6,F7,F8,F9,A2,A3,A4,A5,A6,A7,A8,A9",pure_getters,keep_fargs=false,unsafe_comps,unsafe' \
                  | uglifyjs --mangle --output $out/${module}.min.${extension}
            ''}
          '') targets)}
        '';
    };
  jsDerivation = mkDerivation {
    name = "amarcord-frontend-js-1.0.0";
    srcs = ./elm-srcs.nix;
    src = ./.;
    targets = [ "Main" ];
    srcdir = "./src";
    outputJavaScript = true;
  };
  mtzJs = fetchurl {
    url = "https://raw.githubusercontent.com/uglymol/uglymol.github.io/master/wasm/mtz.js";
    hash = "sha256-Ut9ZJnGu+hbJBWD+XW14mosJ1Lr3tcRx+pHOP+q+awo=";
  };
  mtzWasm = fetchurl {
    url = "https://raw.githubusercontent.com/uglymol/uglymol.github.io/master/wasm/mtz.wasm";
    hash = "sha256-B71/bdEMs/yLMbHzmDiWeQNeoR80pUgNJmnzR/7Pabk=";
  };
in
stdenv.mkDerivation {
  src = ./.;
  name = "amarcord-frontend-1.0.0";
  phases = "unpackPhase installPhase";
  installPhase = ''
    mkdir -p $out
    cp ${jsDerivation}/Main.min.js $out/main.js
    cp ${mtzJs} $out/mtz.js
    cp ${mtzWasm} $out/mtz.wasm
    cp uglymol-custom-element.js $out/
    echo ${uglymol}
    cp src/index.html ${uglymol}/uglymol.min.js ./*.svg ./*.css ./*.png ./*.jpg $out/
  '';
}

