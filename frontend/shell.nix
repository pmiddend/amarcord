{ pkgs ? import <nixpkgs> { } }:
let
  easy-ps = import
    (pkgs.fetchFromGitHub {
      owner = "justinwoo";
      repo = "easy-purescript-nix";
      rev = "678070816270726e2f428da873fe3f2736201f42";
      sha256 = "sha256-JEabdJ+3cZEYDVnzgMH/YFsaGtIBiCFcgvVO9XRgiY4=";
    }) {
    inherit pkgs;
  };
in
pkgs.mkShell {
  buildInputs = [
    easy-ps.purs-0_14_5
    easy-ps.purs-tidy
    easy-ps.purescript-language-server
    easy-ps.spago
    pkgs.nodejs
    pkgs.git
    # in case you're wondering: this is for parcel-2. Wtf?
    pkgs.python3
  ];
}
