let
  pkgs = import (import ../nix/sources.nix { }).nixpkgs { };
in pkgs.mkShell {
  buildInputs = [ pkgs.elmPackages.elm pkgs.elmPackages.elm-review pkgs.nodejs pkgs.elm2nix ];
}
