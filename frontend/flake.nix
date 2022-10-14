{
  description = "Flake for AMARCORD frontend";

  inputs.flake-utils.url = "github:numtide/flake-utils";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs, flake-utils, poetry2nix }:
    {
      # Nixpkgs overlay providing the application
      overlay = nixpkgs.lib.composeManyExtensions [
        poetry2nix.overlay
        (final: prev: {
          # The application
          amarcord-frontend = prev.pkgs.callPackage ./default.nix { };
        })
      ];
    } // (flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ self.overlay ];
        };
      in
      {
        packages = {
          amarcord-frontend = pkgs.amarcord-frontend;
        };

        defaultPackage = pkgs.amarcord-frontend;

        devShell = pkgs.mkShell {
          buildInputs = [ pkgs.elmPackages.elm pkgs.elmPackages.elm-review pkgs.nodejs pkgs.elm2nix ];
        };
      }));
}
