{ nixpkgs ? import <nixpkgs> {} }:
let
  packages = import ./default.nix { inherit nixpkgs; };
in with nixpkgs; mkShell {
  buildInputs = with packages; [ xdrpp xdr-headers ];
}
