{ ghc }:

let
  nixpkgs = import <nixpkgs> {};
  stellar = import ./coinmetrics-stellar/default.nix { inherit nixpkgs; };

in with nixpkgs; haskell.lib.buildStackProject {
  inherit ghc;

  name = "haskell-tools-env";

  buildInputs = [zlib postgresql] ++ stellar.includes ++ lib.attrValues stellar.extraLibs;
}
