{ nixpkgs ? import <nixpkgs> {}
}:
let
  package = import ./default.nix {
    pkgs = nixpkgs;
  };
in with package; {
  inherit bins image;
}
