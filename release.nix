{ nixpkgs ? import <nixpkgs> {}
}:
let
  package = import ./default.nix {
    pkgs = nixpkgs;
  };
in {
  image = package.image;
}
