{ nixpkgs ? import <nixpkgs> {}
}:
let
  package = import ./default.nix {
    pkgs = nixpkgs;
  };
  tools = with nixpkgs; {
    inherit skopeo;
  };
in with package; {
  inherit bins image tools;
  touch = bins // {
    image = image {};
  } // tools;
}
