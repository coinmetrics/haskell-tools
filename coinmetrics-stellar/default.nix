{ nixpkgs }:
rec {
  xdrpp = with nixpkgs; stdenv.mkDerivation rec {
    name = "xdrpp";

    src = builtins.fetchGit {
      url = "https://github.com/xdrpp/xdrpp.git";
    };

    nativeBuildInputs = [ autoreconfHook bison flex pandoc ];

    enableParallelBuilding = true;
  };

  stellar-core = builtins.fetchGit {
    url = "https://github.com/stellar/stellar-core.git";
  };

  xdr-headers = nixpkgs.runCommandCC "xdr-headers" {} ''
    set -e
    mkdir -p $out/include/xdr
    cd ${stellar-core}/src/xdr
    for i in *.x
    do
      ${xdrpp}/bin/xdrc -hh $i -o $out/include/xdr/''${i%.*}.h
    done
  '';

  extraLibs = {
    inherit xdrpp;
  };

  includes = [ xdr-headers ];
}
