{ nixpkgs }:
rec {
  hanalytics = nixpkgs.fetchFromGitHub {
    owner = "quyse";
    repo = "hanalytics";
    rev = "dbe294c4c1524268683764c3cd3ec947105359c9";
    sha256 = "0wpib53843xggnd70vxxivmv9cd0gqgfpia3v3f81q0vdf8mz61k";
  };

  packages = nixpkgs.haskellPackages.override {
    overrides = let
      deps = self: super: stellarPackage.extraLibs;

      sourceOverrides = nixpkgs.haskell.lib.packageSourceOverrides {
        coinmetrics = ./coinmetrics;
        coinmetrics-all-blockchains = ./coinmetrics-all-blockchains;
        coinmetrics-binance = ./coinmetrics-binance;
        coinmetrics-bitcoin = ./coinmetrics-bitcoin;
        coinmetrics-cardano = ./coinmetrics-cardano;
        coinmetrics-cosmos = ./coinmetrics-cosmos;
        coinmetrics-eos = ./coinmetrics-eos;
        coinmetrics-ethereum = ./coinmetrics-ethereum;
        coinmetrics-export = ./coinmetrics-export;
        coinmetrics-grin = ./coinmetrics-grin;
        coinmetrics-iota = ./coinmetrics-iota;
        coinmetrics-monero = ./coinmetrics-monero;
        coinmetrics-monitor = ./coinmetrics-monitor;
        coinmetrics-nem = ./coinmetrics-nem;
        coinmetrics-neo = ./coinmetrics-neo;
        coinmetrics-ripple = ./coinmetrics-ripple;
        coinmetrics-rosetta = ./coinmetrics-rosetta;
        coinmetrics-stellar = ./coinmetrics-stellar;
        coinmetrics-storage = ./coinmetrics-storage;
        coinmetrics-tendermint = ./coinmetrics-tendermint;
        coinmetrics-tezos = ./coinmetrics-tezos;
        coinmetrics-tron = ./coinmetrics-tron;
        coinmetrics-waves = ./coinmetrics-waves;

        hanalytics-avro = "${hanalytics}/hanalytics-avro";
        hanalytics-base = "${hanalytics}/hanalytics-base";
        hanalytics-bigquery = "${hanalytics}/hanalytics-bigquery";
        hanalytics-postgres = "${hanalytics}/hanalytics-postgres";

        avro = "0.4.7.0";
        diskhash = "0.0.4.0";
      };

      tweaks = self: super: with nixpkgs.haskell.lib; {
        coinmetrics-stellar = super.coinmetrics-stellar.overrideAttrs (attrs: {
          buildInputs = attrs.buildInputs ++ stellarPackage.includes;
        });

        diskhash = dontCheck super.diskhash;
      };

      stellarPackage = import ./coinmetrics-stellar/default.nix { inherit nixpkgs; };

    in nixpkgs.lib.foldl nixpkgs.lib.composeExtensions deps [ sourceOverrides tweaks ];
  };

  bins = with (builtins.mapAttrs (name: pkg: nixpkgs.haskell.lib.justStaticExecutables pkg) packages); {
    inherit coinmetrics-export coinmetrics-monitor;
  };

  env = additionalContents: nixpkgs.buildEnv {
    name = "haskell-tools";
    paths = builtins.attrValues bins ++ additionalContents;
  };

  image = { name ? "coinmetrics/haskell-tools", tag ? "latest", additionalContents ? [] }: nixpkgs.dockerTools.buildImage {
    inherit name tag;
    contents = [ nixpkgs.cacert ];
    config = {
      Env = [ "PATH=${env additionalContents}/bin" ];
      User = "1000:1000";
    };
  };
}
