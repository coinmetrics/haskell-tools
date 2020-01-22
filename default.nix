{ pkgs }:
rec {
  hanalytics = pkgs.fetchFromGitHub {
    owner = "quyse";
    repo = "hanalytics";
    rev = "dbe294c4c1524268683764c3cd3ec947105359c9";
    sha256 = "0wpib53843xggnd70vxxivmv9cd0gqgfpia3v3f81q0vdf8mz61k";
  };

  packages = pkgs.haskellPackages.override {
    overrides = pkgs.lib.composeExtensions (pkgs.haskell.lib.packageSourceOverrides {
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

      diskhash = "0.0.4.0";
    }) (self: super: with pkgs.haskell.lib; {
      diskhash = dontCheck super.diskhash;
    });
  };

  bins = with packages; {
    inherit coinmetrics-export coinmetrics-monitor;
  };

  env = pkgs.buildEnv {
    name = "haskell-tools";
    paths = builtins.attrValues bins;
  };

  image = { name ? "coinmetrics/haskell-tools", tag ? "latest" }: pkgs.dockerTools.buildImage {
    inherit name tag;
    contents = [ pkgs.cacert ];
    config = {
      Env = [ "PATH=${env}/bin" ];
      User = "1000:1000";
    };
  };
}
