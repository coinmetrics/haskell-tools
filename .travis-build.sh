#!/bin/bash

set -ex

export FPM_PACKAGE_INFO="--license MIT --vendor CoinMetrics.io --url https://github.com/coinmetrics-io/haskell-tools"

mkdir bin completions pkg

# build binaries
stack --no-terminal test --haddock --no-haddock-deps --copy-bins --local-bin-path bin

# coinmetrics-export package
./bin/coinmetrics-export --bash-completion-script /usr/bin/coinmetrics-export > completions/coinmetrics-export
fpm -s dir -t deb -n coinmetrics-export -p pkg ${FPM_PACKAGE_INFO} -d libpq5          -v 0.0.0.${TRAVIS_BUILD_NUMBER} bin/coinmetrics-export=/usr/bin/ completions/coinmetrics-export=/usr/share/bash-completion/completions/
fpm -s dir -t rpm -n coinmetrics-export -p pkg ${FPM_PACKAGE_INFO} -d postgresql-libs -v 0.0.0.${TRAVIS_BUILD_NUMBER} bin/coinmetrics-export=/usr/bin/ completions/coinmetrics-export=/usr/share/bash-completion/completions/
envsubst < coinmetrics-export/bintray.json.in > coinmetrics-export/bintray.json
envsubst < coinmetrics-export/bintray-deb.json.in > coinmetrics-export/bintray-deb.json
envsubst < coinmetrics-export/bintray-rpm.json.in > coinmetrics-export/bintray-rpm.json

# coinmetrics-monitor package
./bin/coinmetrics-monitor --bash-completion-script /usr/bin/coinmetrics-monitor > completions/coinmetrics-monitor
fpm -s dir -t deb -n coinmetrics-monitor -p pkg ${FPM_PACKAGE_INFO} -v 0.0.0.${TRAVIS_BUILD_NUMBER} bin/coinmetrics-monitor=/usr/bin/ completions/coinmetrics-monitor=/usr/share/bash-completion/completions/
fpm -s dir -t rpm -n coinmetrics-monitor -p pkg ${FPM_PACKAGE_INFO} -v 0.0.0.${TRAVIS_BUILD_NUMBER} bin/coinmetrics-monitor=/usr/bin/ completions/coinmetrics-monitor=/usr/share/bash-completion/completions/
envsubst < coinmetrics-monitor/bintray.json.in > coinmetrics-monitor/bintray.json
envsubst < coinmetrics-monitor/bintray-deb.json.in > coinmetrics-monitor/bintray-deb.json
envsubst < coinmetrics-monitor/bintray-rpm.json.in > coinmetrics-monitor/bintray-rpm.json
