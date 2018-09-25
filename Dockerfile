FROM debian:buster

RUN set -ex; \
	if ! command -v gpg > /dev/null; then \
		apt-get update; \
		apt-get install -y --no-install-recommends \
			netbase \
			libnuma1 \
			ca-certificates \
			libpq5 \
		; \
		rm -rf /var/lib/apt/lists/*; \
	fi

COPY coinmetrics-export coinmetrics-monitor /usr/bin/

RUN useradd -m -u 1000 -s /bin/bash coinmetrics
USER coinmetrics
WORKDIR /home/coinmetrics
