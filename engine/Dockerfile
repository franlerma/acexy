FROM wafy80/acestream:latest

# The base image runs as the non-root 'ace' user; apt needs root.
USER root

# Add iproute2 so the container's default gateway (route) can be changed at
# runtime: enables the VPN -> normal network switch.
# The switch entrypoint runs as root to be able to touch the routing table
# (CAP_NET_ADMIN); the engine itself is launched as 'ace' via setpriv.
RUN apt-get update \
    && apt-get install -y --no-install-recommends iproute2 \
    && rm -rf /var/lib/apt/lists/*

COPY switch-entrypoint.sh /switch-entrypoint.sh
RUN chmod +x /switch-entrypoint.sh

ENTRYPOINT ["/switch-entrypoint.sh"]

# Preserve the original start-engine arguments from the base image
CMD ["--client-console", "--bind-all"]
