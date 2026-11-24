FROM registry.fedoraproject.org/fedora:39

LABEL maintainer="Factory 2 Team" \
      description="A microservice triggered by specific message to tag a build." \
      version="latest"

RUN sed -i '/default_ccache_name = KEYRING:persistent:%{uid}/d' /etc/krb5.conf

RUN chmod 777 /etc/pki/tls/certs/ca-bundle.crt

WORKDIR /src

# This will allow a non-root user to install a custom root CA at run-time
COPY . .

RUN docker/install-dependencies.sh

RUN sed -i '/^koji==/,/--hash=\S*$/d' requirements.txt
RUN sed -i '/^koji==.*$/d' requirements-no-hashes.txt
RUN python3 -m pip install --no-deps .

# Mount to a directory holding the MTS config file
VOLUME /etc/mts
USER 1001
EXPOSE 8080
ENTRYPOINT ["docker/entrypoint.sh"]
