FROM registry.fedoraproject.org/fedora:45

LABEL maintainer="Factory 2 Team" \
      description="A microservice triggered by specific message to tag a build." \
      version="latest"

RUN sed -i '/default_ccache_name = KEYRING:persistent:%{uid}/d' /etc/krb5.conf

# Fedora 44 dropped the /etc/pki/tls/certs/ca-bundle.crt compatibility symlink,
# so address the extracted bundle directly. docker/install-ca.sh appends to
# this same file at run time, which is why it has to be group/world writable:
# the container runs as an arbitrary non-root uid and cannot use
# update-ca-trust.
ENV CA_BUNDLE=/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem
RUN chmod 666 $CA_BUNDLE

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
