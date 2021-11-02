ARG POSTGRESVERSION=14
FROM postgres:${POSTGRESVERSION}
ARG POSTGRESVERSION

# If you change these values makes sure to also remove the volume used (which will wipe all data!) for it to update.
ARG NODENAME=node
ARG NODEPASS
ARG NETWORKNAME=network
ARG NETWORKPASS=$NODEPASS
ARG BACKENDNAME=backend
ARG BACKENDPASS

# Setup script
COPY ./FullSetupDB.sql /docker-entrypoint-initdb.d/FullSetupDB
RUN echo -e "#!/bin/bash\n\
psql -U postgres -c \"CREATE DATABASE blockchain_node WITH ENCODING = 'UTF8'; SET synchronous_commit TO off;\"\n\
psql -U postgres -d blockchain_node -v node_username=$NODENAME -v node_password=$NODEPASS -v network_username=$NETWORKNAME \
	-v network_password=$NETWORKPASS -v backend_username=$BACKENDNAME -v backend_password=$BACKENDPASS -f FullSetupDB" > FullSetupDB.sh