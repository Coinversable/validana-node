FROM postgres:10

ARG NODENAME=node
ARG NODEPASS
ARG BACKENDNAME=backend
ARG BACKENDPASS

COPY ./FullSetupDB.sql /docker-entrypoint-initdb.d/FullSetupDB.sql
RUN sed -i -r \
	-e "s/^--CREATE DATABASE /CREATE DATABASE /g" \
	-e "s/^--\\\\c /\\\\c /g" \
	-e "s/\/\*'Node password here'\*\//'$NODEPASS'/g" \
	-e "s/usename = 'node'/usename = '$NODENAME'/g" \
	-e "s/ROLE node/ROLE $NODENAME/g" \
	-e "s/TO node;/TO $NODENAME;/g" \
	-e "s/\/\*'Backend password here'\*\//'$BACKENDPASS'/g" \
	-e "s/usename = 'backend'/usename = '$BACKENDNAME'/g" \
	-e "s/ROLE backend/ROLE $BACKENDNAME/g" \
	-e "s/TO backend;/TO $BACKENDNAME;/g" \
	/docker-entrypoint-initdb.d/FullSetupDB.sql