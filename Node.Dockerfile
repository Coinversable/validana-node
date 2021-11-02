# Enter the same version as the processor here
ARG NODEVERSION=16
FROM node:${NODEVERSION}

# Clone the projects into the docker container and compile it
ENV NODE_ENV=production
ENV NODE_NO_WARNINGS=1
RUN yarn global add typescript
RUN git clone https://github.com/Coinversable/validana-node.git --branch v2.2.0 /usr/node
RUN yarn --cwd /usr/node install --frozen-lockfile
RUN tsc -p /usr/node/tsconfig.json

# Add environment variables
# If you are the processor and want to use this enter a random length 64 hex string. As node the processor should have this value for you.
#ENV VNODE_ENCRYPTIONKEY=
#ENV VNODE_SENTRYURL=
#ENV VNODE_DBPASSWORD=
#ENV VNODE_DBPASSWORD_NETWORK=
#ENV VNODE_DBUSER=node
#ENV VNODE_DBUSER_NETWORK=network
#ENV VNODE_DBNAME=blockchain
#ENV VNODE_DBHOST=localhost
#ENV VNODE_DBPORT=5432
#ENV VNODE_DBMINCONNECTIONS=0
#ENV VNODE_DBMAXCONNECTIONS=10
#ENV VNODE_LOGLEVEL=0
#ENV VNODE_LISTENPORT=29473
#ENV VNODE_MAXMEMORYNODE=512
#ENV VNODE_MAXMEMORYNETWORK=512
#ENV VNODE_MAXPEERS=20
#ENV VNODE_PEERTIMEOUT=60
# Also available: $severity
#ENV VNODE_LOGFORMAT $color$timestamp: $message: $error

# In case this is a normal node (default). The processor should have some of these values for you.
#ENV VNODE_ISPROCESSOR=false
#ENV VNODE_TLS=true
#ENV VNODE_PUBLICKEY=
#ENV VNODE_PROCESSORHOST=
#ENV VNODE_PROCESSORPORT=443
#ENV VNODE_MAXOUTSTANDINGBLOCKS=500
#ENV VNODE_MINDOWNLOADSPEED=10
#ENV VNODE_MINPEERS=5

# In case this is the processor node
#ENV VNODE_ISPROCESSOR=true
#ENV VNODE_SIGNPREFIX=
#ENV VNODE_PROCESSORPORT=443
#ENV VNODE_REMEMBERPEER=168
#ENV VNODE_BLOCKINTERVAL=5
#ENV VNODE_LATESTEXISTINGBLOCK=-1
#ENV VNODE_MAXBLOCKSIZE=1000000
# Add certificate if wanted
#ENV VNODE_TLS=true
#ENV VNODE_CERTPATH=/usr/node/certificates/mycert.key
#ENV VNODE_KEYPATH=/usr/node/certificates/mycert.cert
# Copy the certificate into the container. Alternately start the container with: -v ~./certificates:/usr/node/certificates
#COPY ./certificates/mycert.key /usr/node/certificates/mycert.key
#COPY ./certificates/mycert.cert /usr/node/certificates/mycert.cert

#Add user and entry point
USER node
WORKDIR /usr/node
ENTRYPOINT ["node","dist/index.js"]