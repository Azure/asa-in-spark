# Use Microsoft Docker Image containing dotnet built from Ubuntu
# https://hub.docker.com/_/microsoft-dotnet-core-sdk
FROM mcr.microsoft.com/dotnet/core/sdk:3.1-bionic

RUN apt-get update \
  && apt-get install -y --no-install-recommends software-properties-common

RUN add-apt-repository -y ppa:openjdk-r/ppa

# Install g++ 
# Install java8 (CMakeList.txt has a dependency on java 8)
RUN apt-get update \
  && apt-get install -y build-essential \
  openjdk-8-jdk \
  cmake \
  maven \
  && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME '/usr/lib/jvm/java-8-openjdk-amd64/' 

# Install sbt
RUN echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list
RUN curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | apt-key add
RUN apt-get update && apt-get install -y sbt && rm -rf /var/lib/apt/lists/* 

# create repo dir to store ASASpark source code
# Note that this is also referred in scala/built.sbt
ENV WORKDIR '/repos/ASASpark'
RUN mkdir -p $WORKDIR
# Set working dir as repo root
WORKDIR $WORKDIR
# move code from local disk to container
COPY cpp cpp
COPY dotnet dotnet
COPY java java
COPY scala scala 
COPY build.sh build.sh

# Install Azure Artifact Credential provider to interactively acquire credentials for Azure Artifacts
# https://github.com/microsoft/artifacts-credprovider
RUN cd dotnet && ./installcredprovider.sh

# Open the URL in a browser and enter the code when prompted for auth 
RUN cd dotnet && dotnet restore --interactive


# Argument to control the purpose of creating the docker image
# 'build' (default mode for those interested in just creating the ASA-x.x.x.jar out of this src code repository) - this will create the ASA-x.x.x.jar while building docker image 
# 'debug' (for developers to troubleshoot and work on the source code) - this will not create the jar while building the docker image using this dockerfile. This will just configure the environment and rely on developers to start and connect to the docker container to further develop/troubleshoot
ARG mode=build

# Either create the jar (publish) or do not create (rely on developer to enter into the container and develop/troubleshoot)
RUN ./build.sh $mode

CMD ["/bin/bash"]
