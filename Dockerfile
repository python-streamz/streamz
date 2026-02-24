# syntax=docker/dockerfile:1
FROM python:3.14-slim
USER root
SHELL ["/bin/bash", "--login", "-c"]

ARG DEBIAN_FRONTEND="noninteractive"
ENV STREAMZ_ENV="streamz-dev"
ENV SCALA_VERSION="2.13"
ENV KAFKA_VERSION="4.2.0"
ENV KAFKA_HOME="/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
ARG KAFKA_SRC_TGZ="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"

RUN apt-get update && \
    apt-get install --yes --no-install-recommends wget vim && \
    rm -rf /var/lib/apt/lists/*

# Install Kafka & Expose Port
RUN wget -q "${KAFKA_SRC_TGZ}" -O "/tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" && \
    tar xfz "/tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -C /opt && \
    rm "/tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
EXPOSE 9092

# Install conda
ADD https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh /miniconda.sh
RUN sh /miniconda.sh -b -p /conda && \
    /conda/bin/conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/main && \
    /conda/bin/conda tos accept --override-channels --channel https://repo.anaconda.com/pkgs/r && \
    /conda/bin/conda update -n base conda
RUN echo "PATH=${PATH}:/conda/bin" >> ~/.bashrc

# Add Streamz source code to the build context
ADD . /streamz/.

# Create the conda environment
RUN conda env create --name ${STREAMZ_ENV} -f /streamz/ci/environment-py314.yml
RUN conda init bash

# Ensures subsequent RUN commands do not need the "conda activate ${STREAMZ_ENV}" command
RUN echo "conda activate ${STREAMZ_ENV}" >> ~/.bashrc

# Install optional dependencies in the conda environment
RUN conda install -c conda-forge jupyterlab \
                                 numpy \
                                 pandas

# Build streamz from source
RUN cd /streamz && \
    pip install -e . --no-deps

CMD ["/streamz/docker/scripts/entry.sh"]
