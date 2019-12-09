FROM python:3.7.5-slim
USER root

ENV DEBIAN_FRONTEND noninteractive
ENV SCALA_VERSION 2.11
ENV KAFKA_VERSION 2.3.0
ENV KAFKA_HOME /opt/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION"

# Install system dependencies and convenience utilities
RUN apt-get update -y && \
    apt-get install -y wget \
                       vim && \
    apt-get clean

# Install conda
ADD https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh /miniconda.sh
RUN sh /miniconda.sh -b -p /conda && /conda/bin/conda update -n base conda
ENV PATH=${PATH}:/conda/bin
SHELL ["/bin/bash", "-c"]

# Add Streamz source code to the build context
ADD . /streamz/.

# Create the conda environment
RUN conda env create --name streamz-dev -f /streamz/conda/environments/streamz_dev.yml

# Build streamz from source
RUN source activate streamz-dev && \
    cd /streamz && \
    python setup.py install

# Install optional dependencies in the conda environment
RUN source activate streamz-dev && \
    conda install -c conda-forge jupyterlab \
                                 numpy \
                                 pandas

# Install Kafka
RUN wget -q http://www.gtlib.gatech.edu/pub/apache/kafka/2.3.0/kafka_2.11-2.3.0.tgz -O /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz && \
        tar xfz /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -C /opt && \
        rm /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz

# Zookeeper & Kafa ports
EXPOSE 2181
EXPOSE 9092

CMD ["/streamz/docker/scripts/entry.sh"]
