FROM python:3.7.5-slim
USER root
SHELL ["/bin/bash", "--login", "-c"]

ENV DEBIAN_FRONTEND noninteractive
ENV SCALA_VERSION 2.11
ENV KAFKA_VERSION 2.3.0
ENV KAFKA_HOME /opt/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION"

# Install conda
ADD https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh /miniconda.sh
RUN sh /miniconda.sh -b -p /conda && /conda/bin/conda update -n base conda
RUN echo "PATH=${PATH}:/conda/bin" >> ~/.bashrc

# Add Streamz source code to the build context
ADD . /streamz/.

# Create the conda environment
RUN conda env create --name streamz-dev -f /streamz/conda/environments/streamz_dev.yml
RUN conda init bash

# Ensures subsequent RUN commands do not need the "conda activate streamz_dev" command
RUN echo "conda activate streamz_dev" >> ~/.bashrc

# Build streamz from source
RUN cd /streamz && \
    python setup.py install

# Install optional dependencies in the conda environment
RUN conda install -c conda-forge jupyterlab \
                                 numpy \
                                 pandas \
                                 wget \
                                 vim

# Install Kafka
RUN wget -q http://www.gtlib.gatech.edu/pub/apache/kafka/2.3.0/kafka_2.11-2.3.0.tgz -O /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz && \
        tar xfz /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -C /opt && \
        rm /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz

# Zookeeper & Kafa ports
EXPOSE 2181
EXPOSE 9092

CMD ["/streamz/docker/scripts/entry.sh"]
