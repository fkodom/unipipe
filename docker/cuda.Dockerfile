ARG UBUNTU_VERSION=20.04
ARG CUDA_VERSION=11.4.2
ARG CUDA_OPSET=base

FROM nvidia/cuda:$CUDA_VERSION-$CUDA_OPSET-ubuntu$UBUNTU_VERSION
ENV DEBIAN_FRONTEND="noninteractive"

# Install apt-get dependencies and Google Cloud SDK, then remove
# files that are not needed to run the container.
RUN apt update --fix-missing \
    && apt install -y software-properties-common \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*

ARG PY_VERSION=3.9

RUN add-apt-repository ppa:deadsnakes/ppa && apt update \
    && apt install -y python$PY_VERSION-dev \
    && update-alternatives --install /usr/bin/python python /usr/bin/python$PY_VERSION 999
# && apt update \
RUN apt install -y python3-setuptools \
    && python -m easy_install pip \
    && apt clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY ./ ./unipipe
RUN pip install ./unipipe