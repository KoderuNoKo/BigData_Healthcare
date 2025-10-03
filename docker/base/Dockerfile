# Start from an official Ubuntu image
FROM ubuntu:22.04

# Set environment variables (avoid tzdata prompt, etc.)
ENV DEBIAN_FRONTEND=noninteractive

# Install common dev tools
RUN apt-get update && apt-get install -y \
    sudo \
    curl \
    wget \
    git \
    vim \
    nano \
    python3 \
    python3-pip \
    openjdk-11-jdk \
    build-essential \
    net-tools \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Install prerequisites for add-apt-repository
RUN apt-get update && apt-get install -y \
    software-properties-common \
    && add-apt-repository ppa:deadsnakes/ppa -y \
    && apt-get update

# Install Python 3.12
RUN apt-get install -y python3.12 python3.12-venv

# Set python3 to point to 3.12
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1 \
 && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.12 2

# Install pip for 3.12
RUN curl -sS https://bootstrap.pypa.io/get-pip.py | python3.12


# Create a non-root user (so devs don’t all run as root)
ARG USER=dev
ARG UID=1000
ARG GID=1000

RUN groupadd -g ${GID} ${USER} \
 && useradd -m -u ${UID} -g ${GID} -s /bin/bash ${USER}

USER ${USER}
WORKDIR /home/${USER}/app