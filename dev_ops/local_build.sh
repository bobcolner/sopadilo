#! /bin/bash

apt-get update -y && apt-get install -qq -y \
    htop \
    nethogs \
    ncdu \
    nano \
    wget \
    git \
    curl

apt-get -qq -y autoremove && \
    apt-get autoclean && \
    rm -rf /var/lib/apt/lists/* /var/log/dpkg.log

# install latest miniconda distro
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -bfp /opt/conda
rm Miniconda3-latest-Linux-x86_64.sh
# add conda to root user path
echo 'export PATH=/opt/conda/bin:$PATH' > /etc/profile.d/conda.sh
conda init -q
conda update -q -n base -q conda

# update conda settings
conda config --system --prepend channels conda-forge && \
    conda config --system --set auto_update_conda false && \
    conda config --system --set show_channel_urls true && \
    conda config --system --set channel_priority strict

# install project code
cd /home/azureuser
mkdir data
git clone https://github.com/bobcolner/sopadilo
cd sopadilo

# install python deps in conda
conda env create -f dev_ops/conda_quant.yaml
# clea up conda files
conda clean --all --yes
