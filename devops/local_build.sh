#! /bin/bash

apt-get update -y && apt-get install -qq -y \
	htop \
	nano \
	wget \
	git \
	curl \
	bzip2 \
	build-essential \
	libssl-dev \
	libffi-dev
	# make \
	# automake \
	# gfortran \
	# g++ \
	# libbz2-dev \
	# libsqlite3-dev \
	# libreadline-dev \
	# zlib1g-dev \
	# libncurses5-dev \
	# libgdbm-dev \
	# ca-certificates \
	# libglib2.0-0 \
	# libxext6 \
apt-get -qq -y autoremove && \
	apt-get autoclean && 
	rm -rf /var/lib/apt/lists/* /var/log/dpkg.log

# add conda to root user path
echo 'export PATH=/opt/conda/bin:$PATH' > /etc/profile.d/conda.sh

# install latest miniconda distro
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -bfp /opt/conda
rm Miniconda3-latest-Linux-x86_64.sh
/opt/conda/bin/conda init -q
conda update -n base -q conda

# update conda settings
conda config --system --prepend channels conda-forge && \
    conda config --system --set auto_update_conda false && \
    conda config --system --set show_channel_urls true && \
    conda config --system --set channel_priority strict

# via alpaca account
export POLYGON_API_KEY=""
export APCA_API_KEY_ID=""
# backblaze b2 storage
export B2_ACCESS_KEY_ID=""  # applicationKeyId
export B2_SECRET_ACCESS_KEY=""  # applicationKey
export B2_ENDPOINT_URL="https://s3.us-west-000.backblazeb2.com"
# quant results path
export DATA_LOCAL_PATH="/home/sopadilo/data"
export DATA_S3_PATH="polygon-equities/data"

# install project code
git clone https://github.com/bobcolner/sopadilo
cd sopadilo

# install python deps in conda
conda env create -f conda_quant.yaml
# clea up conda files
conda clean --all --yes
