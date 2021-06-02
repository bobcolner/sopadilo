# ray-vm
IP: 40.65.111.128
DNS: raystation.westus2.cloudapp.azure.com
name: raystation

raystation.westus2.cloudapp.azure.com:8888/?token=f190976d8e748a764ca7184fbbbe7b402df7baa263a4e273

az vm start --resource-group ray-vm --name raystation
az vm stop --resource-group ray-vm --name raystation

ssh -i ~/.ssh/config azureuser@40.65.111.128

# start notebook server 
# https://medium.com/analytics-vidhya/setting-up-jupyter-lab-instance-on-google-cloud-platform-3a7acaa732b7
jupyter serverextension enable --py jupyterlab --sys-prefix
jupyter lab --ip 0.0.0.0 --port 8888 --no-browser

# clone repo
git clone https://github.com/bobcolner/sopadilo.git

scp ~/QuantClarity/sopadilo/tmp/secerets.json \
	azureuser@raystation.westus2.cloudapp.azure.com:/home/azureuser/sopadilo/tmp/secerets.json

pip install 'ray[default]' pandas statsmodels pyarrow s3fs fsspec pandas_market_calendars pandas-bokeh
