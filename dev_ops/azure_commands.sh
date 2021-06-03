# ray-vm
IP: 40.65.111.128
DNS: raystation.westus2.cloudapp.azure.com
name: raystation
login: azureuser@raystation.westus2.cloudapp.azure.com


az vm start --resource-group ray-vm --name raystation
az vm stop --resource-group ray-vm --name raystation

# ssh -i ~/.ssh/config azureuser@raystation.westus2.cloudapp.azure.com
ssh -i ~/.ssh/config azureuser@40.65.111.128

# start notebook server 
# https://medium.com/analytics-vidhya/setting-up-jupyter-lab-instance-on-google-cloud-platform-3a7acaa732b7
jupyter serverextension enable --py jupyterlab --sys-prefix
jupyter lab --ip 0.0.0.0 --port 8888 --no-browser --allow-root

http://40.65.111.128:8888

# clone repo
git clone https://github.com/bobcolner/sopadilo.git

scp ~/QuantClarity/sopadilo/tmp/secerets.json \
	azureuser@40.65.111.128:/home/azureuser/sopadilo/secerets.json
