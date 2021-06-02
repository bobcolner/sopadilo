# fat-vm
IP: 52.183.59.153
DNS: raystation.westus2.cloudapp.azure.com
name: raystation

az vm start --resource-group fat-vm --name raystation
az vm stop --resource-group fat-vm --name raystation

ssh -i ~/.ssh/config azure_admin@raystation.westus2.cloudapp.azure.com

scp ~/QuantClarity/sopadilo/tmp/secerets.json \
	azure_admin@raystation.westus2.cloudapp.azure.com:/home/azure_admin/sopadilo/tmp/secerets.json


## ray cluster creator

# Create or update the cluster. When the command finishes, it will print
# out the command that can be used to SSH into the cluster head node.
cd /Users/bobcolner/QuantClarity/sopadilo/dev_ops/ray/cloud_cluster_launcher
ray up cluster_azure_prod.yaml

# Get a remote screen on the head node.
ray attach cluster_azure_prod.yaml
# or
ssh -i ~/.ssh/config ubuntu@52.183.62.176
# or
ssh -o IdentitiesOnly=yes -i ~/.ssh/id_rsa ubuntu@52.183.62.176

# test ray setup
python -c 'import ray; ray.init(address="auto")'
exit
# Tear down the cluster.
ray down cluster_azure_prod.yaml


# ray head node
IP: 40.117.181.194
FQDN: ray-head.eastus.cloudapp.azure.com

# start/stop vm
az vm start --resource-group ray --name ray-node-head
az vm stop --resource-group ray --name ray-node-head

az vmss start --resource-group ray --name ray-node-workers
az vmss stop --resource-group ray --name ray-node-workers

# manual ssh into vm with static IP
ssh -i ~/.ssh/config ubuntu@40.117.181.194
ssh -i ~/.ssh/config ubuntu@ray-head.eastus.cloudapp.azure.com

# clone repo
git clone https://github.com/bobcolner/sopadilo.git

# copy files from local to vm
scp -r ~/QuantClarity/sopadilo/ ubuntu@ray-head.eastus.cloudapp.azure.com:/home/ubuntu/
# copy from remote vm to local
scp -r ubuntu@ray-head.eastus.cloudapp.azure.com:/home/ubuntu/ ~/QuantClarity/sopadilo/

# pip install 'ray[default]' pandas statsmodels pyarrow s3fs fsspec pandas_market_calendars pandas-bokeh

# jupyter lab
https://40.117.181.194:8000/user/ubuntu/lab
https://ray-head.eastus.cloudapp.azure.com:8000/user/ubuntu/lab

# ray ui
http://40.117.181.194:8265
http://ray-head.eastus.cloudapp.azure.com:8265
