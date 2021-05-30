
# ray head node
IP: 40.117.181.194
FQDN: ray-head.eastus.cloudapp.azure.com

# manual ssh into vm with static IP
ssh -i ~/.ssh/config ubuntu@40.117.181.194
ssh -i ~/.ssh/config ubuntu@ray-head.eastus.cloudapp.azure.com

# copy files from local to vm
scp -r ~/QuantClarity/sopadilo/ ubuntu@ray-head.eastus.cloudapp.azure.com:/home/ubuntu/
# copy from remote vm to local
scp -r ubuntu@ray-head.eastus.cloudapp.azure.com:/home/ubuntu/ ~/QuantClarity/sopadilo/

pip install 'ray[default tune]' tune-sklearn statsmodels pyarrow s3fs fsspec pandas_market_calendars pandas-bokeh