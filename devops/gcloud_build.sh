# gcloud python SDK docs
#   https://github.com/googleapis/google-cloud-python#google-cloud-python-client


# create vm (n2-standard-2, e2-highcpu-8)
gcloud compute instances create sopadilo \
    --zone=us-west1-b \
    --machine-type=n2-standard-2 \
    --boot-disk-size=50GB \
    --image-family=debian-10 \
    --image-project=debian-cloud \
    --service-account="sopadilo@emerald-skill-201716.iam.gserviceaccount.com" \
    --scopes=cloud-platform \
    --labels="service=sopadilo" \
    --tags=sopadilo \
    --metadata startup-script="/opt/conda/bin/conda run -n quant python /home/bobcolner/sopadilo/run.py" \
    --metadata statup-mode="adhoc" #adhoc, workflow, workflow-spindown


gcloud compute firewall-rules create sopadilo \
    --allow=tcp:8080,tcp:80,tcp:8888,tcp:8786-8787 \
    --target-tags=sopadilo \
    --description="jupyter lab, dask distributed schedular & dashboard"


gcloud iam service-accounts create sopadilo \
    --display-name=sopadilo \
    --description="sopadilo batch jobs"


gcloud projects add-iam-policy-binding emerald-skill-201716 \
    --member="serviceAccount:sopadilo@emerald-skill-201716.iam.gserviceaccount.com" \
    --role=roles/storage.admin
    # --role=roles/bigquery.admin
    # --role=roles/automl.admin
    # --role=roles/cloudscheduler.admin
    # --role=roles/logging.writer


# copy local to remote
gcloud compute scp --zone us-west1-b --recurse \
    /Users/bobcolner/QuantClarity/sopadilo \
    gcp-id:/remote/path


# create pub/sub
gcloud pubsub topics create spinup-sopadilo
gcloud pubsub topics create shutdown-sopadilo


# create cloud functions (in nodejs source dir)
git clone https://github.com/GoogleCloudPlatform/nodejs-docs-samples.git
cd nodejs-docs-samples/functions/scheduleinstance/

gcloud functions deploy startInstancePubSub \
    --region=us-central1 \
    --trigger-topic=spinup-sopadilo \
    --runtime=nodejs8

gcloud functions deploy stopInstancePubSub \
    --region=us-central1 \
    --trigger-topic=shutdown-sopadilo \
    --runtime=nodejs8


# cloud cron scheduler
gcloud scheduler jobs create pubsub spinup-sopadilo \
    --schedule="0 1 * * *" \
    --time-zone=UTC \
    --topic=spinup-sopadilo \
    --message-body '{"zone":"us-west1-b", "label":"service=sopadilo"}'

gcloud scheduler jobs create pubsub shutdown-sopadilo \
    --schedule="0 3 * * *" \
    --time-zone=UTC \
    --topic=shutdown-sopadilo \
    --message-body '{"zone":"us-west1-b", "label":"service=sopadilo"}'

# Scheduling compute instances with Cloud Scheduler
#   https://cloud.google.com/scheduler/docs/start-and-stop-compute-engine-instances-on-a-schedule
# create cloud scheduler
#       field          allowed values
#       -----          --------------
#       minute         0-59
#       hour           0-23
#       day of month   1-31
#       month          1-12 (or names, see below)
#       day of week    0-7 (0 or 7 is Sunday, or use names)
# A field may contain an asterisk (*), which always stands for "first-last".
