# Set correct project and define a REGION variable
gcloud config set project login5-agro
REGION=europe-west1

# Define and create storage locations (buckets) for different purposes
FILES_SOURCE=${DEVSHELL_PROJECT_ID}-files-source
FILES_ERROR=${DEVSHELL_PROJECT_ID}-files-error
FILES_SUCCESS=${DEVSHELL_PROJECT_ID}-files-success
FUNCTIONS_BUCKET=${DEVSHELL_PROJECT_ID}-functions

gsutil mb -c regional -l ${REGION} gs://${FILES_SOURCE}
gsutil mb -c regional -l ${REGION} gs://${FILES_ERROR}
gsutil mb -c coldline -l ${REGION} gs://${FILES_SUCCESS}
gsutil mb -c regional -l ${REGION} gs://${FUNCTIONS_BUCKET}

# Get the source code for specific "pipeline" cloud functions from my git repo
git clone https://github.com/krizman/gcs-bq-streaming-functions-python.git
cd gcs-bq-streaming-functions-python

# Deploy the "streaming" function
gcloud functions deploy streaming --region=${REGION} \
    --source=./functions/streaming --runtime=python37 \
    --stage-bucket=${FUNCTIONS_BUCKET} \
    --trigger-bucket=${FILES_SOURCE} \
    --memory=512MB \
    --timeout=400s

# Define pub/sub events/topics for error and success pipelines
STREAMING_ERROR_TOPIC=streaming_error_topic
STREAMING_SUCCESS_TOPIC=streaming_success_topic
gcloud pubsub topics create ${STREAMING_ERROR_TOPIC}
gcloud pubsub topics create ${STREAMING_SUCCESS_TOPIC}

# Deploy "streaming_error" function used to move corrupt source file to error bucket
gcloud functions deploy streaming_error --region=${REGION} \
	--source=./functions/move_file \
	--entry-point=move_file --runtime=python37 \
	--stage-bucket=${FUNCTIONS_BUCKET} \
	--trigger-topic=${STREAMING_ERROR_TOPIC} \
    --set-env-vars SOURCE_BUCKET=${FILES_SOURCE},DESTINATION_BUCKET=${FILES_ERROR}
	
# Deploy "streaming_success" function used to move successfully parsed source file to success coldline bucket
gcloud functions deploy streaming_success --region=${REGION} \
	--source=./functions/move_file \
	--entry-point=move_file --runtime=python37 \
	--stage-bucket=${FUNCTIONS_BUCKET} \
	--trigger-topic=${STREAMING_SUCCESS_TOPIC} \
    --set-env-vars SOURCE_BUCKET=${FILES_SOURCE},DESTINATION_BUCKET=${FILES_SUCCESS}
	
# create biq query dataset and table with pre-defined schema in my git repo
bq mk Telematics
bq mk Telematics.TractorsData schema.json