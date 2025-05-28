# ----------------------------------------------
#  Variables, they should be adjusted to your setups
# ----------------------------------------------
K8S_PROXY=localhost:8001
COMPACION_SERVICE_JAR_FILE=/path/to/Job-CompactionService.jar
COMPACION_SERVICE_K8S_NAME=flink-svc-compactor
COMPACION_SERVICE_FLINK_ENDPOINT=http://$K8S_PROXY/api/v1/namespaces/default/services/$COMPACION_SERVICE_K8S_NAME:webui/proxy

JOB_JAR_FILE=/path/to/Job-ExampleJob-1.0-SNAPSHOT.jar
JOB_K8S_NAME=flink-svc-2-0
JOB_FLINK_ENDPOINT=http://$K8S_PROXY/api/v1/namespaces/default/services/$JOB_K8S_NAME:webui/proxy

# Step-1: Start the compaction service
# 1.1 Upload JAR file to Flink
UPLOAD_OUTPUT=`curl -X POST --header "Except:" -F "jarfile=@$COMPACION_SERVICE_JAR_FILE" $COMPACION_SERVICE_FLINK_ENDPOINT/jars/upload`
COMPACTION_JAR_FILE_NAME=$(echo "$UPLOAD_OUTPUT" | grep -oE 'flink-web-upload/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}_Job-CompactionService\.jar' | sed 's/^flink-web-upload\///')
echo "Uploaded Compaction Jar File to flink:${COMPACTION_JAR_FILE_NAME}"

# 1.2 start Compaction Service
echo "Starting Compaction Service"
curl --location --request POST "$COMPACION_SERVICE_FLINK_ENDPOINT/jars/$COMPACTION_JAR_FILE_NAME/run" \
    --header 'Content-Type: application/json' \
    --data-raw '{
        "entryClass": "org.apache.flink.streaming.examples.compaction.service.Main",
        "programArgsList": ["-jobName", "compaction_service", "-stateBackend", "forst", "-embedCompactionService", "true"]
    }'

# 1.3 get Compaction Service address
GET_TASKMANAGER_OUTPUT=`curl --location --request GET "$COMPACION_SERVICE_FLINK_ENDPOINT/taskmanagers" \
    --header 'Content-Type: application/json' \
    --data-raw '{ }'
`
COMPACTION_TM_ADDRESS=$(echo "$GET_TASKMANAGER_OUTPUT" | grep -oE '"taskmanagers":\[\{"id":"(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):' | grep -oE '\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}')
echo "$GET_TASKMANAGER_OUTPUT"
echo "Started Compaction Service at:$COMPACTION_TM_ADDRESS"


# Step-2: Start the Flink Job
# 2.1 Upload JAR file to Flink
UPLOAD_OUTPUT=`curl -X POST --header "Except:" -F "jarfile=@$JOB_JAR_FILE" $JOB_FLINK_ENDPOINT/jars/upload`
echo $UPLOAD_OUTPUT
JOB_FILE_NAME=$(echo "$UPLOAD_OUTPUT" | grep -oE 'flink-web-upload/[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}_Job-ExampleJob-1.0-SNAPSHOT\.jar' | sed 's/^flink-web-upload\///')
echo "Uploaded Job Jar File to flink:${JOB_FILE_NAME}"

# 1.2 start Compaction Service
echo "Starting Compaction Service"
curl --location --request POST "$JOB_FLINK_ENDPOINT/jars/$JOB_FILE_NAME/run" \
    --header 'Content-Type: application/json' \
    --data-raw '{
        "entryClass": "my.example.ExampleJob",
        "parallelism": "16",
        "programArgsList": ["-jobName", "ExampleJob", "-compactionServiceAddress", "'tri://$COMPACTION_TM_ADDRESS:50051'"],
        "allowNonRestoredState": null
    }'
