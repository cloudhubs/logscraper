# Aggregator Examples

Log Section (aggregator.log, lines 12-17)

```
{"level":"debug","time":"2020-04-08T19:27:02Z","message":"Consumer has been started, waiting for messages send to topic ccx.ocp.results"}
{"level":"info","offset":1055,"topic":"ccx.ocp.results","group":"ccx_data_pipeline_app","time":"2020-04-08T19:27:02Z","message":"Consumed"}
{"level":"info","offset":1055,"topic":"ccx.ocp.results","organization":11789772,"cluster":"92c04d4a-9f4c-441d-9df9-1e50c426df11","time":"2020-04-08T19:27:02Z","message":"Read"}
{"level":"info","offset":1055,"topic":"ccx.ocp.results","organization":11789772,"cluster":"92c04d4a-9f4c-441d-9df9-1e50c426df11","time":"2020-04-08T19:27:02Z","message":"Organization whitelisted"}
{"level":"info","offset":1055,"topic":"ccx.ocp.results","organization":11789772,"cluster":"92c04d4a-9f4c-441d-9df9-1e50c426df11","time":"2020-04-08T19:27:02Z","message":"Marshalled"}
{"level":"info","offset":1055,"topic":"ccx.ocp.results","organization":11789772,"cluster":"92c04d4a-9f4c-441d-9df9-1e50c426df11","time":"2020-04-08T19:27:02Z","message":"Time ok"}
{"level":"info","offset":1055,"topic":"ccx.ocp.results","organization":11789772,"cluster":"92c04d4a-9f4c-441d-9df9-1e50c426df11","time":"2020-04-08T19:27:02Z","message":"Stored"}
```
Transforms into:
```
{
'messages': [['info', 'Consumed'], ['info', 'Read'], ['info', 'Organization whitelisted'], ['info', 'Marshalled'], ['info', 'Time ok'], ['info', 'Stored']],
'timestamp': '2020-04-08T19:27:02Z',
'organization': 11789772,
'cluster_id': '92c04d4a-9f4c-441d-9df9-1e50c426df11',
'offset': 1055,
'error': False
}
```
-------------------------------------------------------------------------------------------------------------
<div style="page-break-after: always; break-after: page;"></div>

Log Section (aggregator.log, lines 18-23)

```
{"level":"info","offset":1056,"topic":"ccx.ocp.results","group":"ccx_data_pipeline_app","time":"2020-04-08T19:27:02Z","message":"Consumed"}
{"level":"debug","time":"2020-04-08T19:27:02Z","message":"Deserialized report read from message with improper structure:"}
{"level":"debug","time":"2020-04-08T19:27:02Z","message":"map[fingerprints:0xc0000af420 reports:0xc0000af3e0 skips:0xc0000af460 system:0xc0000af3a0]"}
{"level":"error","offset":1056,"topic":"ccx.ocp.results","error":"Improper report structure, missing key info","time":"2020-04-08T19:27:02Z","message":"Error parsing message from Kafka"}
{"level":"error","error":"Improper report structure, missing key info","time":"2020-04-08T19:27:02Z","message":"Error processing message consumed from Kafka"}
{"level":"error","error":"pq: duplicate key value violates unique constraint \"consumer_error_pkey\"","time":"2020-04-08T19:27:02Z","message":"Unable to write consumer error to storage"}
```
Transforms into:
```
{
'messages': [['info', 'Consumed'], ['debug', 'Deserialized report read from message with improper structure:'], ['debug', 'map[fingerprints:0xc0000af420 reports:0xc0000af3e0 skips:0xc0000af460 system:0xc0000af3a0]'], ['error', 'Error parsing message from Kafka'], ['error', 'Improper report structure, missing key info'], ['error', 'Error processing message consumed from Kafka'], ['error', 'pq: duplicate key value violates unique constraint "consumer_error_pkey"'], ['error', 'Unable to write consumer error to storage']], 
'timestamp': '2020-04-08T19:27:02Z',
'organization': None,
'cluster_id': None,
'offset': 1056,
'error': True
}
```
---------------------------------------------------------------------------------------

<div style="page-break-after: always; break-after: page;"></div>

# Pipeline Example

Log Section (pipeline.log, lines )

```
{"levelname": "DEBUG", "asctime": "2020-04-03 19:28:42,364", "name": "controller.kafka_publisher", "filename": "kafka_publisher.py", "message": "Sending response to the ccx.ocp.results topic."}
{"levelname": "DEBUG", "asctime": "2020-04-03 19:28:42,446", "name": "controller.kafka_publisher", "filename": "kafka_publisher.py", "message": "Message has been sent successfully."}
{"levelname": "DEBUG", "asctime": "2020-04-03 19:28:42,447", "name": "controller.kafka_publisher", "filename": "kafka_publisher.py", "message": "Message context: OrgId=11789772, ClusterName=\"bae160b7-b30d-4eb7-ab6e-a3cc3a60e1b7\", LastChecked=\"2020-04-03T19:28:38.886222885Z\""}
{"levelname": "INFO", "asctime": "2020-04-03 19:28:42,448", "name": "controller.kafka_publisher", "filename": "kafka_publisher.py", "message": "Status: Success; Topic: platform.upload.buckit; Partition: 11; Offset: 28; LastChecked: 2020-04-03T19:28:38.886222885Z"}
{"levelname": "DEBUG", "asctime": "2020-04-03 19:28:44,483", "name": "controller.consumer", "filename": "consumer.py", "message": "JSON schema validated"}
{"levelname": "DEBUG", "asctime": "2020-04-03 19:28:44,485", "name": "controller.consumer", "filename": "consumer.py", "message": "Identity schema validated"}
{"levelname": "DEBUG", "asctime": "2020-04-03 19:28:44,485", "name": "controller.consumer", "filename": "consumer.py", "message": "Extracted URL from input message: https://insights-dev-upload-perm.s3.minio.local.net"}
{"levelname": "DEBUG", "asctime": "2020-04-03 19:28:44,485", "name": "insights_messaging.consumers", "filename": "__init__.py", "message": "Downloading https://insights-dev-upload-perm.s3.minio.local.net"}
{"levelname": "DEBUG", "asctime": "2020-04-03 19:28:44,669", "name": "insights_messaging.consumers", "filename": "__init__.py", "message": "Saved https://insights-dev-upload-perm.s3.minio.local.net"}
```
Transforms into:
```
{
'timestamp': '2020-04-03T19:28:42,364',
'organization': 11789772,
'cluster_id': 'bae160b7-b30d-4eb7-ab6e-a3cc3a60e1b7',
'partition': 12,
'offset': 28,
'error': True,
'warning': False,
'messages' [['Sending response to the ccx.ocp.results topic.'], ['Message has been sent successfully.'], ['Message context: OrgId=11789772, ClusterName=\"bae160b7-b30d-4eb7-ab6e-a3cc3a60e1b7\", LastChecked=\"2020-04-03T19:28:38.886222885Z\'], ['Status: Success; Topic: platform.upload.buckit; Partition: 11; Offset: 28; LastChecked: 2020-04-03T19:28:38.886222885Z'], ['JSON schema validated'], ['Identity schema validated'], ['Extracted URL from input message: https://insights-dev-upload-perm.s3.minio.local.net'], ['Downloading https://insights-dev-upload-perm.s3.minio.local.net'], ['Saved https://insights-dev-upload-perm.s3.minio.local.net']]
}
```