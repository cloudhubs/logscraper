# Data pipeline architecture

## Integration with OCM

Customer Services are part of the overall workflow developed and deployed by the CCX team. Customers are now able to display information about their clusters in the OpenShift Cluster Manager (OCM). It is also possible to control their clusters through OCM. The plan for CCX team is to have a new tab in OCM to display information about clusters status (health, performance) based on results from insights rules.


## Overall architecture

![architecture](Pipeline.png "Overall architecture")

1. Event about new data from insights operator is consumed from Kafka. That event contains (among other things) URL to S3 Bucket
1. Insights operator data is read from S3 Bucket and insights rules are applied to that data in `ccx-data-pipeline`
1. Results (basically organization ID + cluster name + insights results JSON) are stored back into Kafka, but into different topic
1. That results are consumed by `insights-results-db-writer` service 
1. `insights-results-db-writer` stores insights results into AWS RDS database
1. `insights-results-aggregator` provides such data via REST API to other tools, like OpenShift Cluster Manager web UI, OpenShift console, etc.

## Components overview

### ccx-data-pipeline

A hosted service that reads new insights data from S3 Bucket (based on events sent from Kafka).
Insights rules are applied to such data and result (in JSON format) is sent back into Kafka into different topic.

### insights-results-aggregator

A hosted service hosted that provides REST API endpoints for cached insights results.

### insights-results-db-writer

A hosted service that stores Insight OCP data that are being consumed by OpenShift Cluster Manager.
Insights OCP data are consumed from selected broker and stored in a storage (that basically works as a cache). The service is
based on the same code as insights-results-aggregator but runs with different parameters.

## Real deployment

`ccx-data-pipeline` are deployed in eight pods because it's work can be done in parallel
`insights-results-db-writer` runs in one pod only - writing to storage is critical part there
`insights-results-aggregator` runs in two to eight pods, scalable as necessary
`ccx-data-pipeline-db` is deployment with storage, one pod (of course)

### An example of deployment

```
$ oc get pods
NAME                                   READY     STATUS    RESTARTS   AGE
ccx-data-pipeline-24-5qlfx             1/1       Running   3          6d
ccx-data-pipeline-24-7pp4d             1/1       Running   4          6d
ccx-data-pipeline-24-7sgbt             1/1       Running   3          6d
ccx-data-pipeline-24-bbw9d             1/1       Running   4          6d
ccx-data-pipeline-24-k2ls6             1/1       Running   0          3d
ccx-data-pipeline-24-knrcp             1/1       Running   0          3h
ccx-data-pipeline-db-4-q9phv           1/1       Running   1          2d
insights-results-aggregator-36-2qjdw   1/1       Running   0          6d
insights-results-aggregator-36-frvrc   1/1       Running   0          6d
insights-results-db-writer-15-nlcz9    1/1       Running   1          6d
```

