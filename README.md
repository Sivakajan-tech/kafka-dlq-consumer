# kafka-dlq-consumer
It is a K8S job to consume `n` number of message that can not be consumed by the Consumer.

# Run Procedure

Apply the Kubernetes Job using:

```
kubectl apply -f k8s_job.yaml
```

Monitor logs to verify message consumption:

```
kubectl logs job/kafka-dlq-consumer-job -n se 
```

This setup ensures that invalid messages are systematically processed, preventing the system from being blocked by un-processable data.