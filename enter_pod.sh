#!/bin/bash

# Get output to ODC output

name_pod=dataset-pipeline-zbshq

# Enter the pod
kubectl exec -it $name_pod -- /bin/bash
