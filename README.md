# GKE Workload Class Controller

WorkloadClass establishes a high-level control layer, allowing Platform Engineers to define operational guardrails and Workload Owners to simply declare their desired workload outcomes.

This project implements the GKE Workload Class feature. It allows platform engineers and workload owners to collaborate on managing workload disruptions during maintenance.

## Disclaimer

This is not an officially supported Google product.
This project is not eligible for the Google Open Source Software Vulnerability Rewards Program.

## Description
The Workload Class Controller manages the lifecycle of disruption policies, ensuring they adhere to organizational guardrails. It also detects and warns if multiple WorkloadClasses in the same namespace share the same PodSelector. A Validating Admission Webhook intercepts eviction requests for pods and enforces temporal constraints (allowed windows) and pod lifecycle protection (minimum run duration).

## Example Usage

**1. Platform Engineer defines a Guardrail**

The platform team creates a `WorkloadClassGuardrail` (Cluster-scoped) to enforce organizational limits:

```yaml
apiVersion: workloads.gke.io/v1
kind: WorkloadClassGuardrail
metadata:
  name: default
spec:
  constraints:
    disruption:
      allowedDisruptionDays:
        - Saturday
        - Sunday
      maxAllowedWindows: 2
      maxNonDisruptionDurationDays: 30
```

**2. Workload Owner defines a WorkloadClass**

The workload owner creates a `WorkloadClass` (Namespace-scoped) to protect their specific pods from disruption during active hours:

```yaml
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: critical-batch
  namespace: sample
spec:
  podSelector:
    matchLabels:
      role: batch-processor
  disruptionPolicy:
    allowedDisruptionWindows:
      - name: "weekend-maintenance"
        daysOfWeek:
          - Saturday
          - Sunday
        startTime: "00:00"
        endTime: "04:00"
        timeZone: "America/Toronto"
    minInitialRunDurationDays: 2
    maxNonDisruptionDurationDays: 1
    allowedDisruptionsOutsideOfWindow:
      - VPA
      - ClusterAutoscaler
```

**3. Testing the Capabilities**

**A. Testing Disruption Webhooks**

You can deploy a dummy deployment that matches the `WorkloadClass` selector and attempt to disrupt its pods. If the current time is outside the `weekend-maintenance` window, the webhook will intercept and reject the disruption:

```sh
# 1. Apply the CRDs, dummy deployment, and sample namespace
kubectl apply -k config/samples/

# 2. Attempt to evict the deployment's pod outside the maintenance window using the Eviction API
kubectl proxy &
PROXY_PID=$!
sleep 2

POD_NAME=$(kubectl get pods -n sample -l role=batch-processor -o jsonpath='{.items[0].metadata.name}')
curl -s -X POST "http://127.0.0.1:8001/api/v1/namespaces/sample/pods/$POD_NAME/eviction" \
  -H "Content-Type: application/json" \
  -d "{\"apiVersion\": \"policy/v1\", \"kind\": \"Eviction\", \"metadata\": {\"name\": \"$POD_NAME\", \"namespace\": \"sample\"}}"

kill $PROXY_PID
```
*Expected Output (if outside window):*
```json
{
  "kind": "Status",
  "apiVersion": "v1",
  "metadata": {},
  "status": "Failure",
  "message": "admission webhook \"vpoddisruption.gke.io\" denied the request: Eviction blocked: currently outside of allowed disruption windows for WorkloadClass critical-batch",
  "reason": "Forbidden",
  "code": 403
}
```

**B. Testing Guardrail Validation**

You can attempt to update the `WorkloadClass` with an invalid policy that violates the `WorkloadClassGuardrail` (e.g., setting 32 maxNonDisruptionDurationDays when the guardrail only allows 30). The validation will fail and the WorkloadClass Status will be updated:

```sh
# Attempt to apply a WorkloadClass with too many windows or invalid duration
kubectl apply -f config/samples/workloads_v1_workloadclass.yaml
```
*Expected Status (if invalid):*
```sh
# Describe the WorkloadClass
kubectl describe workloadclass critical-workload -n demo
```
```
Status:
  Conditions:
    Last Transition Time:  2026-06-05T14:59:29Z
    Message:               maxNonDisruptionDurationDays 32 exceeds guardrail limit 30
    Reason:                ValidationFailed
    Status:                False
    Type:                  Validated
  Maintenance Readiness:   NotReady
Events:                    <none>
```

**C. Testing Selector Conflict Validation**

If you create multiple `WorkloadClass` resources with the same `podSelector` in the same namespace, the controller will detect the conflict and emit a warning Event:

```sh
# Apply a duplicate WorkloadClass in the same namespace with the same selector
kubectl apply -f config/samples/workloads_v1_workloadclass_duplicate.yaml
```

*Expected Events:*
```sh
# Describe the duplicate WorkloadClass
kubectl describe workloadclass critical-batch-duplicate -n sample
```
```
Events:
  Type     Reason            Age   From                       Message
  ----     ------            ----  ----                       -------
  Warning  ValidationFailed  10s   workloadclass-controller   the following WorkloadClasses have the same PodSelector as critical-batch-duplicate: critical-batch
```


## Getting Started

### Prerequisites
- go version v1.24.6+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.
- cert-manager version v1.x (required for webhooks).

### For Developers: Deploy using Make

> **NOTE:** Our GitHub Actions workflow automatically builds and pushes the controller image to `ghcr.io/gke-labs/workload-class` on every push to `main`. You only need to build locally if you are testing uncommitted changes.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster using the public image:**

```sh
make deploy IMG=ghcr.io/gke-labs/workload-class:latest
```

*(If you are testing local code changes, you can optionally build and push your own image first using `make docker-build docker-push IMG=<your-registry>/workload-class:dev` and then deploy with that `IMG`.)*

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples have default values to test it out.

### For Developers: Uninstall using Make
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## For Users: Deploying to GKE with `kubectl`

This section guides you through creating a GKE cluster and deploying the Workload Class controller and resources using `gcloud` and `kubectl` directly, using our pre-built public container image.

### 1. Create a GKE Cluster

Create a GKE Standard cluster to run your workloads:

```sh
gcloud container clusters create workload-class-demo \
    --region us-central1 \
    --num-nodes 3 \
    --project YOUR_PROJECT_ID
```

After creation, ensure your `kubectl` is configured to connect to the cluster:
```sh
gcloud container clusters get-credentials workload-class-demo \
    --region us-central1 \
    --project YOUR_PROJECT_ID
```
*(Note: Adjust the region and project ID as necessary.)*

### 2. Install cert-manager

The controller's webhooks require `cert-manager` to provision certificates. Install it on your cluster:

```sh
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.20.2/cert-manager.yaml
```

Wait for the cert-manager pods to be up and running:
```sh
kubectl wait --for=condition=ready pod -l app.kubernetes.io/instance=cert-manager -n cert-manager --timeout=300s
```

### 3. Deploy the Controller

Deploy the controller directly from the official release bundle. This configuration is already set to automatically pull the latest image from our [public GitHub Container Registry package](https://github.com/gke-labs/workload-class/pkgs/container/workload-class).

Apply the configuration to your cluster:

```sh
kubectl apply -f https://raw.githubusercontent.com/gke-labs/workload-class/main/dist/install.yaml
```

### 4. Create the Guardrail and Workload Class

Once the controller is running, you can create the guardrail and workload class resources.

Apply the sample guardrail:
```sh
kubectl apply -f https://raw.githubusercontent.com/gke-labs/workload-class/main/config/samples/workloads_v1_workloadclassguardrail.yaml
```

Apply a sample namespace:
```sh
kubectl apply -f https://raw.githubusercontent.com/gke-labs/workload-class/main/config/samples/sample_namespace.yaml
```

Apply the sample workload class:
```sh
kubectl apply -f https://raw.githubusercontent.com/gke-labs/workload-class/main/config/samples/workloads_v1_workloadclass.yaml
```

Alternatively, you can apply all samples (including the test namespace and dummy deployment) directly via Kustomize:
```sh
kubectl apply -k https://github.com/gke-labs/workload-class/config/samples\?ref\=main
```

## Project Distribution

Following the options to release and provide this solution to the users.

### By providing a bundle with all YAML files

**1. Build the installer for the image built and published in the registry:**

```sh
make build-installer IMG=ghcr.io/gke-labs/workload-class:latest
```

**NOTE:** The makefile target mentioned above generates an `install.yaml`
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without its
dependencies.

**2. Using the installer**

Users can just run `kubectl apply -f <URL for YAML BUNDLE>` to install
the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/gke-labs/workload-class/main/dist/install.yaml
```

### By providing a Helm Chart

1. Build the chart using the optional helm plugin

```sh
kubebuilder edit --plugins=helm/v2-alpha
```

2. See that a chart was generated under 'dist/chart', and users
can obtain this solution from there.

**NOTE:** If you change the project, you need to update the Helm Chart
using the same command above to sync the latest changes. Furthermore,
if you create webhooks, you need to use the above command with
the '--force' flag and manually ensure that any custom configuration
previously added to 'dist/chart/values.yaml' or 'dist/chart/manager/manager.yaml'
is manually re-applied afterwards.

## Contributing

We welcome contributions! Please see [docs/contributing.md](docs/contributing.md) for more information.
We follow [Google's Open Source Community Guidelines](https://opensource.google.com/conduct/).

**NOTE:** Run `make help` for more information on all potential `make` targets. More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html).

## License

This project is licensed under the [Apache 2.0 License](LICENSE).

