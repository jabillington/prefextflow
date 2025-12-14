
# Prefect flows for running Nextflow pipelines.

## Deployment options:

1. Quick start (serve mode - blocks and runs locally):

   `python src/nextflow_flows.py serve`

2. Production deployment:
   First, create a work pool (once)

   `prefect work-pool create default-agent-pool --type process`
   
   Deploy the flows:

   `prefect deploy --all`
   
   Start a worker to pick up runs:
   
   `prefect worker start --pool default-agent-pool`

3. Run from CLI:
   `prefect deployment run 'hello-world/nextflow-hello'`
   `prefect deployment run 'run-pipeline/nextflow-custom' --param pipeline=nf-core/rnaseq`

4. Run from UI:

   `Open http://localhost:4200 (or your Prefect server URL)`
