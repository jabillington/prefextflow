"""
Prefect flows for running Nextflow pipelines.

Deployment options:

1. Quick start (serve mode - blocks and runs locally):
   python src/nextflow_flows.py serve

2. Production deployment:
   # First, create a work pool (once)
   prefect work-pool create default-agent-pool --type process
   
   # Deploy the flows
   prefect deploy --all
   
   # Start a worker to pick up runs
   prefect worker start --pool default-agent-pool

3. Run from CLI:
   prefect deployment run 'hello-world/nextflow-hello'
   prefect deployment run 'run-pipeline/nextflow-custom' --param pipeline=nf-core/rnaseq

4. Run from UI:
   Open http://localhost:4200 (or your Prefect server URL)
"""

import csv
import re
import subprocess
import sys
from pathlib import Path
from prefect import flow, task, serve
from prefect.artifacts import create_table_artifact
from prefect.logging import get_run_logger

ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])|\[K|\r')


@task
def run_nextflow(
    pipeline: str,
    params: dict | None = None,
    profile: str | None = None,
    work_dir: Path | None = None,
    resume: bool = False,
) -> Path:
    """Execute a Nextflow pipeline with tracing enabled."""
    logger = get_run_logger()
    
    trace_file = Path(f"trace-{pipeline.replace('/', '-')}.txt")
    
    cmd = [
        "nextflow", "run", pipeline,
        "-ansi-log", "false",
        "-with-trace", str(trace_file),
    ]
    
    if profile:
        cmd.extend(["-profile", profile])
    if work_dir:
        cmd.extend(["-work-dir", str(work_dir)])
    if resume:
        cmd.append("-resume")
    if params:
        for key, value in params.items():
            cmd.extend([f"--{key}", str(value)])
    
    logger.info(f"Running: {' '.join(cmd)}")
    
    with subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    ) as proc:
        for line in proc.stdout:
            cleaned = ANSI_ESCAPE.sub('', line).strip()
            if cleaned:
                logger.info(cleaned)
        proc.wait()
    
    if proc.returncode != 0:
        raise RuntimeError(f"Nextflow failed with exit code {proc.returncode}")
    
    return trace_file


@task
def report_nextflow_tasks(trace_file: Path) -> list[dict]:
    """Parse Nextflow trace and create a Prefect artifact."""
    logger = get_run_logger()
    
    if not trace_file.exists():
        logger.warning(f"Trace file not found: {trace_file}")
        return []
    
    with open(trace_file) as f:
        reader = csv.DictReader(f, delimiter='\t')
        tasks = []
        for row in reader:
            tasks.append({
                "process": row.get("name", ""),
                "status": row.get("status", ""),
                "duration": row.get("duration", ""),
                "realtime": row.get("realtime", ""),
                "cpu": row.get("%cpu", ""),
                "memory": row.get("peak_rss", ""),
                "exit": row.get("exit", ""),
            })
    
    create_table_artifact(
        key="nextflow-tasks",
        table=tasks,
        description="Nextflow process execution details",
    )
    
    completed = sum(1 for t in tasks if t["status"] == "COMPLETED")
    failed = sum(1 for t in tasks if t["status"] == "FAILED")
    logger.info(f"Tasks: {completed} completed, {failed} failed, {len(tasks)} total")
    
    return tasks


@flow(log_prints=True)
def hello_world(
    params: dict | None = None,
    profile: str | None = None,
    work_dir: str | None = None,
    resume: bool = False,
) -> list[dict]:
    """Run Nextflow hello world with task reporting."""
    trace_file = run_nextflow(pipeline="hello_world",    
                              params=params,
                              profile=profile,
                              work_dir=Path(work_dir) if work_dir else None,
                              resume=resume)
    tasks = report_nextflow_tasks(trace_file)
    print(f"Pipeline complete! Ran {len(tasks)} processes")


@flow(log_prints=True)
def run_pipeline(
    pipeline: str,
    params: dict | None = None,
    profile: str | None = None,
    work_dir: str | None = None,
    resume: bool = False,
) -> list[dict]:
    """
    Run any Nextflow pipeline with configurable parameters.
    
    Args:
        pipeline: Pipeline name or path (e.g., 'nf-core/rnaseq', './my_pipeline.nf')
        params: Pipeline-specific parameters as key-value pairs
        profile: Nextflow profile to use (e.g., 'singularity', 'docker', 'lsf')
        work_dir: Working directory for intermediate files
        resume: Whether to resume from cached results
    
    Returns:
        List of task execution details from the trace
    """
    trace_file = run_nextflow(
        pipeline=pipeline,
        params=params,
        profile=profile,
        work_dir=Path(work_dir) if work_dir else None,
        resume=resume,
    )
    tasks = report_nextflow_tasks(trace_file)
    print(f"Pipeline '{pipeline}' complete! Ran {len(tasks)} processes")
    return tasks


def main():
    """Entry point with subcommands."""
    if len(sys.argv) < 2:
        print("Usage: python nextflow_flows.py [serve|run|deploy]")
        print("  serve  - Start serving deployments (development)")
        print("  run    - Run hello_world directly")
        print("  deploy - Print deployment instructions")
        sys.exit(1)
    
    cmd = sys.argv[1]
    
    if cmd == "serve":
        # Serve multiple flows concurrently
        hello_deploy = hello_world.to_deployment(
            name="nextflow-hello",
            tags=["nextflow", "test"],
        )
        pipeline_deploy = run_pipeline.to_deployment(
            name="nextflow-custom",
            tags=["nextflow", "genomics"],
        )
        serve(hello_deploy, pipeline_deploy)
    
    elif cmd == "run":
        hello_world()
    
    elif cmd == "deploy":
        print("""
To deploy to Prefect server:

1. Ensure Prefect server is running:
   prefect server start
   
2. Create a work pool:
   prefect work-pool create genomics-pool --type process
   
3. Deploy the flows (uses prefect.yaml):
   prefect deploy --all
   
4. Start a worker:
   prefect worker start --pool genomics-pool
   
5. Trigger runs from UI or CLI:
   prefect deployment run 'hello-world/nextflow-hello'
   prefect deployment run 'run-pipeline/nextflow-custom' \\
       --param pipeline=nf-core/rnaseq \\
       --param profile=singularity
""")
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()