import csv
import re
import subprocess
from pathlib import Path
from prefect import flow, task
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
    
    # Summary stats
    completed = sum(1 for t in tasks if t["status"] == "COMPLETED")
    failed = sum(1 for t in tasks if t["status"] == "FAILED")
    logger.info(f"Tasks: {completed} completed, {failed} failed, {len(tasks)} total")
    
    return tasks


@flow(log_prints=True)
def hello_world() -> None:
    """Run Nextflow hello world with task reporting."""
    trace_file = run_nextflow(pipeline="hello")
    tasks = report_nextflow_tasks(trace_file)
    
    print(f"Pipeline complete! Ran {len(tasks)} processes")


if __name__ == "__main__":
    hello_world()