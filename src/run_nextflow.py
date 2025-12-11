import subprocess
from pathlib import Path
from prefect import flow, task, tags
from prefect.logging import get_run_logger


@task
def run_nextflow(
    pipeline: str,
    params: dict | None = None,
    profile: str | None = None,
    work_dir: Path | None = None,
    resume: bool = False,
) -> subprocess.CompletedProcess:
    """Execute a Nextflow pipeline."""
    logger = get_run_logger()
    
    cmd = ["nextflow", "run", pipeline]
    
    if profile:
        cmd.extend(["-profile", profile])
    
    if work_dir:
        cmd.extend(["-work-dir", str(work_dir)])
    
    if resume:
        cmd.append("-resume")
    
    # Add pipeline parameters
    if params:
        for key, value in params.items():
            cmd.extend([f"--{key}", str(value)])
    
    logger.info(f"Running: {' '.join(cmd)}")
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
    )
    
    # Log output
    if result.stdout:
        logger.info(result.stdout)
    if result.stderr:
        logger.warning(result.stderr)
    
    # Raise on failure so Prefect marks the task as failed
    result.check_returncode()
    
    return result


@flow(log_prints=True)
def hello_world() -> None:
    """Run Nextflow's hello world demo pipeline."""
    
    run_nextflow(
        pipeline="hello",  # Nextflow's built-in demo
        # No params needed for the hello demo
        # No profile needed - it's pure Groovy, no containers
    )
    
    print("Pipeline complete!")


if __name__ == "__main__":
    hello_world()