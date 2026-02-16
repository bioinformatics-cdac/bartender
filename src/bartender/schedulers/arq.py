from asyncio import TimeoutError, create_subprocess_shell, gather, sleep, to_thread
import logging
from datetime import timedelta
from typing import Any, Literal, Optional, Union

from functools import partial
from arq import ArqRedis, Worker, create_pool
from arq.connections import RedisSettings
from arq.jobs import Job, JobStatus
from pydantic import BaseModel, RedisDsn, parse_obj_as
from pydantic.types import PositiveInt

from bartender.db.models.job_model import State
from bartender.schedulers.abstract import AbstractScheduler, JobDescription

# try:
from kubernetes import client, config as k8s_config
# except ImportError:
#     client = None
#     k8s_config = None

logger = logging.getLogger(__name__)


def _map_arq_status(arq_status: JobStatus, success: bool) -> State:
    status_map: dict[JobStatus, State] = {
        JobStatus.deferred: "queued",
        JobStatus.queued: "queued",
        JobStatus.in_progress: "running",
    }
    if arq_status == JobStatus.complete:
        if success:
            return "ok"
        return "error"
    try:
        return status_map[arq_status]
    except KeyError:
        # fallback to error when arq status is unmapped.
        return "error"


class ArqSchedulerConfig(BaseModel):
    """Configuration for ArqScheduler."""

    type: Literal["arq"] = "arq"
    redis_dsn: RedisDsn = parse_obj_as(RedisDsn, "redis://localhost:6379")
    queue: str = "arq:queue"
    max_jobs: PositiveInt = 1  # noqa: WPS462
    """Maximum number of jobs to run at a time inside a single worker."""  # noqa: E501, WPS322, WPS428
    job_timeout: Union[PositiveInt, timedelta] = 43200 #3600  # noqa: WPS462
    """Maximum job run time.

    Default is one hour.

    In seconds or string in
    `ISO 8601 duration format <https://en.wikipedia.org/wiki/ISO_8601#Durations>`_.

    For example, "PT12H" represents a max runtime of "twelve hours".
    """  # noqa: WPS428

    @property
    def redis_settings(self) -> RedisSettings:
        """Settings for arq.

        Returns:
            The settings based on redis_dsn.
        """
        return RedisSettings.from_dsn(self.redis_dsn)

    k8s_namespace: Optional[str] = None
    k8s_image: Optional[str] = None



class ArqScheduler(AbstractScheduler):
    """Arq scheduler.

    See https://arq-docs.helpmanual.io/.
    """

    def __init__(self, config: ArqSchedulerConfig) -> None:
        """Arq scheduler.

        Args:
            config: The config.
        """
        self.config: ArqSchedulerConfig = config
        self.connection: Optional[ArqRedis] = None

    async def close(self) -> None:  # noqa: D102
        if self.connection is not None:
            await self.connection.close()

    async def submit(self, description: JobDescription) -> str:  # noqa: D102
        pool = await self._pool()
        job = await pool.enqueue_job("_exec", description)
        if job is None:
            # TODO better error?
            raise RuntimeError("Job already exists")
        return job.job_id

    async def state(self, job_id: str) -> State:  # noqa: D102
        pool = await self._pool()
        job = Job(job_id, pool)
        arq_status = await job.status()
        success = False
        if arq_status == JobStatus.not_found:
            raise KeyError(job_id)
        if arq_status == JobStatus.complete:
            result = await job.result_info()
            if result is None:
                raise RuntimeError(
                    f"Failed to fetch result from completed arq job {job_id}",
                )
            success = result.success
        return _map_arq_status(arq_status, success)

    async def cancel(self, job_id: str) -> None:  # noqa: D102
        pool = await self._pool()
        job = Job(job_id, pool)
        try:
            await job.abort(timeout=0.1)
        except TimeoutError:
            # job.result() times out on cancelled queued job
            return

    def __eq__(self, other: object) -> bool:
        return isinstance(other, ArqScheduler) and self.config == other.config

    def __repr__(self) -> str:
        config = repr(self.config)
        return f"ArqScheduler(config={config})"

    async def _pool(self) -> ArqRedis:
        if self.connection is None:
            self.connection = await create_pool(
                self.config.redis_settings,
                default_queue_name=self.config.queue,
            )
        return self.connection


class JobFailureError(Exception):
    """Error during job running."""


async def _exec(  # noqa: WPS210
    ctx: dict[Any, Any],
    description: JobDescription,
) -> None:
    config: ArqSchedulerConfig = ctx["config"]
    # if config.k8s_image:
    await _exec_k8s(ctx, description, config)
    # else:
    #     await _exec_local(description)


async def _exec_k8s(
    ctx: dict[Any, Any],
    description: JobDescription,
    config: ArqSchedulerConfig,
) -> None:
    print(f"ctx: {ctx}", flush=True)
    print(f"description: {description}", flush=True)
    print(f"config: {config}", flush=True)
    if client is None:
        raise ImportError("kubernetes library is not installed")

    # Load config
    try:
        await to_thread(k8s_config.load_incluster_config)
    except k8s_config.ConfigException:
        await to_thread(k8s_config.load_kube_config)

    batch_api = client.BatchV1Api()
    core_api = client.CoreV1Api()
    
    job_id = ctx["job_id"]
    namespace = "haddock"
    job_name = f"bartender-{job_id}"
    
    # Create Job Object
    job = client.V1Job(
        api_version="batch/v1",
        kind="Job",
        metadata=client.V1ObjectMeta(name=job_name),
        spec=client.V1JobSpec(
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"app": "bartender-job"}),
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    containers=[
                        client.V1Container(
                            name="job",
                            image="icr.icecloud.in/k8s/bartender",
                            command=["/bin/sh", "-c", f"cd {description.job_dir} && {description.command}"],
                            volume_mounts=[
                                client.V1VolumeMount(
                                    name="job-data",
                                    mount_path="/jobs",
                                )
                            ],
                        )
                    ],
                    volumes=[
                        client.V1Volume(
                            name="job-data",
                            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                                claim_name="bartender-job-data"
                            )
                        )
                    ]
                )
            ),
            backoff_limit=0,
        )
    )

    logger.info(f"Submitting K8s job {job_name} to {namespace}")
    await to_thread(batch_api.create_namespaced_job, namespace, job)

    # Wait for completion
    while True:
        try:
            k8s_job = await to_thread(batch_api.read_namespaced_job_status, job_name, namespace)
            if k8s_job.status.succeeded:
                break
            if k8s_job.status.failed:
                raise JobFailureError(f"K8s job {job_name} failed")
        except client.ApiException as e:
            logger.error(f"Error checking job status: {e}")
            raise
        await sleep(30)

    # Convert logs
    pod_list = await to_thread(
        core_api.list_namespaced_pod,
        namespace,
        label_selector=f"job-name={job_name}"
    )
    if pod_list.items:
        pod_name = pod_list.items[0].metadata.name
        logs = await to_thread(core_api.read_namespaced_pod_log, pod_name, namespace)
        # Assuming simple stdout capture. Splitting stderr is harder without stream.
        # But read_namespaced_pod_log returns string.
        # We'll write to stdout.txt
        (description.job_dir / "stdout.txt").write_text(logs)
        (description.job_dir / "stderr.txt").write_text("") # No separate stderr
        (description.job_dir / "returncode").write_text("0")
    
    # Cleanup
    await to_thread(
        batch_api.delete_namespaced_job,
        job_name,
        namespace,
        propagation_policy="Background",
    )


async def _exec_local(  # noqa: WPS210
    description: JobDescription,
) -> None:
    stderr_fn = description.job_dir / "stderr.txt"
    stdout_fn = description.job_dir / "stdout.txt"

    with open(stderr_fn, "w") as stderr:
        with open(stdout_fn, "w") as stdout:
            proc = await create_subprocess_shell(
                description.command,
                stdout=stdout,
                stderr=stderr,
                cwd=description.job_dir,
            )
            returncode = await proc.wait()
            (description.job_dir / "returncode").write_text(str(returncode))
            if returncode != 0:
                raise JobFailureError(
                    f"Job failed with return code {returncode}",
                )


def arq_worker(config: ArqSchedulerConfig, burst: bool = False) -> Worker:
    """Worker that runs jobs submitted to arq queue.

    Args:
        config: The config. Should be equal to the one used to submit job.
        burst: Whether to stop the worker once all jobs have been run.

    Returns:
        A worker.
    """
async def _startup(ctx: dict[Any, Any], config: ArqSchedulerConfig) -> None:
    ctx["config"] = config


def arq_worker(config: ArqSchedulerConfig, burst: bool = False) -> Worker:
    """Worker that runs jobs submitted to arq queue.

    Args:
        config: The config. Should be equal to the one used to submit job.
        burst: Whether to stop the worker once all jobs have been run.

    Returns:
        A worker.
    """
    
    print(f"arq_worker config: {config}", flush=True)

    functions = [_exec]
    return Worker(
        on_startup=partial(_startup, config=config),

        redis_settings=config.redis_settings,
        queue_name=config.queue,
        max_jobs=config.max_jobs,
        job_timeout=config.job_timeout,
        functions=functions,  # type: ignore
        burst=burst,
        allow_abort_jobs=True,
    )


async def run_workers(configs: list[ArqSchedulerConfig]) -> None:
    """Run worker for each arq scheduler config.

    Args:
        configs: The configs.
    """
    print(f"run_workers config: {configs}", flush=True)
    workers = [arq_worker(config) for config in configs]
    await gather(*[worker.async_run() for worker in workers])

