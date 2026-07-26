"""Full AI Pipeline Orchestrator."""

import logging

import redis.asyncio as aioredis
from asgiref.sync import async_to_sync
from sqlalchemy import select

from app.celery_worker import celery_app as celery
from app.core.config import get_settings
from app.core.database import make_celery_session
from app.core.redis import RedisService, redis_pool
from app.models.chunk import Chunk, ChunkStatus
from app.models.job import Job, JobStatus, JobType
from app.pipeline.analyzer import analyze_files
from app.pipeline.catalog import CatalogEntry, lookup
from app.pipeline.detector import detect_file
from app.pipeline.generator import DockerfileGenerator
from app.pipeline.security import SecurityScanner, ThreatLevel
from app.pipeline.splitter import compute_chunks
from app.pipeline.verifier import DockerConfigVerifier
from app.services.minio_service import minio_service

logger = logging.getLogger(__name__)
settings = get_settings()


async def send_customer_update(job_id: str, step: str, detail: str):
    import json
    import redis as sync_redis
    
    payload = json.dumps({
        "type": "detection_step",
        "job_id": job_id,
        "step": step,
        "detail": detail
    })
    
    try:
        # Use sync Redis to avoid event loop issues in Celery prefork workers
        r = sync_redis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
        r.publish("job_updates", payload)
        r.close()
    except Exception as e:
        logger.warning(f"Could not publish update for job {job_id}: {e}")


async def _mark_job_failed(job_id: str):
    """Set a job's status to FAILED (best-effort)."""
    try:
        async with make_celery_session() as session:
            result = await session.execute(select(Job).where(Job.id == job_id))
            job = result.scalar_one_or_none()
            if job and job.status not in (JobStatus.FAILED, JobStatus.CANCELLED):
                job.status = JobStatus.FAILED
                await session.commit()
    except Exception as e:
        logger.error(f"Could not mark job {job_id} failed: {e}")


async def process_pipeline_async(job_id: str, user_id: str):
    # Retrieve MinIO object keys
    prefix = f"{job_id}/"
    file_keys = minio_service.list_objects(settings.BUCKET_JOB_INPUTS, prefix=prefix)

    if not file_keys:
        logger.error(f"No files found for job {job_id}")
        return

    await send_customer_update(job_id, "detecting", "Reading file formats safely via MinIO...")

    # 1. Detect
    detections = []
    for key in file_keys:
        filename = key.split("/")[-1]
        det = detect_file(job_id, key, filename)
        detections.append(det)

    # 2. Analyze
    await send_customer_update(job_id, "analyzing", "Deep context analysis running...")

    try:
        profile = analyze_files(job_id, file_keys, detections)
    except Exception as e:
        logger.info(f"Heuristics analysis failed for {job_id}: {e}. Triggering Gemini workload classifier...")
        try:
            from app.pipeline.classifier import GeminiClassifier
            classifier = GeminiClassifier()
            profile = await classifier.classify(file_keys)
        except Exception as classifier_error:
            logger.error(f"Gemini classifier failed for {job_id}: {classifier_error}. Prompting for Dockerfile.")
            async with make_celery_session() as session:
                result = await session.execute(select(Job).where(Job.id == job_id))
                job = result.scalar_one_or_none()
                if job:
                    job.status = JobStatus.NEEDS_DOCKERFILE
                    await session.commit()
            await send_customer_update(job_id, "needs_dockerfile", "Pipeline paused. Could not auto-detect workload type from uploaded file(s). Please provide a Dockerfile or authorize AI generation.")
            return

    await send_customer_update(job_id, "analyzing", f"Detected profile: {profile.type} ({profile.framework})")

    # 2.5. Security Scan (Tier 1)
    await send_customer_update(job_id, "security_scan", "Scanning for malicious patterns and crypto-miners...")
    scanner = SecurityScanner()
    try:
        findings = await scanner.scan_job(job_id, file_keys)

        # Block if any HIGH or CRITICAL threats are found
        high_threats = [f for f in findings if f.threat_level in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]]
        if high_threats:
            msg = f"Security Violation: {high_threats[0].category} ({high_threats[0].message})"
            await send_customer_update(job_id, "failed", msg)

            # Update DB
            async with make_celery_session() as session:
                result = await session.execute(select(Job).where(Job.id == job_id))
                job = result.scalar_one_or_none()
                if job:
                    job.status = JobStatus.FAILED
                    await session.commit()
            return

        if findings:
            await send_customer_update(job_id, "security_scan", f"Security: {len(findings)} low/medium warnings found (Execution permitted).")
        else:
            await send_customer_update(job_id, "security_scan", "Security verification passed.")

    except Exception as e:
        logger.error(f"Security scan failed for {job_id}: {e}")
        # We don't fail for internal scanner errors, but log them
        pass

    # 3. Container image resolution
    # 3a. Custom Dockerfile (user-supplied) takes precedence for python-style jobs.
    cat_entry = None
    if profile.custom_dockerfile and profile.type in ("ml_training", "data"):
        await send_customer_update(job_id, "catalog", "Using your custom Dockerfile...")
        from app.pipeline.catalog import GENERIC_PYTHON_ENTRYPOINT, parse_dockerfile
        try:
            df_bytes = minio_service.download_bytes(settings.BUCKET_JOB_INPUTS, profile.custom_dockerfile)
            base_image, setup_cmds = parse_dockerfile(df_bytes.decode("utf-8", errors="ignore"))
        except Exception as e:
            await send_customer_update(job_id, "failed", f"Could not read your Dockerfile: {e}")
            await _mark_job_failed(job_id)
            return
        if not base_image:
            base_image = ("nvidia/cuda:12.1.1-cudnn8-runtime-ubuntu22.04"
                          if profile.gpu_required else "python:3.11-slim")
        cat_entry = CatalogEntry(
            image=base_image,
            entrypoint_template=GENERIC_PYTHON_ENTRYPOINT,
            env_vars=["INPUT", "OUTPUT_PATH", "CHUNK_START", "CHUNK_END", "JOB_ID"],
            gpu_required=profile.gpu_required,
            preinstalled_packages=[],
            tested=False,
            setup_commands=setup_cmds,
        )
        await send_customer_update(job_id, "catalog", f"Configured from your Dockerfile (base {base_image}).")

    # 3b. Tier 1 catalog lookup.
    if cat_entry is None:
        await send_customer_update(job_id, "catalog", "Looking for matching Pre-verified Docker Containers...")
        cat_entry = lookup(profile)

    if not cat_entry:
        await send_customer_update(job_id, "catalog", "Triggering Gemini AI Container Config Generator...")
        try:
            import io
            import zipfile
            from app.pipeline.catalog import GENERIC_PYTHON_ENTRYPOINT

            # Fetch script code for context from zip or flat file
            zip_key = next((k for k in file_keys if k.endswith('.zip')), None)
            src_bytes = b""
            build_files: dict[str, str] = {}
            requirements_txt: str | None = None

            # Build manifest filenames to search for
            manifest_filenames = [
                "CMakeLists.txt", "Cargo.toml", "Makefile", "go.mod", "package.json"
            ]

            if zip_key:
                zip_bytes = minio_service.download_bytes(settings.BUCKET_JOB_INPUTS, zip_key)
                with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
                    # Read entry file source
                    try:
                        src_bytes = z.read(profile.entry_file)
                    except Exception:
                        pass

                    # Extract build manifest files (first 4KB each)
                    for info in z.infolist():
                        if info.is_dir():
                            continue
                        filename = info.filename.split("/")[-1]
                        if filename in manifest_filenames:
                            try:
                                content = z.read(info.filename)[:4096]
                                build_files[filename] = content.decode('utf-8', errors='ignore')
                            except Exception:
                                pass
                        elif filename.lower() == "requirements.txt":
                            try:
                                content = z.read(info.filename)[:4096]
                                requirements_txt = content.decode('utf-8', errors='ignore')
                            except Exception:
                                pass
            else:
                # Flat file upload
                src_bytes = minio_service.download_bytes(settings.BUCKET_JOB_INPUTS, profile.entry_file)

                # Check for manifest files in the flat upload
                for key in file_keys:
                    filename = key.split("/")[-1]
                    if filename in manifest_filenames:
                        try:
                            content = minio_service.download_bytes(settings.BUCKET_JOB_INPUTS, key)[:4096]
                            build_files[filename] = content.decode('utf-8', errors='ignore')
                        except Exception:
                            pass
                    elif filename.lower() == "requirements.txt":
                        try:
                            content = minio_service.download_bytes(settings.BUCKET_JOB_INPUTS, key)[:4096]
                            requirements_txt = content.decode('utf-8', errors='ignore')
                        except Exception:
                            pass

            src_str = src_bytes.decode('utf-8', errors='ignore')

            generator = DockerfileGenerator()
            gen_result = await generator.generate(
                src_str, requirements_txt, profile, build_files
            )

            # Handle wrapper script for compiled workloads
            if gen_result.needs_wrapper and gen_result.wrapper_script:
                wrapper_filename = "_campugrid_wrapper.py"

                if zip_key:
                    # Repack the zip with the wrapper script added
                    await send_customer_update(
                        job_id, "catalog",
                        f"Generated Python wrapper for compiled workload. {gen_result.reasoning}"
                    )

                    # Download original zip
                    original_zip_bytes = minio_service.download_bytes(settings.BUCKET_JOB_INPUTS, zip_key)

                    # Create new zip with original contents + wrapper
                    new_zip_buffer = io.BytesIO()
                    with zipfile.ZipFile(io.BytesIO(original_zip_bytes)) as original_z:
                        with zipfile.ZipFile(new_zip_buffer, "w", zipfile.ZIP_DEFLATED) as new_z:
                            # Copy all files from original
                            for info in original_z.infolist():
                                if not info.is_dir():
                                    new_z.writestr(info, original_z.read(info.filename))

                            # Add wrapper script at root
                            new_z.writestr(wrapper_filename, gen_result.wrapper_script)

                    # Upload repacked zip
                    new_zip_key = f"{job_id}/_campugrid_repacked.zip"
                    new_zip_buffer.seek(0)
                    minio_service.upload_bytes(
                        settings.BUCKET_JOB_INPUTS,
                        new_zip_key,
                        new_zip_buffer.read(),
                        "application/zip"
                    )

                    # Update profile to use the repacked zip and wrapper
                    profile.split_params["minio_key"] = new_zip_key
                    profile.entry_file = wrapper_filename

                    logger.info(f"Repacked zip with wrapper: {new_zip_key}")
                else:
                    # Flat file upload - upload wrapper directly
                    await send_customer_update(
                        job_id, "catalog",
                        f"Generated Python wrapper for compiled workload. {gen_result.reasoning}"
                    )

                    wrapper_key = f"{job_id}/{wrapper_filename}"
                    minio_service.upload_bytes(
                        settings.BUCKET_JOB_INPUTS,
                        wrapper_key,
                        gen_result.wrapper_script.encode('utf-8'),
                        "text/x-python"
                    )

                    # Update profile: minio_key points to wrapper, entry_file is bare filename
                    profile.split_params["minio_key"] = wrapper_key
                    profile.entry_file = wrapper_filename  # bare filename for entrypoint template

                    logger.info(f"Uploaded wrapper script: {wrapper_key}")

                await send_customer_update(
                    job_id, "catalog",
                    f"Wrapper configured for build+run (base {gen_result.base_image})."
                )
            else:
                # No wrapper needed - standard Python workload
                await send_customer_update(
                    job_id, "catalog",
                    f"Generated container config (base {gen_result.base_image})."
                )

            cat_entry = CatalogEntry(
                image=gen_result.base_image,
                entrypoint_template=GENERIC_PYTHON_ENTRYPOINT,
                env_vars=["INPUT", "OUTPUT_PATH", "CHUNK_START", "CHUNK_END", "JOB_ID"],
                gpu_required=profile.gpu_required,
                preinstalled_packages=[],
                tested=False,
                setup_commands=gen_result.setup_commands,
            )
        except Exception as e:
            logger.error(f"Gemini container generation failed for {job_id}: {e}. Pausing pipeline.")
            async with make_celery_session() as session:
                result = await session.execute(select(Job).where(Job.id == job_id))
                job = result.scalar_one_or_none()
                if job:
                    job.status = JobStatus.NEEDS_DOCKERFILE
                    await session.commit()
            await send_customer_update(job_id, "needs_dockerfile", f"Pipeline paused. Could not find catalog match and AI generation failed: {e}. Please provide a Dockerfile.")
            return


    else:
        # Tier 2: Check if unknown imports require adaptation
        # Fast-path: skip Gemini entirely if all imports are already covered by the catalog image.
        # This avoids async/Celery deadlocks from the Gemini aio client.
        pre_installed = {pkg.split("==")[0] for pkg in cat_entry.preinstalled_packages}
        LOCAL_MODULE_PATTERNS = {"models", "utils", "config", "data", "train", "src", "test", "helpers", "common"}
        STDLIB_SKIP = {"os", "sys", "json", "re", "math", "time", "datetime", "logging", "pathlib",
                       "argparse", "random", "copy", "abc", "io", "typing", "collections", "functools",
                       "itertools", "hashlib", "uuid", "traceback", "warnings", "inspect", "shutil",
                       "subprocess", "threading", "multiprocessing", "socket", "struct", "enum", "dataclasses"}
        
        user_imports = set(profile.imports or [])
        truly_missing = user_imports - pre_installed - STDLIB_SKIP - LOCAL_MODULE_PATTERNS
        
        if not truly_missing:
            # All imports are covered — skip Gemini entirely
            await send_customer_update(job_id, "catalog", f"Match fully verified: {cat_entry.image}")
            v_res = type("V", (), {"compatible": True, "needs_adaptation": False, "conflicts": None, "commands": None})()
        else:
            logger.info(f"Unknown imports for {job_id}: {truly_missing} — calling Gemini verifier")
            await send_customer_update(job_id, "catalog", f"Found Base Match {cat_entry.image}. Running AI Import Verifier...")
            verifier = DockerConfigVerifier()
            # Run in a thread to avoid Celery async_to_sync deadlock with the aio Gemini client
            import asyncio
            v_res = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: __import__("asgiref.sync", fromlist=["async_to_sync"]).async_to_sync(
                    verifier.verify_and_adapt
                )(cat_entry, list(truly_missing), None)
            )

        if v_res.compatible and v_res.needs_adaptation:
            # Install the extra deps at container start; image stays the real base.
            cat_entry.setup_commands = v_res.commands or ""
            cat_entry.tested = False
            await send_customer_update(job_id, "catalog", "Adapter configured extra dependencies for your code.")
        elif not v_res.compatible:
            async with make_celery_session() as session:
                result = await session.execute(select(Job).where(Job.id == job_id))
                job = result.scalar_one_or_none()
                if job:
                    job.status = JobStatus.NEEDS_DOCKERFILE
                    await session.commit()
                    
            error_msg = (
                f"Your code requested dependencies that clashed with our base containers: {v_res.conflicts}\n\n"
                "Please upload a custom 'Dockerfile' alongside your code to resolve this. To adhere to CampuGrid architecture:\n"
                "1. Choose a valid base image (e.g. nvidia/cuda:12.1-cudnn8-runtime-ubuntu22.04 or python:3.11-slim).\n"
                "2. Use 'RUN pip install ...' for your dependencies.\n"
                "3. Use 'WORKDIR /workspace'.\n"
                "4. You do NOT need an ENTRYPOINT or CMD; CampuGrid natively injects execution wrappers automatically."
            )
            await send_customer_update(job_id, "needs_dockerfile", error_msg)
            return
        else:
            await send_customer_update(job_id, "catalog", f"Match fully verified: {cat_entry.image}")

    # 4. Split Chunks
    await send_customer_update(job_id, "splitting", "Calculating optimal chunk parallelism boundaries...")

    # Query redis to see how many nodes we have active to help splitter
    r = aioredis.Redis.from_url(settings.REDIS_URL, decode_responses=True)
    redis_svc = RedisService(r)
    all_nodes = await redis_svc.get_active_nodes()
    
    available_nodes = 0
    for node in all_nodes:
        status = await redis_svc.get_node_status(node["node_id"])
        if status == "available":
            available_nodes += 1

    # Fetch job again to get requirements
    async with make_celery_session() as session:
        result = await session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one_or_none()
        requires_public_network = job.requires_public_network if job else False

    chunks_data = compute_chunks(profile, available_nodes, cat_entry, requires_public_network, str(job_id))

    await send_customer_update(job_id, "queued", f"Generated {len(chunks_data)} execution units for P2P dispatch.")

    # Write to database mapping job back to real schema
    async with make_celery_session() as session:
        result = await session.execute(select(Job).where(Job.id == job_id))
        job = result.scalar_one_or_none()
        if job:
            job.type = JobType(profile.type)
            job.status = JobStatus.QUEUED
            job.container_image = cat_entry.image
            job.profile = {
                "type": profile.type,
                "framework": profile.framework,
                "gpu": profile.gpu_required,
                "vram": profile.resources.vram_gb,
                "confidence": profile.confidence,
                "entry_file": profile.entry_file,
                "split_keys": profile.split_params,
                "resources": {
                    "cpu_cores": profile.resources.cpu_cores,
                    "ram_gb": profile.resources.ram_gb,
                    "vram_gb": profile.resources.vram_gb,
                }
            }
            job.total_chunks = len(chunks_data)

            # persist chunks
            chunk_ids: list[str] = []
            for ch in chunks_data:
                db_chunk = Chunk(
                    job_id=job.id,
                    chunk_index=ch.chunk_index,
                    status=ChunkStatus.PENDING,
                    spec={
                        "image": cat_entry.image,
                        "chunk_start": ch.chunk_start,
                        "chunk_end": ch.chunk_end,
                        "command": ch.command,
                        "env_vars": ch.env_vars,
                        "network_mode": ch.network_mode,
                        "requires_public_network": ch.requires_public_network,
                        "gpu_required": cat_entry.gpu_required,
                        "vram_gb": profile.resources.vram_gb,
                        "ram_gb": profile.resources.ram_gb
                    },
                    estimated_seconds=3600
                )
                session.add(db_chunk)
                await session.flush()
                chunk_ids.append(str(db_chunk.id))

            # Commit chunk rows BEFORE enqueueing/dispatching. Celery workers run
            # in a separate process; if we dispatch before commit, the worker can
            # look up the chunk, find nothing, and silently drop the dispatch.
            await session.commit()

            from app.scheduler.matcher import dispatch_chunk
            for cid in chunk_ids:
                await redis_svc.push_chunk(cid)
                dispatch_chunk.delay(cid)

        # Emit completion message to unblock the frontend and display the JobProfileCard
        import json
        import dataclasses
        completion_payload = json.dumps({
            "type": "pipeline_complete",
            "job_id": job_id,
            "profile": dataclasses.asdict(profile)
        })
        await r.publish("job_updates", completion_payload)

    await r.aclose()


@celery.task(name="pipeline.analyze_and_dispatch")
def analyze_and_dispatch(job_id: str, user_id: str):
    """Entry point from FastAPI triggering async workflow inside Celery worker."""
    async_to_sync(process_pipeline_async)(job_id, user_id)
