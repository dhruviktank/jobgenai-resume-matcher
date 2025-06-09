import json
import logging
import time
import asyncio
from fastapi import FastAPI, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from scripts import JobDescriptionProcessor, ResumeProcessor, Score

DATABASE_URL = "postgresql+asyncpg://neondb_owner:npg_SfzAVOih23Xp@ep-fancy-sunset-a1xqv7sq-pooler.ap-southeast-1.aws.neon.tech/jobgenai"

engine = create_async_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800,
    pool_pre_ping=True,
)
async_session = async_sessionmaker(bind=engine, expire_on_commit=False)

logging.basicConfig(level=logging.INFO)
app = FastAPI()

async def get_db():
    async with async_session() as session:
        yield session

class JobMatchRequest(BaseModel):
    taskId: str

@app.get("/")
async def root():
    return {"status": "✅ Health Checkup: Success"}

@app.post("/webhook/job-match")
async def process(request: JobMatchRequest, db: AsyncSession = Depends(get_db)):
    task_id = request.taskId
    logging.info(f"📥 Received job match request for task_id={task_id}")
    asyncio.create_task(background_process(task_id))
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Task accepted for background processing.",
            "taskId": task_id
        })
    }


async def background_process(task_id: str):
    start_time = time.time()
    score = Score(task_id, async_session)

    try:
        logging.info(f"🚀 Starting background processing for task_id={task_id}")

        if not await score.is_valid_task():
            logging.warning(f"⚠️ Task ID {task_id} does not exist. Skipping processing.")
            await score.update_status("FAILED")
            return

        if not await process_resumes(task_id):
            logging.error(f"❌ Resume processing failed for task_id={task_id}")
            await score.update_status("FAILED")
            return

        if not await process_job_descriptions(task_id):
            logging.error(f"❌ Job description processing failed for task_id={task_id}")
            await score.update_status("FAILED")
            return

        if not await update_match_score(task_id):
            logging.error(f"❌ Score update failed for task_id={task_id}")
            await score.update_status("FAILED")
            return

        await score.update_status("SUCCESS")
        elapsed = time.time() - start_time
        logging.info(f"✅ Background processing completed in {elapsed:.2f} seconds for task_id={task_id}")

    except Exception:
        logging.exception(f"❌ Unhandled error during background processing for task_id={task_id}")
        await score.update_status("FAILED")


async def process_resumes(task_id):
    try:
        processor = ResumeProcessor(task_id, async_session)
        return await processor.process()
    except Exception:
        logging.exception(f"❌ Resume processing failed for task_id={task_id}")
        return False


async def process_job_descriptions(task_id):
    try:
        processor = JobDescriptionProcessor(task_id, async_session)
        return await processor.process()
    except Exception:
        logging.exception(f"❌ Job description processing failed for task_id={task_id}")
        return False


async def update_match_score(task_id):
    try:
        score = Score(task_id, async_session)
        await score.calculate_score()
        return True
    except Exception:
        logging.exception(f"❌ Match score update failed for task_id={task_id}")
        return False
