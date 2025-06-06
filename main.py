import json
import logging
import time

from pydantic import BaseModel
from fastapi import FastAPI, BackgroundTasks
from scripts import JobDescriptionProcessor, ResumeProcessor, Score
from scripts.utils.db import init_pool, close_all
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_pool()
    yield
    close_all()

app = FastAPI(lifespan=lifespan)

logging.basicConfig(level=logging.INFO)

class JobMatchRequest(BaseModel):
    taskId: str

@app.get("/")
def root():
    try:
        return {"status": "✅ Health Checkup: Success"}
    except Exception as e:
        logging.exception("❌ Health check failed.")
        return {"error": str(e)}

@app.post("/webhook/job-match")
def process(request: JobMatchRequest, background_tasks: BackgroundTasks):
    task_id = request.taskId
    logging.info(f"📥 Received job match request for task_id={task_id}")
    background_tasks.add_task(background_process, task_id)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Task accepted for background processing.",
            "taskId": task_id
        })
    }

def background_process(task_id: str):
    start_time = time.time()
    score = Score(task_id)

    try:
        logging.info(f"🚀 Starting background processing for task_id={task_id}")

        if not score.is_valid_task():
            logging.warning(f"⚠️ Task ID {task_id} does not exist. Skipping processing.")
            return

        if not process_resumes(task_id):
            logging.error(f"❌ Resume processing failed for task_id={task_id}")
            score.update_status("FAILED")
            return

        if not process_job_descriptions(task_id):
            logging.error(f"❌ Job description processing failed for task_id={task_id}")
            score.update_status("FAILED")
            return

        if not update_match_score(task_id):
            logging.error(f"❌ Score update failed for task_id={task_id}")
            score.update_status("FAILED")
            return

        score.update_status("SUCCESS")
        elapsed = time.time() - start_time
        logging.info(f"✅ Background processing completed in {elapsed:.2f} seconds for task_id={task_id}")

    except Exception as e:
        logging.exception(f"❌ Unhandled error during background processing for task_id={task_id}")
        score.update_status("FAILED")

def process_resumes(task_id):
    try:
        processor = ResumeProcessor(task_id)
        return processor.process()
    except Exception as e:
        logging.exception(f"❌ Resume processing failed for task_id={task_id}")
        return False

def process_job_descriptions(task_id):
    try:
        processor = JobDescriptionProcessor(task_id)
        return processor.process()
    except Exception as e:
        logging.exception(f"❌ Job description processing failed for task_id={task_id}")
        return False

def update_match_score(task_id):
    try:
        score = Score(task_id)
        score.calculate_score()
        score.update_status("SUCCESS")
        return True
    except Exception as e:
        logging.exception(f"❌ Match score update failed for task_id={task_id}")
        return False
