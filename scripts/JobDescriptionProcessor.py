import logging
from bs4 import BeautifulSoup
from .parsers import ParseJobDesc
from models import Job
from sqlalchemy.future import select
from sqlalchemy.exc import NoResultFound
from sqlalchemy import text

class JobDescriptionProcessor:
    def __init__(self, task_id: str, session_factory):
        self.task_id = task_id
        self.session_factory = session_factory

    async def process(self) -> bool:
        try:
            job_data = await self.get_current_task_jobs()
            if isinstance(job_data, dict) and "error" in job_data:
                raise Exception(job_data["error"])

            for job in job_data:
                raw_description = self.read_html_description(job.htmlDescription)
                parsed = ParseJobDesc(raw_description).get_JSON()

                if "extracted_keywords" not in parsed:
                    logging.warning(f"No keywords extracted for job_id={job.id}")
                    continue

                success = await self.save_jd_keywords(job.id, parsed['extracted_keywords'])
                if success is not True:
                    logging.error(f"Failed to update keywords for job_id={job.id}: {success}")

            return True
        except Exception as e:
            logging.exception(f"❌ Error in JobDescriptionProcessor.process for task_id={self.task_id}: {str(e)}")
            return False

    async def save_jd_keywords(self, job_id: str, keywords: list):
        if not isinstance(keywords, list):
            logging.warning("⚠️ Keywords must be a list")
            return "Keywords must be a list"

        async with self.session_factory() as session:
            try:
                stmt = select(Job).where(Job.id == job_id)
                result = await session.execute(stmt)
                job = result.scalar_one()

                job.keywords = keywords
                await session.commit()
                return True
            except NoResultFound:
                logging.error(f"Job not found for job_id={job_id}")
                return f"Job not found for job_id={job_id}"
            except Exception as e:
                logging.exception(f"❌ Error updating keywords in Job table for job_id={job_id}")
                return str(e)

    async def get_current_task_jobs(self):
        async with self.session_factory() as session:
            try:
                raw_sql = text("""
                    SELECT j.*
                    FROM "JobMatched" jm
                    JOIN "Job" j ON jm."jobId" = j."id"
                    WHERE jm."taskRequestId" = :task_id
                    AND j."keywords" IS NOT NULL
                    AND array_length(j."keywords", 1) = 0
                """)

                result = await session.execute(raw_sql, {"task_id": self.task_id})
                rows = result.fetchall()

                jobs_without_keywords = [Job(**dict(row)) for row in rows]

                if not jobs_without_keywords:
                    logging.info(f"No jobs without keywords for task_id={self.task_id}")

                return jobs_without_keywords
            except Exception as e:
                logging.exception(f"❌ Error fetching jobs for task_id={self.task_id}")
                return {"error": str(e)}

    def read_html_description(self, html_content: str) -> str:
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            return soup.get_text(separator=" ", strip=True)
        except Exception as e:
            logging.exception("❌ Error parsing HTML content in job description")
            return ""
