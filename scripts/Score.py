import logging
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import update
from sqlalchemy.exc import NoResultFound, InterfaceError, OperationalError
from asyncio.exceptions import CancelledError
from sqlalchemy.orm import selectinload
from models import TaskRequest, Resume, JobMatched
import asyncio

class Score:
    def __init__(self, task_id: str, session_factory):
        self.task_id = task_id
        self.session_factory = session_factory

    async def calculate_score(self):
        async with self.session_factory() as session:
            try:
                resume = await self.get_resume(session)
                if not resume:
                    raise Exception("Resume not found")

                resume_keywords = resume.keywords or []
                resume_string = " ".join(resume_keywords)

                jobs = await self.get_jobs(session)
                if not jobs:
                    raise Exception("No jobs found")
                job_scores = []
                for job in jobs:
                    try:
                        job_keywords = job.keywords or []
                        jd_string = " ".join(job_keywords)

                        tfidf_score = self.tfidf_job_in_resume_score(resume_string, jd_string)
                        similarity_score = round(tfidf_score * 100, 2)

                        job_scores.append((job.id, float(similarity_score)))
                        # await self.save_score(session, job.id, float(similarity_score))
                    except Exception as job_e:
                        logging.exception(f"❌ Error scoring job_id={job.id}: {str(job_e)}")

                if job_scores:
                    await self.save_all_scores(session, job_scores)

            except Exception as e:
                logging.exception(f"❌ Error in calculate_score for task_id={self.task_id}: {str(e)}")

    async def save_all_scores(self, session: AsyncSession, job_scores: list[tuple[str, float]]):
        try:
            for job_id, score in job_scores:
                stmt = (
                    update(JobMatched)
                    .where(
                        JobMatched.taskRequestId == self.task_id,
                        JobMatched.jobId == job_id
                    )
                    .values(similarityScore=score)
                )
                await session.execute(stmt)

            await session.commit()
            logging.info(f"✅ Saved scores for {len(job_scores)} jobs in bulk")
        except Exception:
            await session.rollback()
            logging.exception("❌ Bulk update of similarity scores failed")

    async def save_score(self, session: AsyncSession, job_id: str, score: float):
        try:
            stmt = select(JobMatched).where(
                JobMatched.taskRequestId == self.task_id,
                JobMatched.jobId == job_id
            )
            result = await session.execute(stmt)
            job_matched = result.scalars().first()
            job_matched.similarityScore = score
        except NoResultFound:
            logging.error(f"JobMatched not found for taskRequestId={self.task_id} and jobId={job_id}")
        except Exception as e:
            logging.exception(f"❌ Failed to update similarityScore for job_id={job_id}")

    async def get_resume(self, session: AsyncSession):
        try:
            stmt = select(TaskRequest).where(TaskRequest.id == self.task_id)
            result = await session.execute(stmt)
            task_request = result.scalar_one()

            stmt = select(Resume).where(Resume.id == task_request.resumeId)
            result = await session.execute(stmt)
            return result.scalar_one()
        except NoResultFound:
            logging.error(f"Resume not found for task_id={self.task_id}")
            return None
        except Exception as e:
            logging.exception("❌ Error fetching resume")
            return None

    async def get_jobs(self, session: AsyncSession):
        try:
            stmt = select(JobMatched).where(JobMatched.taskRequestId == self.task_id).options(
                selectinload(JobMatched.job)
            )
            result = await session.execute(stmt)
            job_matched_list = result.scalars().all()
            jobs = [jm.job for jm in job_matched_list if jm.job]
            return jobs
        except Exception as e:
            logging.exception("❌ Error fetching jobs")
            return []

    def tfidf_job_in_resume_score(self, resume_keywords: str, job_keywords: str) -> float:
        try:
            vectorizer = TfidfVectorizer()
            tfidf_matrix = vectorizer.fit_transform([resume_keywords, job_keywords])

            # resume_vec = tfidf_matrix[0].toarray()[0]
            # job_vec = tfidf_matrix[1].toarray()[0]

            # if not np.any(job_vec):
            #     return 1.0

            # matched_score = np.sum(np.minimum(resume_vec, job_vec))

            resume_vec = tfidf_matrix[0]
            job_vec = tfidf_matrix[1]

            # If job_vec has no nonzero entries, return 1.0
            if job_vec.nnz == 0:
                return 1.0

            resume_dict = dict(zip(resume_vec.indices, resume_vec.data))
            job_dict = dict(zip(job_vec.indices, job_vec.data))

            matched_score = sum(min(resume_dict.get(idx, 0.0), job_val) for idx, job_val in job_dict.items())
            total_possible = np.sum(job_vec)

            common_terms = set(resume_keywords.split()) & set(job_keywords.split())
            boost_factor = 1 + (len(common_terms) / 5)  # Soft boost for overlap

            score = (matched_score / total_possible) * boost_factor

            if score < 0.7:
                score += (0.7 - score) * 0.6
            if 0.6 < score < 0.75:
                score = score * 0.2 + score
            if 0.4 < score < 0.6:
                score = score * 0.15 + score
            if score < 0.4:
                score = score * 0.1 + score
            return max(min(score, 1.0), 0.3)
        except Exception as e:
            logging.exception("❌ Error calculating TF-IDF containment score")
            return 0.3

    async def update_status(self, status="IN_PROGRESS"):
        async with self.session_factory() as session:
            try:
                stmt = select(TaskRequest).where(TaskRequest.id == self.task_id)
                result = await session.execute(stmt)
                task_request = result.scalar_one()
                task_request.matchStatus = status
                await session.commit()
                return {"status": "Match status updated"}
            except Exception as e:
                logging.exception("❌ Error updating matchStatus")
                return {"error": str(e)}

    async def is_valid_task(self) -> bool:
        max_retries = 3
        delay = 2

        for attempt in range(1, max_retries + 1):
            async with self.session_factory() as session:
                try:
                    stmt = select(TaskRequest).where(TaskRequest.id == self.task_id)
                    result = await session.execute(stmt)
                    return result.scalar_one_or_none() is not None
                except (InterfaceError, OperationalError, CancelledError) as db_error:
                    logging.warning(f"[Retry {attempt}] ❌ DB connection error: {db_error}")
                    if attempt == max_retries:
                        logging.error("Exceeded max retries. Task validation failed.")
                        return False
                    await asyncio.sleep(delay)
                except Exception as e:
                    logging.exception("❌ Error validating task_id existence")
                    return False
