import logging
from sqlalchemy.future import select
from sqlalchemy.exc import NoResultFound
from .parsers import ParseResume
from models import TaskRequest, Resume

class ResumeProcessor:
    def __init__(self, task_id: int, session_factory):
        self.task_id = task_id
        self.session_factory = session_factory
        self.raw_text = None
        self.resume_id = None

    async def process(self) -> bool:
        try:
            success = await self.get_resume_data()
            if not success:
                logging.error(f"❌ Failed to fetch resume data for task_id={self.task_id}")
                return False

            resume_dict = ParseResume(self.raw_text).get_JSON()
            if "extracted_keywords" not in resume_dict:
                logging.warning(f"⚠️ No extracted_keywords found in resume for task_id={self.task_id}")
                return False

            saved = await self.save_resume_keywords(resume_dict["extracted_keywords"])
            if saved:
                logging.info(f"✅ Resume keywords saved for resume_id={self.resume_id}")
                return True
            else:
                logging.error(f"❌ Failed to save resume keywords for resume_id={self.resume_id}")
                return False

        except Exception as e:
            logging.exception(f"❌ Unexpected error while processing task_id={self.task_id}: {str(e)}")
            return False

    async def get_resume_data(self) -> bool:
        async with self.session_factory() as session:
            try:
                stmt = select(TaskRequest).where(TaskRequest.id == self.task_id)
                result = await session.execute(stmt)
                task = result.scalar_one()

                stmt = select(Resume).where(Resume.id == task.resumeId)
                result = await session.execute(stmt)
                resume = result.scalar_one()

                self.resume_id = resume.id
                self.raw_text = resume.rawText
                return True
            except NoResultFound:
                logging.error(f"❌ No matching resume found for task_id={self.task_id}")
                return False
            except Exception as e:
                logging.exception(f"❌ Error fetching resume data for task_id={self.task_id}: {str(e)}")
                return False

    async def save_resume_keywords(self, keywords: list) -> bool:
        if not isinstance(keywords, list):
            logging.warning("⚠️ Keywords must be a list")
            return False

        async with self.session_factory() as session:
            try:
                stmt = select(Resume).where(Resume.id == self.resume_id)
                result = await session.execute(stmt)
                resume = result.scalar_one()
                resume.keywords = keywords
                await session.commit()
                return True
            except NoResultFound:
                logging.error(f"Resume not found for resume_id={self.resume_id}")
                return False
            except Exception as e:
                logging.exception(f"❌ Error updating keywords for resume_id={self.resume_id}: {str(e)}")
                return False
