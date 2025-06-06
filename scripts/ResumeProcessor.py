from .utils.db import get_conn, put_conn
from .parsers import ParseResume
import logging

class ResumeProcessor:
    def __init__(self, task_id):
        self.task_id = task_id
        self.raw_text = None
        self.resume_id = None

    def process(self) -> bool:
        try:
            success = self.get_resume_data()
            if not success:
                logging.error(f"❌ Failed to fetch resume data for task_id={self.task_id}")
                return False

            resume_dict = ParseResume(self.raw_text).get_JSON()
            if "extracted_keywords" not in resume_dict:
                logging.warning(f"⚠️ No extracted_keywords found in resume for task_id={self.task_id}")
                return False

            if self.save_resume_keywords(resume_dict["extracted_keywords"]):
                logging.info(f"✅ Resume keywords saved for resume_id={self.resume_id}")
                return True
            else:
                logging.error(f"❌ Failed to save resume keywords for resume_id={self.resume_id}")
                return False

        except Exception as e:
            logging.exception(f"❌ Unexpected error while processing task_id={self.task_id}: {str(e)}")
            return False

    def get_resume_data(self) -> bool:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT r.id, r."rawText"
                    FROM public."TaskRequest" t
                    JOIN public."Resume" r ON r.id = t."resumeId"
                    WHERE t.id = %s
                """, (self.task_id,))
                result = cur.fetchone()

                if not result:
                    logging.error(f"❌ No matching resume found for task_id={self.task_id}")
                    return False

                self.resume_id, self.raw_text = result
                return True

        except Exception as e:
            logging.exception(f"❌ Error fetching resume data for task_id={self.task_id}: {str(e)}")
            return False

        finally:
            put_conn(conn)

    def save_resume_keywords(self, keywords: list) -> bool:
        if not isinstance(keywords, list):
            logging.warning("⚠️ Keywords must be a list")
            return False

        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE public."Resume"
                    SET keywords = %s
                    WHERE id = %s
                """, (keywords, self.resume_id))
                conn.commit()
                return True

        except Exception as e:
            logging.exception(f"❌ Error updating keywords for resume_id={self.resume_id}: {str(e)}")
            return False

        finally:
            put_conn(conn)
