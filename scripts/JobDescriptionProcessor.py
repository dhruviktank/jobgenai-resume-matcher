import logging
from bs4 import BeautifulSoup
from .parsers import ParseJobDesc
from .utils.db import get_conn, put_conn

class JobDescriptionProcessor:
    def __init__(self, task_id: int):
        self.task_id = task_id

    def process(self) -> bool:
        try:
            job_data = self.get_current_task_jobs()
            if isinstance(job_data, dict) and "error" in job_data:
                raise Exception(job_data["error"])

            for job in job_data:
                raw_description = self.read_html_description(job["description"])
                parsed = ParseJobDesc(raw_description).get_JSON()

                if "extracted_keywords" not in parsed:
                    logging.warning(f"No keywords extracted for job_id={job['id']}")
                    continue

                success = self.save_jd_keywords(job['id'], parsed['extracted_keywords'])
                if success is not True:
                    logging.error(f"Failed to update keywords for job_id={job['id']}: {success}")

            return True
        except Exception as e:
            logging.exception(f"❌ Error in JobDescriptionProcessor.process for task_id={self.task_id}: {str(e)}")
            return False

    def save_jd_keywords(self, job_id, keywords):
        conn = get_conn()
        try:
            cur = conn.cursor()
            if not isinstance(keywords, list):
                raise ValueError("Keywords must be a list")

            cur.execute("""
                UPDATE public."Job"
                SET keywords = %s
                WHERE id = %s
            """, (keywords, job_id))
            conn.commit()
            cur.close()
            return True
        except Exception as e:
            logging.exception(f"❌ Error updating keywords in Job table for job_id={job_id}")
            return str(e)
        finally:
            put_conn(conn)

    def get_current_task_jobs(self):
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT jd.id, jd."htmlDescription"
                FROM public."TaskRequest" t
                JOIN public."JobMatched" j ON j."taskRequestId" = t.id
                JOIN public."Job" jd ON j."jobId" = jd.id
                WHERE t.id = %s
                AND jd.keywords IS NULL
            """, (self.task_id,))
            rows = cur.fetchall()
            cur.close()
            put_conn(conn)

            if not rows:
                logging.info(f"No jobs without keywords for task_id={self.task_id}")
                return []

            return [{"id": row[0], "description": row[1]} for row in rows]
        except Exception as e:
            logging.exception(f"❌ Error fetching jobs for task_id={self.task_id}")
            put_conn(conn)
            return {"error": str(e)}

    def read_html_description(self, html_content: str) -> str:
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            return soup.get_text(separator=" ", strip=True)
        except Exception as e:
            logging.exception("❌ Error parsing HTML content in job description")
            return ""
