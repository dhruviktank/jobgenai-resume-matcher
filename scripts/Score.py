import logging
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from .utils.db import get_conn, put_conn
import time

class Score:
    def __init__(self, task_id: int):
        self.task_id = task_id

    def calculate_score(self):
        try:
            resume = self.get_resume()
            if "error" in resume:
                raise Exception(resume["error"])
            resume_keywords = resume.get("keywords", [])
            resume_string = " ".join(resume_keywords)

            jobs = self.get_jobs()
            if "error" in jobs:
                raise Exception(jobs["error"])

            for job in jobs:
                start_time = time.time()
                try:
                    job_keywords = job.get("keywords", [])
                    jd_string = " ".join(job_keywords)

                    tfidf_score = self.tfidf_job_in_resume_score(resume_string, jd_string)
                    similarity_score = round(tfidf_score * 100, 2)

                    self.save_score(job["id"], float(similarity_score))
                    elapsed = time.time() - start_time
                    logging.info(f"✅ Job ID {job['id']} scored {similarity_score} in {elapsed:.3f} seconds")
                except Exception as job_e:
                    logging.exception(f"❌ Error scoring job_id={job['id']}: {str(job_e)}")

        except Exception as e:
            logging.exception(f"❌ Error in calculate_score for task_id={self.task_id}: {str(e)}")

    def save_score(self, job_id, score):
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                UPDATE public."JobMatched"
                SET "similarityScore" = %s
                WHERE "taskRequestId" = %s AND "jobId" = %s
            """, (score, self.task_id, job_id))
            conn.commit()
            cur.close()
            return {"status": "Score saved"}
        except Exception as e:
            logging.exception(f"❌ Failed to update similarityScore for job_id={job_id}")
            return {"error": str(e)}
        finally:
            put_conn(conn)

    def get_resume(self):
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT r.id, r.keywords
                FROM public."TaskRequest" t
                JOIN public."Resume" r ON r.id = t."resumeId"
                WHERE t.id = %s
            """, (self.task_id,))
            row = cur.fetchone()
            cur.close()
            if row:
                return {"id": row[0], "keywords": row[1]}
            else:
                raise ValueError("No resume found for task_id: {}".format(self.task_id))
        except Exception as e:
            logging.exception("❌ Error fetching resume")
            return {"error": str(e)}
        finally:
            put_conn(conn)

    def get_jobs(self):
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT jd.id, jd.keywords
                FROM public."TaskRequest" t
                JOIN public."JobMatched" j ON j."taskRequestId" = t.id
                JOIN public."Job" jd ON j."jobId" = jd.id
                WHERE t.id = %s
            """, (self.task_id,))
            rows = cur.fetchall()
            cur.close()
            return [{"id": row[0], "keywords": row[1]} for row in rows]
        except Exception as e:
            logging.exception("❌ Error fetching jobs")
            return {"error": str(e)}
        finally:
            put_conn(conn)

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

            matched_score = 0.0
            for idx, job_val in job_dict.items():
                resume_val = resume_dict.get(idx, 0.0)
                matched_score += min(resume_val, job_val)
            total_possible = np.sum(job_vec)

            common_terms = set(resume_keywords.split()) & set(job_keywords.split())
            boost_factor = 1 + (len(common_terms) / 5)  # Soft boost for overlap

            score = (matched_score / total_possible) * boost_factor

            if score < 0.7:
                score = score + (0.7 - score) * 0.6
            if 0.6 < score < 0.75:
                score = score * 0.2 + score
            if 0.4 < score < 0.6:
                score = score * 0.15 + score
            if score < 0.4:
                score = score * 0.1 + score
            final_score = max(min(score, 1.0), 0.3)  # Clamp between 0.3 and 1.0
            return round(final_score, 4)
        except Exception as e:
            logging.exception("❌ Error calculating TF-IDF containment score")
            return 0.3  # Safe fallback minimum

    def tfidf_cosine_similarity(self, str1, str2):
        try:
            vectorizer = TfidfVectorizer(stop_words='english')
            tfidf_matrix = vectorizer.fit_transform([str1, str2])
            cosine_sim = cosine_similarity(tfidf_matrix[0:1], tfidf_matrix[1:2])
            return float(cosine_sim[0][0])
        except Exception as e:
            logging.exception("❌ Error calculating cosine similarity")
            return 0.0

    def update_status(self, status = "IN_PROGRESS"):
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                UPDATE public."TaskRequest"
                SET "matchStatus" = %s
                WHERE id = %s
            """, (status, self.task_id))
            conn.commit()
            cur.close()
            return {"status": "Match status updated"}
        except Exception as e:
            logging.exception("❌ Error updating matchStatus")
            return {"error": str(e)}
        finally:
            put_conn(conn)

    def is_valid_task(self) -> bool:
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT 1 FROM public."TaskRequest" WHERE id = %s
            """, (self.task_id,))
            exists = cur.fetchone() is not None
            cur.close()
            return exists
        except Exception as e:
            logging.exception("❌ Error validating task_id existence")
            return False
        finally:
            put_conn(conn)