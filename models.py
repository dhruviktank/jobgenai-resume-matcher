from sqlalchemy import (
    Column, String, DateTime, ForeignKey, Enum, Text, Table, Index, JSON, Float
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import ARRAY
import enum

Base = declarative_base()


# Enums
class Platform(str, enum.Enum):
    LINKEDIN = "LINKEDIN"
    INDEED = "INDEED"
    NAUKRI = "NAUKRI"
    GLASSDOOR = "GLASSDOOR"
    APNA = "APNA"


class ResumeStatus(str, enum.Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class JobStatus(str, enum.Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class MatchStatus(str, enum.Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


# Models
class User(Base):
    __tablename__ = "User"

    id = Column(String, primary_key=True)
    createdAt = Column(DateTime, server_default=func.now())

    task_requests = relationship("TaskRequest", back_populates="user")
    resumes = relationship("Resume", back_populates="user")


class Resume(Base):
    __tablename__ = "Resume"

    id = Column(String, primary_key=True)
    userId = Column(String, ForeignKey("User.id"))
    jobRole = Column(String)
    jobLocation = Column(String)
    resumeUrl = Column(String, nullable=False)
    resumeJson = Column(JSON)
    rawText = Column(Text, nullable=False)
    keywords = Column(ARRAY(String))
    createdAt = Column(DateTime, server_default=func.now())

    user = relationship("User", back_populates="resumes")
    task_requests = relationship("TaskRequest", back_populates="resume")


class Job(Base):
    __tablename__ = "Job"

    id = Column(String, primary_key=True)
    title = Column(String, nullable=False)
    company = Column(String, nullable=False)
    location = Column(String, nullable=False)
    htmlDescription = Column(Text, nullable=False)
    link = Column(String, nullable=False)
    postedAt = Column(String, nullable=False)
    keywords = Column(ARRAY(String))
    platform = Column(Enum(Platform), nullable=False)
    createdAt = Column(DateTime, server_default=func.now())

    matched_jobs = relationship("JobMatched", back_populates="job")

    __table_args__ = (
        Index('ix_job_unique', "title", "company", "location", "platform", unique=True),
        Index('ix_job_id_title_company', "id", "title", "company"),
    )


class TaskRequest(Base):
    __tablename__ = "TaskRequest"

    id = Column(String, primary_key=True)
    userId = Column(String, ForeignKey("User.id"))
    resumeId = Column(String, ForeignKey("Resume.id"))
    resumeStatus = Column(Enum(ResumeStatus), default=ResumeStatus.PENDING)
    jobStatus = Column(Enum(JobStatus), default=JobStatus.PENDING)
    matchStatus = Column(Enum(MatchStatus, name="MatchStatus", create_type=False, native_enum=True),
        nullable=False,)
    createdAt = Column(DateTime, server_default=func.now())

    user = relationship("User", back_populates="task_requests")
    resume = relationship("Resume", back_populates="task_requests")
    job_matched = relationship("JobMatched", back_populates="task_request")


class JobMatched(Base):
    __tablename__ = "JobMatched"

    id = Column(String, primary_key=True)
    taskRequestId = Column(String, ForeignKey("TaskRequest.id"))
    jobId = Column(String, ForeignKey("Job.id"))
    similarityScore = Column(Float)

    task_request = relationship("TaskRequest", back_populates="job_matched")
    job = relationship("Job", back_populates="matched_jobs")


class CacheJob(Base):
    __tablename__ = "CacheJob"

    id = Column(String, primary_key=True)
    role = Column(String, nullable=False)
    location = Column(String, nullable=False)
    jobIds = Column(JSON)
    createdAt = Column(DateTime, server_default=func.now())
