import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class JobSchema(BaseModel):
    id: uuid.UUID
    name: str
    status: str
    max_working_time: float
    start_at: datetime
    tries: int
    args: tuple
    kwargs: dict
    working_time: Optional[float]
    dependencies: Optional[list["JobSchema"]]
    results: Optional[list]


class JobListSchema(BaseModel):
    jobs: list[JobSchema]
