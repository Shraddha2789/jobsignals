from fastapi import APIRouter

from .companies import router as companies_router
from .insights import router as insights_router
from .jobs import router as jobs_router
from .salaries import router as salaries_router
from .skills import router as skills_router
from .stats import router as stats_router

v1_router = APIRouter(prefix="/v1")
v1_router.include_router(jobs_router)
v1_router.include_router(skills_router)
v1_router.include_router(companies_router)
v1_router.include_router(salaries_router)
v1_router.include_router(insights_router)
v1_router.include_router(stats_router)

__all__ = ["v1_router"]
