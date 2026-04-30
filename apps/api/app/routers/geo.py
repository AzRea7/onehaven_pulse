from fastapi import APIRouter
from sqlalchemy import text

from app.db.session import engine

router = APIRouter(prefix="/geo", tags=["geo"])


@router.get("/summary")
def geo_summary():
    with engine.connect() as connection:
        result = connection.execute(
            text(
                """
                SELECT
                    geo_type,
                    COUNT(*) AS count
                FROM geo.dim_geo
                GROUP BY geo_type
                ORDER BY geo_type
                """
            )
        )

        rows = result.mappings().all()

    return {
        "items": [
            {
                "geo_type": row["geo_type"],
                "count": row["count"],
            }
            for row in rows
        ]
    }
