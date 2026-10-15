"""Employee Attendance & Analytics API.

Run from the repository root:
    uvicorn app.main:app --port 8000

Configuration:
    MONGO_URI  MongoDB connection URI (defaults to local MongoDB)
    MONGO_DB   Database name (defaults to attendance_db)

The API follows openapi.yaml and DATA_MODEL.md. In particular, calendar dates are
IST date strings, instants cross the API boundary as epoch milliseconds, and
analytics are computed by MongoDB aggregation pipelines.

Recommended walkthrough order (same numbering as the code sections below):
  1. Configuration, MongoDB collections, indexes, and startup
  2. Request models and input validation
  3. Time/date rules, rounding, and response serialization
  4. MongoDB aggregation pipelines for analytics
  5. Reusable query filters and explain helpers
  6. Health check and employee API routes
  7. Punch-in, punch-out, attendance list, and regularization routes
  8. Monthly, department, and late-leaderboard analytics routes
  9. Admin explain route (execution-stats query plans)
"""
from __future__ import annotations

import calendar
import json
import logging
import os
import re
import threading
from contextlib import asynccontextmanager
from datetime import date as Date
from datetime import datetime, time as Time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Annotated, Literal

from bson import json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, StrictInt, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

# =============================================================================
# 1. CONFIGURATION, MONGODB COLLECTIONS, INDEXES, AND STARTUP
# -----------------------------------------------------------------------------

# Load local settings before creating the MongoDB client. Existing shell variables
# take precedence over values in .env, which is useful for deployment.
load_dotenv()

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "attendance_db")

logger = logging.getLogger(__name__)

# tz_aware=True makes MongoDB BSON dates timezone-aware UTC datetimes in Python.
client = MongoClient(
    MONGO_URI,
    tz_aware=True,
    tzinfo=UTC,
    serverSelectionTimeoutMS=2500,
    connectTimeoutMS=2500,
    maxPoolSize=100,
)
db = client[MONGO_DB]
employees = db["employees"]
attendance_logs = db["attendance_logs"]

_indexes_lock = threading.Lock()
_indexes_ready = False


# Named indexes are also used by the explain endpoint so its plan is reproducible.
# Unique indexes enforce employee-code and employee/date uniqueness in MongoDB.
def ensure_indexes() -> None:
    """Create the indexes used by writes, filters, sorts, and analytics.

    create_index is idempotent when the same key pattern and options already exist.
    The unique indexes enforce business invariants under concurrent requests.
    """
    global _indexes_ready
    if _indexes_ready:
        return

    with _indexes_lock:
        if _indexes_ready:
            return

        employees.create_index(
            [("emp_code", ASCENDING)],
            unique=True,
            name="employees_emp_code_unique",
        )
        employees.create_index(
            [("joined_on", ASCENDING)],
            name="employees_joined_on",
        )
        employees.create_index(
            [("department", ASCENDING)],
            name="employees_department",
        )
        employees.create_index(
            [("department", ASCENDING), ("emp_code", ASCENDING)],
            name="employees_department_emp_code",
        )

        attendance_logs.create_index(
            [("emp_code", ASCENDING), ("date", ASCENDING)],
            unique=True,
            name="attendance_emp_date_unique",
        )
        attendance_logs.create_index(
            [("date", DESCENDING), ("emp_code", ASCENDING)],
            name="attendance_date_emp",
        )
        attendance_logs.create_index(
            [("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)],
            name="attendance_status_date_emp",
        )

        _indexes_ready = True
        logger.info("MongoDB indexes are ready")


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Create required indexes as the FastAPI app starts; close the client on exit."""
    # Keep the app importable and let /health return 503 if MongoDB is unavailable.
    try:
        ensure_indexes()
    except PyMongoError:
        logger.exception("Could not create MongoDB indexes during startup")
    yield
    client.close()


app = FastAPI(
    title="Employee Attendance & Analytics API",
    version="2.0.0",
    description="Attendance management and MongoDB-backed analytics.",
    lifespan=lifespan,
)


# =============================================================================
# 2. REQUEST MODELS AND INPUT VALIDATION
# -----------------------------------------------------------------------------

MIN_EPOCH_MS = 100_000_000_000
MAX_EPOCH_MS = 4_102_444_800_000
EpochMillis = Annotated[StrictInt, Field(ge=MIN_EPOCH_MS, le=MAX_EPOCH_MS)]

AttendanceStatus = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]
PRESENCE_STATUSES = ("PRESENT", "WFH", "ON_DUTY")
NON_WORKING_STATUSES = ("ABSENT", "LEAVE")
MONTH_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])$"
DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"
TIME_PATTERN = r"^([01]\d|2[0-3]):[0-5]\d$"


class APIModel(BaseModel):
    """Shared request-model settings: ignore extra keys and trim input strings."""
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)


class EmployeeCreate(APIModel):
    emp_code: str = Field(pattern=r"^EMP\d{4,6}$")
    name: str = Field(min_length=1, max_length=100)
    email: str = Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    department: str = Field(min_length=1, max_length=50)
    shift_start: str = Field(default="09:30", pattern=TIME_PATTERN)
    shift_end: str = Field(default="18:30", pattern=TIME_PATTERN)
    joined_on: str = Field(pattern=DATE_PATTERN)

    @model_validator(mode="after")
    def validate_employee_dates_and_shift(self) -> "EmployeeCreate":
        try:
            Date.fromisoformat(self.joined_on)
        except ValueError as exc:
            raise ValueError("joined_on must be a real YYYY-MM-DD date") from exc
        return self
        


class PunchInRequest(APIModel):
    emp_code: str = Field(min_length=1)
    punched_at: EpochMillis | None = None
    status: PresenceStatus = "PRESENT"

    @model_validator(mode="before")
    @classmethod
    def reject_null_punched_at(cls, values):
        if isinstance(values, dict) and "punched_at" in values and values["punched_at"] is None:
            raise ValueError("punched_at must be an epoch-millisecond integer when supplied")
        return values


class PunchOutRequest(APIModel):
    emp_code: str = Field(min_length=1)
    punched_at: EpochMillis | None = None

    @model_validator(mode="before")
    @classmethod
    def reject_null_punched_at(cls, values):
        if isinstance(values, dict) and "punched_at" in values and values["punched_at"] is None:
            raise ValueError("punched_at must be an epoch-millisecond integer when supplied")
        return values


class RegularizeRequest(APIModel):
    status: AttendanceStatus | None = None
    punch_in: EpochMillis | None = None
    punch_out: EpochMillis | None = None
    reason: str = Field(min_length=5, max_length=200)
    regularized_by: str = Field(min_length=1, max_length=50)

    @model_validator(mode="before")
    @classmethod
    def reject_explicit_nulls_for_optional_fields(cls, values):
        if isinstance(values, dict):
            for field in ("status", "punch_in", "punch_out"):
                if field in values and values[field] is None:
                    raise ValueError(f"{field} cannot be null when supplied")
        return values


# =============================================================================
# 3. TIME/DATE RULES, ROUNDING, AND RESPONSE SERIALIZATION
# -----------------------------------------------------------------------------


def _as_utc(value: datetime) -> datetime:
    """Normalize a database or application datetime to timezone-aware UTC."""
    if value.tzinfo is None:
        # PyMongo's default naive datetimes represent UTC. Treat any legacy naive
        # value the same way instead of interpreting it in the server's timezone.
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _truncate_seconds(value: datetime) -> datetime:
    return _as_utc(value).replace(microsecond=0)


def _epoch_ms_to_datetime(value: int) -> datetime:
    """Convert validated epoch milliseconds to UTC, truncating fractional seconds."""
    # Integer division avoids a floating-point timestamp conversion at the boundary.
    return datetime.fromtimestamp(value // 1000, tz=UTC)


def _datetime_to_epoch_ms(value: datetime | None) -> int | None:
    if value is None:
        return None
    value = _truncate_seconds(value)
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    return int((value - epoch).total_seconds()) * 1000


def _now_utc_seconds() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _parse_time(value: str) -> Time:
    return Time.fromisoformat(value)


def _scheduled_datetime(attendance_date: str, shift_time: str, *, next_day: bool = False) -> datetime:
    day = Date.fromisoformat(attendance_date)
    if next_day:
        day += timedelta(days=1)
    local_value = datetime.combine(day, _parse_time(shift_time), tzinfo=IST)
    return local_value.astimezone(UTC)


def get_attendance_date(punch_in: datetime, employee: dict) -> str:
    """Return the IST attendance date, including the overnight-shift exception."""
    local_punch_in = _truncate_seconds(punch_in).astimezone(IST)
    start_minutes = _minutes_after_midnight(employee["shift_start"])
    end_minutes = _minutes_after_midnight(employee["shift_end"])
    is_overnight = end_minutes <= start_minutes
    punch_minutes = local_punch_in.hour * 60 + local_punch_in.minute

    attendance_day = local_punch_in.date()
    if is_overnight and punch_minutes < end_minutes:
        attendance_day -= timedelta(days=1)
    return attendance_day.isoformat()


def _minutes_after_midnight(value: str) -> int:
    parsed = _parse_time(value)
    return parsed.hour * 60 + parsed.minute


def compute_late_minutes(punch_in: datetime, shift_start: str, attendance_date: str) -> int:
    """Apply R2: strictly more than ten minutes late; report whole elapsed minutes."""
    actual = _truncate_seconds(punch_in)
    scheduled_start = _scheduled_datetime(attendance_date, shift_start)
    elapsed_seconds = int((actual - scheduled_start).total_seconds())
    if elapsed_seconds > 10 * 60:
        return max(0, elapsed_seconds // 60)
    return 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """Apply R4 using decimal ROUND_HALF_UP rather than Python's banker's rounding."""
    start = _truncate_seconds(punch_in)
    end = _truncate_seconds(punch_out)
    elapsed_seconds = int((end - start).total_seconds())
    hours = (Decimal(elapsed_seconds) / Decimal(3600)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP
    )
    return float(hours)


def compute_overtime_minutes(
    punch_out: datetime, shift_start: str, shift_end: str, attendance_date: str
) -> int:
    """Apply R3: only count overtime when it reaches at least 30 whole minutes."""
    is_overnight = _minutes_after_midnight(shift_end) <= _minutes_after_midnight(shift_start)
    scheduled_end = _scheduled_datetime(attendance_date, shift_end, next_day=is_overnight)
    elapsed_seconds = int((_truncate_seconds(punch_out) - scheduled_end).total_seconds())
    if elapsed_seconds >= 30 * 60:
        return max(0, elapsed_seconds // 60)
    return 0


def _derived_values(
    employee: dict,
    attendance_date: str,
    punch_in: datetime | None,
    punch_out: datetime | None,
    status: str,
) -> dict:
    if status in NON_WORKING_STATUSES:
        return {
            "late_minutes": 0,
            "work_hours": None,
            "overtime_minutes": 0,
            "half_day": False,
        }

    late_minutes = compute_late_minutes(punch_in, employee["shift_start"], attendance_date) if punch_in else 0
    if punch_in is None or punch_out is None:
        return {
            "late_minutes": late_minutes,
            "work_hours": None,
            "overtime_minutes": 0,
            "half_day": False,
        }

    work_hours = compute_work_hours(punch_in, punch_out)
    return {
        "late_minutes": late_minutes,
        "work_hours": work_hours,
        "overtime_minutes": compute_overtime_minutes(
            punch_out, employee["shift_start"], employee["shift_end"], attendance_date
        ),
        "half_day": Decimal(str(work_hours)) < Decimal("4.50"),
    }


def _serialize_history(history: list[dict] | None) -> list[dict]:
    serialized = []
    for entry in history or []:
        changes = {}
        for field, change in (entry.get("changes") or {}).items():
            before = change.get("from")
            after = change.get("to")
            if field in ("punch_in", "punch_out"):
                before = _datetime_to_epoch_ms(before) if before is not None else None
                after = _datetime_to_epoch_ms(after) if after is not None else None
            changes[field] = {"from": before, "to": after}
        serialized.append(
            {
                "at": _datetime_to_epoch_ms(entry["at"]),
                "by": entry["by"],
                "reason": entry["reason"],
                "changes": changes,
            }
        )
    return serialized


def serialize_attendance(document: dict) -> dict:
    """Return only contract fields; MongoDB's internal _id is never exposed."""
    return {
        "emp_code": document["emp_code"],
        "date": document["date"],
        "status": document["status"],
        "punch_in": _datetime_to_epoch_ms(document.get("punch_in")),
        "punch_out": _datetime_to_epoch_ms(document.get("punch_out")),
        "work_hours": document.get("work_hours"),
        "late_minutes": int(document.get("late_minutes", 0) or 0),
        "overtime_minutes": int(document.get("overtime_minutes", 0) or 0),
        "half_day": bool(document.get("half_day", False)),
        "history": _serialize_history(document.get("history", [])),
    }


def serialize_employee(document: dict) -> dict:
    return {
        "emp_code": document["emp_code"],
        "name": document["name"],
        "email": document["email"],
        "department": document["department"],
        "shift_start": document["shift_start"],
        "shift_end": document["shift_end"],
        "joined_on": document["joined_on"],
        "created_at": _datetime_to_epoch_ms(document["created_at"]),
    }


# =============================================================================
# 4. MONGODB AGGREGATION PIPELINES FOR ANALYTICS
# -----------------------------------------------------------------------------

# The functions in this section build MongoDB aggregation stages. They do not
# execute the query themselves; the API routes later pass these pipelines to
# collection.aggregate(...).


def _month_bounds(month: str) -> tuple[str, str]:
    """Return inclusive first and last ISO calendar dates for YYYY-MM."""
    try:
        year_text, month_text = month.split("-")
        year, month_number = int(year_text), int(month_text)
        last_day = calendar.monthrange(year, month_number)[1]
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail="month must be YYYY-MM") from exc
    return f"{year:04d}-{month_number:02d}-01", f"{year:04d}-{month_number:02d}-{last_day:02d}"


def _rounded_ratio_units_half_up_expression(
    numerator_expression, denominator_expression, places: int, multiplier: int = 1
) -> dict:
    """Return the rounded integer units for a non-negative ratio using HALF_UP.

    For example, with places=4 the result is the rounded ratio multiplied by 10,000.
    Using integer arithmetic around the tie avoids MongoDB $round's ties-to-even rule.
    The expressions used here are counts or half-counts, so scaling produces integers.
    """
    scale = 10**places
    scaled_numerator = {
        "$multiply": [numerator_expression, multiplier * scale]
    }
    return {
        "$floor": {
            "$divide": [
                {"$add": [{"$multiply": [scaled_numerator, 2]}, denominator_expression]},
                {"$multiply": [denominator_expression, 2]},
            ]
        }
    }


def _half_up_expression(value_expression, places: int) -> dict:
    """Round a non-negative value HALF_UP; used for exact half-day increments."""
    scale = 10**places
    return {
        "$divide": [
            {"$floor": {"$add": [{"$multiply": [value_expression, scale]}, 0.5]}},
            scale,
        ]
    }


def _day_of_week_expression(date_string_expression) -> dict:
    # The stored date is a calendar string, so interpret it at UTC midnight only
    # to get its calendar weekday. The weekday is independent of the time zone.
    return {"$dayOfWeek": {"$dateFromString": {"dateString": date_string_expression}}}


def _sum_over_logs(expression) -> dict:
    return {
        "$sum": {
            "$map": {
                "input": {"$ifNull": ["$logs", []]},
                "as": "log",
                "in": expression,
            }
        }
    }


def _present_day_contribution() -> dict:
    return {
        "$let": {
            "vars": {"weekday": _day_of_week_expression("$$log.date")},
            "in": {
                "$cond": [
                    {
                        "$and": [
                            {"$in": ["$$log.status", list(PRESENCE_STATUSES)]},
                            {"$gte": ["$$weekday", 2]},
                            {"$lte": ["$$weekday", 6]},
                        ]
                    },
                    {"$cond": [{"$ifNull": ["$$log.half_day", False]}, 0.5, 1]},
                    0,
                ]
            },
        }
    }


def _working_days_expression(month_start: str, month_end: str) -> dict:
    """Count Mon-Fri from max(joined_on, month start) through month end in MongoDB."""
    start_expr = {
        "$max": [
            {"$dateFromString": {"dateString": "$joined_on"}},
            {"$dateFromString": {"dateString": month_start}},
        ]
    }
    end_expr = {"$dateFromString": {"dateString": month_end}}
    return {
        "$let": {
            "vars": {"effective_start": start_expr, "period_end": end_expr},
            "in": {
                "$cond": [
                    {"$gt": ["$$effective_start", "$$period_end"]},
                    0,
                    {
                        "$sum": {
                            "$map": {
                                "input": {
                                    "$range": [
                                        0,
                                        {
                                            "$add": [
                                                {
                                                    "$dateDiff": {
                                                        "startDate": "$$effective_start",
                                                        "endDate": "$$period_end",
                                                        "unit": "day",
                                                    }
                                                },
                                                1,
                                            ]
                                        },
                                    ]
                                },
                                "as": "offset",
                                "in": {
                                    "$let": {
                                        "vars": {
                                            "weekday": {
                                                "$dayOfWeek": {
                                                    "$dateAdd": {
                                                        "startDate": "$$effective_start",
                                                        "unit": "day",
                                                        "amount": "$$offset",
                                                    }
                                                }
                                            }
                                        },
                                        "in": {
                                            "$cond": [
                                                {
                                                    "$and": [
                                                        {"$gte": ["$$weekday", 2]},
                                                        {"$lte": ["$$weekday", 6]},
                                                    ]
                                                },
                                                1,
                                                0,
                                            ]
                                        },
                                    }
                                },
                            }
                        }
                    },
                ]
            },
        }
    }


def _employee_monthly_pipeline(emp_code: str, month: str) -> list[dict]:
    """Build the monthly employee report as a MongoDB aggregation, not Python loops."""
    month_start, month_end = _month_bounds(month)
    return [
        {"$match": {"emp_code": emp_code}},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": month_start, "$lte": month_end}}}
                ],
                "as": "logs",
            }
        },
        {
            "$set": {
                "month": {"$literal": month},
                "working_days": _working_days_expression(month_start, month_end),
                "present_days": _sum_over_logs(_present_day_contribution()),
                "leave_days": _sum_over_logs({"$cond": [{"$eq": ["$$log.status", "LEAVE"]}, 1, 0]}),
                "late_count": _sum_over_logs(
                    {"$cond": [{"$gt": [{"$ifNull": ["$$log.late_minutes", 0]}, 0]}, 1, 0]}
                ),
                "total_late_minutes": _sum_over_logs({"$ifNull": ["$$log.late_minutes", 0]}),
                "total_overtime_minutes": _sum_over_logs(
                    {"$ifNull": ["$$log.overtime_minutes", 0]}
                ),
            }
        },
        {
            "$set": {
                "present_days": _half_up_expression("$present_days", 2),
                "attendance_pct": {
                    "$cond": [
                        {"$gt": ["$working_days", 0]},
                        {
                            "$divide": [
                                _rounded_ratio_units_half_up_expression(
                                    "$present_days", "$working_days", 4, multiplier=100
                                ),
                                10000,
                            ]
                        },
                        None,
                    ]
                },
            }
        },
        {
            "$project": {
                "_id": 0,
                "emp_code": 1,
                "month": 1,
                "working_days": 1,
                "present_days": 1,
                "leave_days": 1,
                "late_count": 1,
                "total_late_minutes": 1,
                "total_overtime_minutes": 1,
                "attendance_pct": 1,
            }
        },
    ]


def _department_summary_pipeline(month: str, department: str | None = None) -> list[dict]:
    """Aggregate department headcount and attendance metrics for one month."""
    month_start, month_end = _month_bounds(month)
    employee_match: dict = {"joined_on": {"$lte": month_end}}
    if department is not None:
        employee_match["department"] = department

    return [
        {"$match": employee_match},
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "emp_code",
                "foreignField": "emp_code",
                "pipeline": [
                    {"$match": {"date": {"$gte": month_start, "$lte": month_end}}}
                ],
                "as": "logs",
            }
        },
        {
            "$set": {
                "_present_days": _sum_over_logs(_present_day_contribution()),
                "_late_count": _sum_over_logs(
                    {"$cond": [{"$gt": [{"$ifNull": ["$$log.late_minutes", 0]}, 0]}, 1, 0]}
                ),
                "_total_late_minutes": _sum_over_logs({"$ifNull": ["$$log.late_minutes", 0]}),
                "_leave_count": _sum_over_logs(
                    {"$cond": [{"$eq": ["$$log.status", "LEAVE"]}, 1, 0]}
                ),
                "_on_duty_count": _sum_over_logs(
                    {"$cond": [{"$eq": ["$$log.status", "ON_DUTY"]}, 1, 0]}
                ),
                "_work_hours_cents_sum": _sum_over_logs(
                    {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$$log.status", list(PRESENCE_STATUSES)]},
                                    {"$ne": [{"$ifNull": ["$$log.work_hours", None]}, None]},
                                ]
                            },
                            {"$round": [{"$multiply": ["$$log.work_hours", 100]}, 0]},
                            0,
                        ]
                    }
                ),
                "_work_hours_count": _sum_over_logs(
                    {
                        "$cond": [
                            {
                                "$and": [
                                    {"$in": ["$$log.status", list(PRESENCE_STATUSES)]},
                                    {"$ne": [{"$ifNull": ["$$log.work_hours", None]}, None]},
                                ]
                            },
                            1,
                            0,
                        ]
                    }
                ),
            }
        },
        {
            "$group": {
                "_id": "$department",
                "headcount": {"$sum": 1},
                "present_days": {"$sum": "$_present_days"},
                "late_count": {"$sum": "$_late_count"},
                "total_late_minutes": {"$sum": "$_total_late_minutes"},
                "leave_count": {"$sum": "$_leave_count"},
                "on_duty_count": {"$sum": "$_on_duty_count"},
                "work_hours_cents_sum": {"$sum": "$_work_hours_cents_sum"},
                "work_hours_count": {"$sum": "$_work_hours_count"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "department": "$_id",
                "headcount": 1,
                "present_days": _half_up_expression("$present_days", 2),
                "avg_work_hours": {
                    "$cond": [
                        {"$gt": ["$work_hours_count", 0]},
                        {
                            "$divide": [
                                {
                                    "$floor": {
                                        "$divide": [
                                            {
                                                "$add": [
                                                    {"$multiply": ["$work_hours_cents_sum", 2]},
                                                    "$work_hours_count",
                                                ]
                                            },
                                            {"$multiply": ["$work_hours_count", 2]},
                                        ]
                                    }
                                },
                                100,
                            ]
                        },
                        None,
                    ]
                },
                "late_count": 1,
                "total_late_minutes": 1,
                "leave_count": 1,
                "on_duty_count": 1,
            }
        },
        {"$sort": {"department": 1}},
    ]


def _late_leaderboard_pipeline(
    month: str, limit: int = 10, department: str | None = None
) -> list[dict]:
    """Rank employees by late minutes; preserve ties at the requested rank cutoff."""
    month_start, month_end = _month_bounds(month)
    pipeline: list[dict] = [
        {
            "$match": {
                "date": {"$gte": month_start, "$lte": month_end},
                "late_minutes": {"$gt": 0},
            }
        },
        {
            "$group": {
                "_id": "$emp_code",
                "total_late_minutes": {"$sum": "$late_minutes"},
                "late_count": {"$sum": 1},
            }
        },
        {
            "$lookup": {
                "from": "employees",
                "localField": "_id",
                "foreignField": "emp_code",
                "as": "employee",
            }
        },
        {"$unwind": "$employee"},  # Orphan attendance logs are ignored by contract.
    ]
    if department is not None:
        pipeline.append({"$match": {"employee.department": department}})
    pipeline.extend(
        [
            {
                "$set": {
                    "emp_code": "$_id",
                    "name": "$employee.name",
                    "department": "$employee.department",
                }
            },
            {
                "$setWindowFields": {
                    "sortBy": {"total_late_minutes": -1},
                    "output": {"rank": {"$rank": {}}},
                }
            },
            # Limit applies to rank, not array position; ties at the cutoff remain.
            {"$match": {"rank": {"$lte": limit}}},
            {"$sort": {"total_late_minutes": -1, "emp_code": 1}},
            {
                "$project": {
                    "_id": 0,
                    "rank": 1,
                    "emp_code": 1,
                    "name": 1,
                    "department": 1,
                    "total_late_minutes": 1,
                    "late_count": 1,
                }
            },
        ]
    )
    return pipeline


def _department_trend_pipeline(department: str, start: Date, end: Date) -> list[dict]:
    """Create one row per calendar date and calculate the seven-day moving average."""
    days_in_range = (end - start).days + 1
    start_iso = start.isoformat()
    date_from_string = {"$dateFromString": {"dateString": "$date"}}

    return [
        {"$match": {"department": department}},
        {
            "$group": {
                "_id": "$department",
                "employees": {
                    "$push": {"emp_code": "$emp_code", "joined_on": "$joined_on"}
                },
            }
        },
        {
            "$set": {
                "calendar_days": {
                    "$map": {
                        "input": {"$range": [0, days_in_range]},
                        "as": "offset",
                        "in": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": {
                                    "$dateAdd": {
                                        "startDate": {"$dateFromString": {"dateString": start_iso}},
                                        "unit": "day",
                                        "amount": "$$offset",
                                    }
                                },
                            }
                        },
                    }
                }
            }
        },
        {"$unwind": "$calendar_days"},
        {"$set": {"date": "$calendar_days"}},
        {
            "$set": {
                "date_obj": date_from_string,
                "is_working_day": {
                    "$let": {
                        "vars": {"weekday": {"$dayOfWeek": date_from_string}},
                        "in": {
                            "$and": [
                                {"$gte": ["$$weekday", 2]},
                                {"$lte": ["$$weekday", 6]},
                            ]
                        },
                    }
                },
                "active_employees": {
                    "$filter": {
                        "input": "$employees",
                        "as": "employee",
                        "cond": {"$lte": ["$$employee.joined_on", "$date"]},
                    }
                },
            }
        },
        {
            "$set": {
                "headcount": {"$size": "$active_employees"},
                "active_employee_codes": {
                    "$map": {
                        "input": "$active_employees",
                        "as": "employee",
                        "in": "$$employee.emp_code",
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 1,
                "date": 1,
                "date_obj": 1,
                "is_working_day": 1,
                "headcount": 1,
                "active_employee_codes": 1,
            }
        },
        {
            "$lookup": {
                "from": "attendance_logs",
                "localField": "active_employee_codes",
                "foreignField": "emp_code",
                "let": {"selected_day": "$date"},
                "pipeline": [
                    {"$match": {"$expr": {"$eq": ["$date", "$$selected_day"]}}}
                ],
                "as": "logs",
            }
        },
        {
            "$set": {
                "present_count": _sum_over_logs(
                    {
                        "$cond": [
                            {"$in": ["$$log.status", list(PRESENCE_STATUSES)]},
                            {"$cond": [{"$ifNull": ["$$log.half_day", False]}, 0.5, 1]},
                            0,
                        ]
                    }
                ),
                "late_count": _sum_over_logs(
                    {"$cond": [{"$gt": [{"$ifNull": ["$$log.late_minutes", 0]}, 0]}, 1, 0]}
                ),
            }
        },
        {
            "$set": {
                "attendance_rate_units": {
                    "$cond": [
                        {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
                        _rounded_ratio_units_half_up_expression(
                            "$present_count", "$headcount", 4
                        ),
                        None,
                    ]
                }
            }
        },
        {
            "$set": {
                "attendance_rate": {
                    "$cond": [
                        {"$eq": [{"$ifNull": ["$attendance_rate_units", None]}, None]},
                        None,
                        {"$divide": ["$attendance_rate_units", 10000]},
                    ]
                }
            }
        },
        {
            "$setWindowFields": {
                "sortBy": {"date": 1},
                "output": {
                    "_moving_avg_units_raw": {
                        "$avg": "$attendance_rate_units",
                        "window": {"documents": [-6, "current"]},
                    }
                },
            }
        },
        {
            "$set": {
                "moving_avg_7d": {
                    "$cond": [
                        {"$eq": [{"$ifNull": ["$_moving_avg_units_raw", None]}, None]},
                        None,
                        {
                            "$divide": [
                                {"$floor": {"$add": ["$_moving_avg_units_raw", 0.5]}},
                                10000,
                            ]
                        },
                    ]
                }
            }
        },
        {"$sort": {"date": 1}},
        {
            "$project": {
                "_id": 0,
                "date": 1,
                "is_working_day": 1,
                "headcount": 1,
                "present_count": 1,
                "late_count": 1,
                "attendance_rate": 1,
                "moving_avg_7d": 1,
            }
        },
    ]


# =============================================================================
# 5. REUSABLE QUERY FILTERS AND EXPLAIN HELPERS
# -----------------------------------------------------------------------------


def _attendance_filter(
    emp_code: str | None,
    date_from: Date | None,
    date_to: Date | None,
    status: str | None,
) -> dict:
    if date_from is not None and date_to is not None and date_from > date_to:
        raise HTTPException(status_code=422, detail="date_from must be on or before date_to")
    query: dict = {}
    if emp_code:
        query["emp_code"] = emp_code
    if date_from is not None or date_to is not None:
        query["date"] = {}
        if date_from is not None:
            query["date"]["$gte"] = date_from.isoformat()
        if date_to is not None:
            query["date"]["$lte"] = date_to.isoformat()
    if status:
        query["status"] = status
    return query


def _attendance_query_hint(emp_code: str | None, status: str | None) -> str:
    if emp_code:
        return "attendance_emp_date_unique"
    if status:
        return "attendance_status_date_emp"
    return "attendance_date_emp"


def _explain_aggregate(collection_name: str, pipeline: list[dict], hint: str) -> dict:
    command_document = {
        "aggregate": collection_name,
        "pipeline": pipeline,
        "cursor": {},
        "hint": hint,
    }
    return db.command({"explain": command_document, "verbosity": "executionStats"})


def _explain_find(
    collection_name: str,
    query: dict,
    sort: dict,
    skip: int,
    limit: int,
    hint: str,
) -> dict:
    command_document = {
        "find": collection_name,
        "filter": query,
        "sort": sort,
        "skip": skip,
        "limit": limit,
        "hint": hint,
    }
    return db.command({"explain": command_document, "verbosity": "executionStats"})


def _json_safe(value: dict) -> dict:
    """Keep raw explain details while making BSON-only types JSON serializable."""
    return json.loads(json_util.dumps(value))


def _required_explain_month(month: str | None) -> str:
    if not month:
        raise HTTPException(status_code=422, detail="month is required for this endpoint")
    # FastAPI query pattern is optional on this multi-purpose explain endpoint.
    if not re.fullmatch(MONTH_PATTERN, month):
        raise HTTPException(status_code=422, detail="month must be YYYY-MM")
    return month


# =============================================================================
# 6. HEALTH CHECK AND EMPLOYEE API ROUTES
# -----------------------------------------------------------------------------


@app.get("/health", tags=["system"])
def health() -> dict:
    """Return healthy only if MongoDB answers and the expected indexes are ready."""
    try:
        db.command("ping")
        ensure_indexes()
    except PyMongoError as exc:
        raise HTTPException(status_code=503, detail="MongoDB is unavailable") from exc
    return {"status": "ok"}


@app.post("/employees", status_code=201, tags=["employees"])
def create_employee(body: EmployeeCreate) -> dict:
    document = body.model_dump()
    document["created_at"] = _now_utc_seconds()
    try:
        employees.insert_one(document)
    except DuplicateKeyError as exc:
        # The unique database index, not a racy pre-check, enforces uniqueness.
        raise HTTPException(status_code=409, detail="emp_code already exists") from exc
    return serialize_employee(document)


@app.get("/employees", tags=["employees"])
def list_employees(
    department: str | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> dict:
    query = {"department": department} if department is not None else {}
    total = employees.count_documents(query)
    cursor = employees.find(query).sort("emp_code", ASCENDING).skip((page - 1) * page_size).limit(page_size)
    return {
        "items": [serialize_employee(document) for document in cursor],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


# =============================================================================
# 7. PUNCH-IN, PUNCH-OUT, ATTENDANCE LIST, AND REGULARIZATION ROUTES
# -----------------------------------------------------------------------------


@app.post("/attendance/punch-in", status_code=201, tags=["attendance"])
def punch_in(body: PunchInRequest) -> dict:
    employee = employees.find_one({"emp_code": body.emp_code})
    if employee is None:
        raise HTTPException(status_code=404, detail="employee not found")

    punched_at = _truncate_seconds(
        _epoch_ms_to_datetime(body.punched_at) if body.punched_at is not None else _now_utc_seconds()
    )
    attendance_date = get_attendance_date(punched_at, employee)
    derived = _derived_values(employee, attendance_date, punched_at, None, body.status)
    document = {
        "emp_code": body.emp_code,
        "date": attendance_date,
        "status": body.status,
        "punch_in": punched_at,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": derived["late_minutes"],
        "overtime_minutes": 0,
        "half_day": False,
        "history": [],
    }

    try:
        attendance_logs.insert_one(document)
    except DuplicateKeyError as exc:
        # Atomic uniqueness guarantee: only one concurrent request can insert this key.
        raise HTTPException(status_code=409, detail="already punched in for this date") from exc
    return serialize_attendance(document)


@app.post("/attendance/punch-out", tags=["attendance"])
def punch_out(body: PunchOutRequest) -> dict:
    employee = employees.find_one({"emp_code": body.emp_code})
    if employee is None:
        raise HTTPException(status_code=404, detail="employee not found")

    punched_at = _truncate_seconds(
        _epoch_ms_to_datetime(body.punched_at) if body.punched_at is not None else _now_utc_seconds()
    )
    current = attendance_logs.find_one(
        {"emp_code": body.emp_code, "punch_in": {"$type": "date", "$lte": punched_at}},
        sort=[("punch_in", DESCENDING)],
    )
    if current is None:
        raise HTTPException(status_code=404, detail="no punch-in found at or before punched_at")

    punch_in_value = _truncate_seconds(current["punch_in"])
    elapsed_seconds = int((punched_at - punch_in_value).total_seconds())
    if elapsed_seconds <= 0 or elapsed_seconds > 24 * 60 * 60:
        raise HTTPException(status_code=422, detail="punched_at must be after punch_in and within 24 hours")
    if current.get("punch_out") is not None:
        raise HTTPException(status_code=409, detail="record is already punched out")

    derived = _derived_values(
        employee,
        current["date"],
        punch_in_value,
        punched_at,
        current.get("status", "PRESENT"),
    )
    filter_for_atomic_close = {
        "_id": current["_id"],
        "status": current.get("status", "PRESENT"),
        "punch_in": current["punch_in"],
        "$or": [{"punch_out": None}, {"punch_out": {"$exists": False}}],
    }
    if "history" in current:
        filter_for_atomic_close["history"] = current["history"]
    else:
        filter_for_atomic_close["history"] = {"$exists": False}
    updated = attendance_logs.find_one_and_update(
        filter_for_atomic_close,
        {
            "$set": {
                "punch_out": punched_at,
                "work_hours": derived["work_hours"],
                "overtime_minutes": derived["overtime_minutes"],
                "half_day": derived["half_day"],
                "late_minutes": derived["late_minutes"],
            }
        },
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        # Another simultaneous punch-out won the conditional update.
        raise HTTPException(status_code=409, detail="record is already punched out")
    return serialize_attendance(updated)


@app.get("/attendance", tags=["attendance"])
def list_attendance(
    emp_code: str | None = None,
    date_from: Date | None = None,
    date_to: Date | None = None,
    status: Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"] | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> dict:
    query = _attendance_filter(emp_code, date_from, date_to, status)
    total = attendance_logs.count_documents(query)
    hint = _attendance_query_hint(emp_code, status)
    cursor = (
        attendance_logs.find(query)
        .sort([("date", DESCENDING), ("emp_code", ASCENDING)])
        .skip((page - 1) * page_size)
        .limit(page_size)
        .hint(hint)
    )
    return {
        "items": [serialize_attendance(document) for document in cursor],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@app.patch("/attendance/{emp_code}/{date}", tags=["attendance"])
def regularize_attendance(emp_code: str, date: Date, body: RegularizeRequest) -> dict:
    """Correct attendance fields and append an audit-history entry atomically."""
    attendance_date = date.isoformat()
    employee = employees.find_one({"emp_code": emp_code})
    if employee is None:
        raise HTTPException(status_code=404, detail="employee not found")

    current = attendance_logs.find_one({"emp_code": emp_code, "date": attendance_date})
    if current is None:
        raise HTTPException(status_code=404, detail="attendance record not found")

    fields_set = body.model_fields_set
    final_status = body.status if "status" in fields_set else current["status"]

    if final_status in NON_WORKING_STATUSES:
        if "punch_in" in fields_set or "punch_out" in fields_set:
            raise HTTPException(
                status_code=422,
                detail="punch_in and punch_out cannot be supplied with ABSENT or LEAVE",
            )
        final_punch_in = None
        final_punch_out = None
    else:
        final_punch_in = (
            _epoch_ms_to_datetime(body.punch_in)
            if "punch_in" in fields_set
            else current.get("punch_in")
        )
        final_punch_out = (
            _epoch_ms_to_datetime(body.punch_out)
            if "punch_out" in fields_set
            else current.get("punch_out")
        )
        if final_punch_in is None:
            raise HTTPException(status_code=422, detail="a presence status requires punch_in")
        final_punch_in = _truncate_seconds(final_punch_in)
        if get_attendance_date(final_punch_in, employee) != attendance_date:
            raise HTTPException(status_code=422, detail="punch_in must belong to the record attendance date")
        if final_punch_out is not None:
            final_punch_out = _truncate_seconds(final_punch_out)
            elapsed_seconds = int((final_punch_out - final_punch_in).total_seconds())
            if elapsed_seconds <= 0 or elapsed_seconds > 24 * 60 * 60:
                raise HTTPException(status_code=422, detail="punch_out must be after punch_in and within 24 hours")

    derived = _derived_values(
        employee, attendance_date, final_punch_in, final_punch_out, final_status
    )
    final_values = {
        "status": final_status,
        "punch_in": final_punch_in,
        "punch_out": final_punch_out,
        "work_hours": derived["work_hours"],
        "late_minutes": derived["late_minutes"],
        "overtime_minutes": derived["overtime_minutes"],
        "half_day": derived["half_day"],
    }

    change_fields = (
        "status",
        "punch_in",
        "punch_out",
        "work_hours",
        "late_minutes",
        "overtime_minutes",
        "half_day",
    )
    changes: dict = {}
    logical_defaults = {
        "status": current.get("status"),
        "punch_in": None,
        "punch_out": None,
        "work_hours": None,
        "late_minutes": 0,
        "overtime_minutes": 0,
        "half_day": False,
    }
    for field in change_fields:
        old_value = current.get(field, logical_defaults[field])
        # The data model defines missing derived fields as their default values.
        if old_value is None and field in ("late_minutes", "overtime_minutes"):
            old_value = 0
        if old_value is None and field == "half_day":
            old_value = False
        new_value = final_values[field]
        if old_value != new_value:
            changes[field] = {"from": old_value, "to": new_value}

    if not changes:
        raise HTTPException(status_code=422, detail="regularization does not change the record")

    history_entry = {
        "at": _now_utc_seconds(),
        "by": body.regularized_by,
        "reason": body.reason,
        "changes": changes,
    }

    # Optimistic compare-and-set covers all mutable fields and history. If another
    # correction/punch operation wins after our read, this update matches nothing
    # and returns 409 instead of overwriting its changes or audit entry.
    atomic_filter: dict = {"_id": current["_id"]}
    for field in change_fields:
        atomic_filter[field] = current.get(field)
    if "history" in current:
        atomic_filter["history"] = current["history"]
    else:
        atomic_filter["history"] = {"$exists": False}

    updated = attendance_logs.find_one_and_update(
        atomic_filter,
        {"$set": final_values, "$push": {"history": history_entry}},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise HTTPException(status_code=409, detail="record changed concurrently; retry regularization")
    return serialize_attendance(updated)


# =============================================================================
# 8. MONTHLY, DEPARTMENT, AND LATE-LEADERBOARD ANALYTICS ROUTES
# -----------------------------------------------------------------------------


@app.get("/analytics/employees/{emp_code}/monthly", tags=["analytics"])
def employee_monthly(
    emp_code: str,
    month: str = Query(..., pattern=MONTH_PATTERN),
) -> dict:
    pipeline = _employee_monthly_pipeline(emp_code, month)
    rows = list(employees.aggregate(pipeline, hint="employees_emp_code_unique", allowDiskUse=True))
    if not rows:
        raise HTTPException(status_code=404, detail="employee not found")
    return rows[0]


@app.get("/analytics/departments/summary", tags=["analytics"])
def department_summary(
    month: str = Query(..., pattern=MONTH_PATTERN),
    department: str | None = None,
) -> dict:
    pipeline = _department_summary_pipeline(month, department)
    rows = list(
        employees.aggregate(pipeline, hint="employees_joined_on", allowDiskUse=True)
    )
    return {"month": month, "items": rows}


@app.get("/analytics/leaderboard/late", tags=["analytics"])
def late_leaderboard(
    month: str = Query(..., pattern=MONTH_PATTERN),
    limit: int = Query(10, ge=1, le=50),
    department: str | None = None,
) -> dict:
    pipeline = _late_leaderboard_pipeline(month, limit, department)
    rows = list(
        attendance_logs.aggregate(pipeline, hint="attendance_date_emp", allowDiskUse=True)
    )
    return {"month": month, "items": rows}


@app.get("/analytics/departments/{department}/trend", tags=["analytics"])
def department_trend(
    department: str,
    from_: Date = Query(..., alias="from"),
    to: Date = Query(...),
) -> dict:
    if to < from_:
        raise HTTPException(status_code=422, detail="to must be on or after from")
    if (to - from_).days + 1 > 92:
        raise HTTPException(status_code=422, detail="date range must not exceed 92 days")

    pipeline = _department_trend_pipeline(department, from_, to)
    rows = list(
        employees.aggregate(pipeline, hint="employees_department", allowDiskUse=True)
    )
    if not rows:
        raise HTTPException(status_code=404, detail="department not found")
    return {"department": department, "items": rows}


# =============================================================================
# 9. ADMIN EXPLAIN ROUTE (EXECUTION-STATS QUERY PLANS)
# -----------------------------------------------------------------------------


@app.get("/admin/explain/{endpoint}", tags=["admin"])
def explain_endpoint(
    endpoint: Literal[
        "attendance_list",
        "employee_monthly",
        "department_summary",
        "late_leaderboard",
        "department_trend",
    ],
    emp_code: str | None = None,
    month: str | None = Query(None, pattern=MONTH_PATTERN),
    department: str | None = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Date | None = None,
    date_to: Date | None = None,
    status: Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"] | None = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    from_: Date | None = Query(None, alias="from"),
    to: Date | None = None,
) -> dict:
    """Return MongoDB executionStats for the selected endpoint's real query pipeline."""
    if endpoint == "attendance_list":
        query = _attendance_filter(emp_code, date_from, date_to, status)
        hint = _attendance_query_hint(emp_code, status)
        explain = _explain_find(
            "attendance_logs",
            query,
            {"date": -1, "emp_code": 1},
            (page - 1) * page_size,
            page_size,
            hint,
        )
        return {"endpoint": endpoint, "collection": "attendance_logs", "explain": _json_safe(explain)}

    if endpoint == "employee_monthly":
        if not emp_code:
            raise HTTPException(status_code=422, detail="emp_code is required for employee_monthly")
        selected_month = _required_explain_month(month)
        explain = _explain_aggregate(
            "employees", _employee_monthly_pipeline(emp_code, selected_month), "employees_emp_code_unique"
        )
        return {"endpoint": endpoint, "collection": "employees", "explain": _json_safe(explain)}

    if endpoint == "department_summary":
        selected_month = _required_explain_month(month)
        explain = _explain_aggregate(
            "employees", _department_summary_pipeline(selected_month, department), "employees_joined_on"
        )
        return {"endpoint": endpoint, "collection": "employees", "explain": _json_safe(explain)}

    if endpoint == "late_leaderboard":
        selected_month = _required_explain_month(month)
        explain = _explain_aggregate(
            "attendance_logs",
            _late_leaderboard_pipeline(selected_month, limit, department),
            "attendance_date_emp",
        )
        return {"endpoint": endpoint, "collection": "attendance_logs", "explain": _json_safe(explain)}

    # department_trend
    if not department:
        raise HTTPException(status_code=422, detail="department is required for department_trend")
    if from_ is None or to is None:
        raise HTTPException(status_code=422, detail="from and to are required for department_trend")
    if to < from_:
        raise HTTPException(status_code=422, detail="to must be on or after from")
    if (to - from_).days + 1 > 92:
        raise HTTPException(status_code=422, detail="date range must not exceed 92 days")
    explain = _explain_aggregate(
        "employees", _department_trend_pipeline(department, from_, to), "employees_department"
    )
    return {"endpoint": endpoint, "collection": "employees", "explain": _json_safe(explain)}
