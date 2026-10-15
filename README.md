# Candidate kit

Read in this order:

1. `PROBLEM_STATEMENT.docx`: what you are asked to do, how it is evaluated.
2. `openapi.yaml`: the API contract and the business rules (R1 - R10).
3. `DATA_MODEL.md`: what is stored in MongoDB, with nine sample documents.
4. `app/main.py`: the starter. Put **all** your code in this one file. It must start with `uvicorn app.main:app --port 8000`.
5. `REVIEW.md`, `DECISIONS.md`: templates to fill in.

`sample_data/` holds the nine sample documents as MongoDB Extended JSON; `sample_seed.py` loads them. The grader's dataset is
much larger and contains cases they do not show.

## Local setup
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env            # set MONGO_URI / MONGO_DB (local docker: docker run -p 27017:27017 mongo:7)
python sample_seed.py
uvicorn app.main:app --port 8000 --reload
```
Employee Attendance & Analytics API
A small backend project built with FastAPI and MongoDB for the HROne engineering assignment. It manages employee attendance, punch-in/punch-out, attendance corrections, and monthly or department analytics.

What it does
- Create and list employees.
- Record punch-in and punch-out times.
- Correct an attendance record and keep a history of changes.
- Show monthly employee reports, department summaries, late-comer rankings, and daily trends.
- Use MongoDB indexes and aggregation pipelines for data queries.
Run locally

You need Python 3.11+ and access to MongoDB 6.0+.
1. Install the packages:
   pip install -r requirements.txt
2. Set the database settings in your local environment (or in a local .env file that must not be committed):
   MONGO_URI=mongodb+srv://hrone_benchmark_user:HROne_naresh_011@cluster0.3odclxv.mongodb.net/
   MONGO_DB=attendance_db
3. Start the API from the project root:
   uvicorn app.main:app --port 8000
4. Open http://127.0.0.1:8000/docs to try the API. The /health endpoint checks whether the service can reach MongoDB.
Simple flow
```mermaid
flowchart TD
    A[Client / API Docs] --> B[FastAPI Routes]
    B --> C[Validate Request]
    C --> D{Valid request?}
    D -- No --> E[Return validation error]
    D -- Yes --> F[MongoDB collections]
    F --> G[Attendance records and indexes]
    F --> H[Aggregation pipelines for analytics]
    G --> I[API response]
    H --> I
```
Notes
- Timestamps sent to the API use integer epoch milliseconds; MongoDB stores instants as UTC dates.
- Attendance dates and shift times follow the assignment's IST rules.
- Keep .env, database credentials, virtual environments, and sample data dumps out of the public repository.
