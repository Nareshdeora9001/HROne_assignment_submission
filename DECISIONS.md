# DECISIONS.md  (about 150-200 words in total - plain language, your own words)

1. **Indexes.** Which indexes did you create, and which query or guarantee does each one serve? Which one did you consider and reject?
Answer:- I use the employees_emp_code_unique which ensure unique employee , employees_joined_on supports the period cutoff.
I use the employees_department and employees_department_emp_code for supporting department filtering and employee order.
the unique attendance_emp_date_unique enforces one log per employee/date and is the final guard against duplicate punch-in.
attendance_date_emp supports date range reads and ordering.
attendance_status_date_emp supports status/date filtering.
I would not add a standalone status index without explain evidence bcz satus along has low selectivity.


2. **Punch-in race.** Two identical punch-in requests arrive at the same instant. Walk through exactly what happens in your code.
Answer:- Both requests may initially pass their checks, but the unique (emp_code, date) index allows only one insertion. the other request receives a duplicate key error (499). one request returns 201.


3. **Ties.** Two employees are tied on late minutes at the cutoff of `limit`. What does your leaderboard return, and why?
Answer:- The leaderboard assigns rank before filtering (rank <= limit).
Employees tied at the cutoff are all returned, equal ranks share a number, and the next rank is skipped. Sorting by emp_code makes tied results deterministic


4. **Headcount.** How does your department summary make sure employees with zero logs are counted?
Answer:- The department summary starts from employees who joined by month-end and the looks up attendance logs. Employees eith no logs remain in the pipeline and still contribute to headcount


5. **One thing you would change** if this had to serve 100x the data.
Answer:- I would measure execution statistics during realistic queries, tune indexes based on observed plans, and consider precomputed summaries with a clearily defined refresh and consistency stretegy