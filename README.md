Number of rows in your table after running the program: **29840**
## Assumptions and Comments
 - SQL Server Setup: Assumes a local SQL Server instance is available with Windows Authentication (Trusted_Connection=True).
 - Data Format: Assumes the CSV follows the sample format, with datetime strings in "MM/dd/yyyy hh:mm:ss tt" and valid numeric values. Invalid rows are skipped.
 - EST Timezone: "Eastern Standard Time" includes daylight saving adjustments (EDT when applicable), handled by TimeZoneInfo.
 - No Additional Columns: Only the specified columns are stored, though a computed column for duration could enhance query 3 if allowed.
 - Performance: Indexes are sufficient for the sample size; for larger datasets, additional tuning (e.g., clustered index) might be considered.