using CsvHelper;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;

public class Program
{
    private static readonly string connectionStringToSQLServer = "Server=localhost;Database=CabData;Trusted_Connection=True;";
    private static readonly string connectionStringToLocalDB = "Data Source=(localdb)\\MSSQLLocalDB;Initial Catalog=master;Database=CabData;Trusted_Connection=True;Integrated Security=True;Connect Timeout=30;Encrypt=False;";
    private static readonly TimeZoneInfo estZone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
    private static readonly int batchSize = 10000;

    public static void Main(string[] args)
    {
        if (args.Length == 0)
        {
            Console.WriteLine("Usage: dotnet run -- <csv_file_path>");
            return;
        }

        string csvFilePath = args[0];

        try
        {
            using (var connection = new SqlConnection(connectionStringToLocalDB))
            {
                connection.Open();

                // Clear staging table
                using (var cmd = new SqlCommand("TRUNCATE TABLE Staging_CabTrips", connection))
                {
                    cmd.ExecuteNonQuery();
                }

                // Process CSV and load into staging table
                ProcessCsv(csvFilePath, connection);

                // Insert unique records into final table
                InsertUniqueRecords(connection);

                // Export duplicates to CSV
                ExportDuplicates(connection);

                // Print row count
                PrintRowCount(connection);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }

    private static void ProcessCsv(string csvFilePath, SqlConnection connection)
    {
        using (var reader = new StreamReader(csvFilePath))
        using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
        {
            csv.Read();
            csv.ReadHeader();

            var dataTable = CreateDataTable();
            int rowCount = 0;

            while (csv.Read())
            {
                try
                {
                    var row = dataTable.NewRow();
                    row["tpep_pickup_datetime"] = ParseDateTime(csv["tpep_pickup_datetime"]);
                    row["tpep_dropoff_datetime"] = ParseDateTime(csv["tpep_dropoff_datetime"]);
                    row["passenger_count"] = csv.GetField<int>("passenger_count");
                    row["trip_distance"] = csv.GetField<decimal>("trip_distance");
                    row["store_and_fwd_flag"] = MapFlag(csv["store_and_fwd_flag"].Trim());
                    row["PULocationID"] = csv.GetField<int>("PULocationID");
                    row["DOLocationID"] = csv.GetField<int>("DOLocationID");
                    row["fare_amount"] = csv.GetField<decimal>("fare_amount");
                    row["tip_amount"] = csv.GetField<decimal>("tip_amount");
                    dataTable.Rows.Add(row);

                    rowCount++;

                    if (rowCount % batchSize == 0)
                    {
                        BulkInsert(dataTable, connection);
                        dataTable.Rows.Clear();
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Skipping row due to error: {ex.Message}");
                }
            }

            if (dataTable.Rows.Count > 0)
            {
                BulkInsert(dataTable, connection);
            }

            Console.WriteLine($"Processed {rowCount} rows from CSV.");
        }
    }

    private static DataTable CreateDataTable()
    {
        var dataTable = new DataTable();
        dataTable.Columns.Add("tpep_pickup_datetime", typeof(DateTime));
        dataTable.Columns.Add("tpep_dropoff_datetime", typeof(DateTime));
        dataTable.Columns.Add("passenger_count", typeof(int));
        dataTable.Columns.Add("trip_distance", typeof(decimal));
        dataTable.Columns.Add("store_and_fwd_flag", typeof(string));
        dataTable.Columns.Add("PULocationID", typeof(int));
        dataTable.Columns.Add("DOLocationID", typeof(int));
        dataTable.Columns.Add("fare_amount", typeof(decimal));
        dataTable.Columns.Add("tip_amount", typeof(decimal));
        return dataTable;
    }

    private static DateTime ParseDateTime(string dateStr)
    {
        var dt = DateTime.ParseExact(dateStr, "MM/dd/yyyy hh:mm:ss tt", CultureInfo.InvariantCulture);
        return TimeZoneInfo.ConvertTimeToUtc(dt, estZone);
    }

    private static string MapFlag(string flag)
    {
        return flag switch
        {
            "N" => "No",
            "Y" => "Yes",
            _ => throw new ArgumentException($"Invalid store_and_fwd_flag value: {flag}")
        };
    }

    private static void BulkInsert(DataTable dataTable, SqlConnection connection)
    {
        using (var bulkCopy = new SqlBulkCopy(connection))
        {
            bulkCopy.DestinationTableName = "Staging_CabTrips";
            bulkCopy.WriteToServer(dataTable);
        }
    }

    private static void InsertUniqueRecords(SqlConnection connection)
    {
        string insertUniqueSql = @"
            INSERT INTO CabTrips
            SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, 
                   store_and_fwd_flag, PULocationID, DOLocationID, fare_amount, tip_amount
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count 
                                             ORDER BY (SELECT NULL)) AS rn
                FROM Staging_CabTrips
            ) AS sub
            WHERE rn = 1;
        ";
        using (var cmd = new SqlCommand(insertUniqueSql, connection))
        {
            cmd.ExecuteNonQuery();
        }
    }

    private static void ExportDuplicates(SqlConnection connection)
    {
        string duplicatesSql = @"
            SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, 
                   store_and_fwd_flag, PULocationID, DOLocationID, fare_amount, tip_amount
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count 
                                             ORDER BY (SELECT NULL)) AS rn
                FROM Staging_CabTrips
            ) AS sub
            WHERE rn > 1;
        ";
        using (var cmd = new SqlCommand(duplicatesSql, connection))
        using (var reader = cmd.ExecuteReader())
        using (var writer = new StreamWriter("duplicates.csv"))
        using (var csvWriter = new CsvWriter(writer, CultureInfo.InvariantCulture))
        {
            // Write header
            for (int i = 0; i < reader.FieldCount; i++)
            {
                csvWriter.WriteField(reader.GetName(i));
            }
            csvWriter.NextRecord();

            // Write duplicate rows
            int duplicateCount = 0;
            while (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    csvWriter.WriteField(reader.GetValue(i));
                }
                csvWriter.NextRecord();
                duplicateCount++;
            }
            Console.WriteLine($"Exported {duplicateCount} duplicate rows to duplicates.csv.");
        }
    }

    private static void PrintRowCount(SqlConnection connection)
    {
        using (var cmd = new SqlCommand("SELECT COUNT(*) FROM CabTrips", connection))
        {
            int rowCount = (int)cmd.ExecuteScalar();
            Console.WriteLine($"Number of rows in CabTrips: {rowCount}");
        }
    }
}