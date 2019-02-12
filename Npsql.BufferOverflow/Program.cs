using System;
using System.Diagnostics;
using System.Globalization;
using Npgsql;
using NpgsqlTypes;

namespace Npsql.BufferOverflow
{
    public static class App
    {
        private static DateTime MaxDate { get; } = DateTime.Parse("9999-01-01", DateTimeFormatInfo.InvariantInfo);
        private static DateTime Now { get; } = DateTime.Parse("2/12/2019 12:00:00 AM", DateTimeFormatInfo.InvariantInfo);
        private static string GenerateString(int length) => new string('1', length);
        private static int GenerateNumber(int length) => int.Parse(GenerateString(length));
        
        private static readonly string template =
            @"INSERT INTO ""dbo1"".""tbl1"" (""id"", ""col_1_1"", ""col_2_2_2"", ""col_3_3_"", ""col_4_4"", ""col5"", ""col_6"", ""col_7_"", ""col_8_8_8"", ""col_9_9_9_9_9_"", ""col_10_10"", ""col_11_"", ""col_12_1"", ""col_13_"") VALUES ({0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13});";

        private static void AppendStatement(NpgsqlCommand cmd, int i, int variableParameterLength)
        {
            cmd.CommandText += "\n" + string.Format(template, $"@p1_{i}", $"@p2_{i}", $"@p3_{i}", $"@p4_{i}", $"@p5_{i}",
                                   $"@p6_{i}", $"@p7_{i}", $"@p8_{i}", $"@p9_{i}", $"@p10_{i}", $"@p11_{i}", $"@p12_{i}", $"@p13_{i}",
                                   $"@p14_{i}");
            
            cmd.Parameters.AddWithValue($"p1_{i}", NpgsqlDbType.Integer, GenerateNumber(4));
            cmd.Parameters.AddWithValue($"p2_{i}", NpgsqlDbType.Text, GenerateString(7));
            cmd.Parameters.AddWithValue($"p3_{i}", NpgsqlDbType.Text, GenerateString(4));
            cmd.Parameters.AddWithValue($"p4_{i}", NpgsqlDbType.Text, GenerateString(variableParameterLength));
            // This column is mapped to an enum in db, but due to specific of our code we map it as string
            // We are using an F# Npgsql type provider in out solution.
            // https://github.com/demetrixbio/FSharp.Data.Npgsql
            cmd.Parameters.AddWithValue($"p5_{i}", NpgsqlDbType.Unknown, GenerateString(3));
            cmd.Parameters.AddWithValue($"p6_{i}", NpgsqlDbType.Text, GenerateString(64));
            cmd.Parameters.AddWithValue($"p7_{i}", NpgsqlDbType.Integer, GenerateNumber(3));
            cmd.Parameters.AddWithValue($"p8_{i}", NpgsqlDbType.Integer, variableParameterLength.ToString().Length);
            cmd.Parameters.AddWithValue($"p9_{i}", NpgsqlDbType.Text, GenerateString(2));
            cmd.Parameters.AddWithValue($"p10_{i}", NpgsqlDbType.Integer, GenerateNumber(1));
            cmd.Parameters.AddWithValue($"p11_{i}", NpgsqlDbType.Unknown, DBNull.Value);
            cmd.Parameters.AddWithValue($"p12_{i}", NpgsqlDbType.Timestamp, Now);
            cmd.Parameters.AddWithValue($"p13_{i}", NpgsqlDbType.Timestamp, MaxDate);
            cmd.Parameters.AddWithValue($"p14_{i}", NpgsqlDbType.Timestamp, Now);
        }

        private static void ResetBuffer(NpgsqlConnection connection, NpgsqlTransaction transaction)
        {
            using (var cmd = new NpgsqlCommand())
            {
                cmd.Connection = connection;
                cmd.Transaction = transaction;
                cmd.CommandText = "SELECT 1;";
                cmd.ExecuteNonQuery();
            }
        }
        
        public static void Main()
        {
            const string connString = "Host=localhost;Username=postgres;Password=postgres;Database=postgres";
            using (var conn = new NpgsqlConnection(connString))
            {
                conn.Open();
                using (var transaction = conn.BeginTransaction())
                {
                    ResetBuffer(conn, transaction);
                    using (var cmd = new NpgsqlCommand(""))
                    {
                        cmd.Connection = conn;
                        cmd.Transaction = transaction;
                        AppendStatement(cmd, 5, 1120);
                        AppendStatement(cmd, 6, 1118);
                        AppendStatement(cmd, 7, 1124);
                        AppendStatement(cmd, 8, 1108);
                        AppendStatement(cmd, 9, 1117);
                        try
                        {
                            cmd.ExecuteNonQuery();
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex);
                            Debugger.Break();
                        }
                        transaction.Rollback();
                    }
                }
            }
        }
    }
}