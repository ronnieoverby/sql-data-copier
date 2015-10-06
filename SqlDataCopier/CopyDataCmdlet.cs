using System;
using System.Linq;
using System.Management.Automation;
using System.Threading;
using System.Data.SqlClient;
using CoreTechs.Common.Database;
using System.Collections.Generic;

namespace SqlDataCopier
{
    [Cmdlet("Copy", "SqlData")]
    public class CopySqlDataCmdlet : Cmdlet
    {
        [Parameter(Mandatory = true)]
        public string SourceConnectionString { get; set; }

        [Parameter(Mandatory = true)]
        public string DestinationConnectionString { get; set; }

        [Parameter]
        public string[] Tables { get; set; }

        [Parameter]
        public string[] ExcludedTables { get; set; }

        [Parameter]
        public SqlBulkCopyOptions BulkCopyOptions { get; set; } = SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls;

        internal void TestProcessRecord()
        {
            ProcessRecord();
        }

        protected override void ProcessRecord()
        {
            var tx = new SqlDataTransfer(SourceConnectionString, DestinationConnectionString);

            if (!(Tables?.Length > 0))
            {
                Tables = GetTables();
            }

            if (ExcludedTables?.Length > 0)
            {
                Tables = Tables.Except(ExcludedTables, StringComparer.OrdinalIgnoreCase).ToArray();
            }

            var rowCounts = GetRowCounts();
            var total = rowCounts.Sum(x => x.Value);
            var copyCounts = Tables.ToDictionary(x => x, x => 0L);
            string currentTable = "";
            bool dontWriteProgress = false;


            SqlRowsCopiedEventHandler rowsCopied = (s, e) =>
            {
                copyCounts[currentTable] = e.RowsCopied;
                

                if (!dontWriteProgress)
                {
                    try
                    {

                        WriteProgress(new ProgressRecord(1, "SQL Data Copy", "Copy progress for all data.")
                        {
                            PercentComplete = GetPercent(copyCounts.Sum(x=>x.Value), total)
                        });

                        WriteProgress(new ProgressRecord(2, $"Copying {currentTable}", "Copy progress for current table.")
                        {
                            PercentComplete = GetPercent(copyCounts[currentTable], rowCounts[currentTable])
                        });
                    }
                    catch
                    {
                        dontWriteProgress = true;
                    }
                }
            };

            foreach (var table in Tables)
            {
                currentTable = table;
                tx.TransferData(table, sqlBulkCopyOptions: BulkCopyOptions, sqlBulkCopyCustomizer: bcp =>
                {
                    bcp.EnableStreaming = true;
                    bcp.SqlRowsCopied += rowsCopied;
                    bcp.NotifyAfter = 2000;
                });
            }
        }

        private Dictionary<string, int> GetRowCounts()
        {
            return Tables.ToDictionary(t => t, CountRows);
        }

        private int GetPercent(long copied, long total)
        {
            return (int)(copied * 100.0 / total);
        }

        private int CountRows(string table)
        {
            using (var source = new SqlConnection(SourceConnectionString))
            {
                return source.ScalarSql<int>(@"SELECT SUM (row_count)
FROM sys.dm_db_partition_stats
WHERE object_id=OBJECT_ID( @table )   
AND (index_id=0 or index_id=1);", new SqlParameter("@table", table));
            }
        }

        private string[] GetTables()
        {
            const string sql = @"WITH TablesCTE(SchemaName, TableName, TableID, Ordinal) AS
(
    SELECT
        OBJECT_SCHEMA_NAME(so.object_id) AS SchemaName,
        OBJECT_NAME(so.object_id) AS TableName,
        so.object_id AS TableID,
        0 AS Ordinal
    FROM
        sys.objects AS so
    WHERE
        so.type = 'U'
        AND so.is_ms_Shipped = 0
    UNION ALL
    SELECT
        OBJECT_SCHEMA_NAME(so.object_id) AS SchemaName,
        OBJECT_NAME(so.object_id) AS TableName,
        so.object_id AS TableID,
        tt.Ordinal + 1 AS Ordinal
    FROM
        sys.objects AS so
    INNER JOIN sys.foreign_keys AS f
        ON f.parent_object_id = so.object_id
        AND f.parent_object_id != f.referenced_object_id
    INNER JOIN TablesCTE AS tt
        ON f.referenced_object_id = tt.TableID
    WHERE
        so.type = 'U'
        AND so.is_ms_Shipped = 0
)

SELECT DISTINCT
        t.Ordinal,
        t.SchemaName,
        t.TableName,
        t.TableID
    FROM
        TablesCTE AS t
    INNER JOIN
        (
            SELECT
                itt.SchemaName as SchemaName,
                itt.TableName as TableName,
                itt.TableID as TableID,
                Max(itt.Ordinal) as Ordinal
            FROM
                TablesCTE AS itt
            GROUP BY
                itt.SchemaName,
                itt.TableName,
                itt.TableID
        ) AS tt
        ON t.TableID = tt.TableID
        AND t.Ordinal = tt.Ordinal
ORDER BY
    t.Ordinal,
    t.TableName";

            using (var source = new SqlConnection(SourceConnectionString))
            using (var dest = new SqlConnection(DestinationConnectionString))
            {
                var sourceTables = source.QuerySql(sql).AsDynamic();
                var destTables = dest.QuerySql(sql).AsDynamic();

                var q = from s in sourceTables
                        from d in destTables
                        where s.TableName == d.TableName && s.SchemaName == d.SchemaName
                        orderby d.Ordinal
                        select $"{d.SchemaName}.{d.TableName}";

                return q.ToArray();
            }

        }
    }
}
