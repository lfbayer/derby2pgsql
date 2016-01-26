/*
 * Copyright 2016 Leo Bayer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lbayer.derby2pgsql;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility for import a Derby database into a Postgres database
 */
public class Main
{
    private String schemaName;
    private String fromDbUrl;
    private String toDbUrl;

    public static void main(String[] args)
    {
        if (args.length < 3)
        {
            System.err.println("Usage: derby2pgsql <from-url> <to-url> <schema-name>");
            System.exit(2);
            return;
        }

        Main main = new Main();
        main.fromDbUrl = args[0];
        main.toDbUrl = args[1];
        main.schemaName = args[2];

        try
        {
            main.run();
        }
        catch (Throwable e)
        {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void run() throws SQLException, IOException
    {
        try (Connection derbyConn = DriverManager.getConnection(fromDbUrl);
             Connection psqlConn = DriverManager.getConnection(toDbUrl))
        {
            runWithConncetions(derbyConn, psqlConn);
        }
    }

    public void runWithConncetions(Connection derbyConn, Connection psqlConn) throws SQLException, IOException
    {
        execute("d", derbyConn, "SET SCHEMA " + schemaName.toUpperCase());

        execute("p", psqlConn, "SET CONSTRAINTS ALL DEFERRED");

        Iterable<String> tables = getTablesFromDerby(derbyConn);
        for (String table : tables)
        {
            execute("p", psqlConn, "ALTER TABLE " + table + " DISABLE TRIGGER ALL");
        }

        for (String table : tables)
        {
            copyData(derbyConn, psqlConn, table);
        }

        for (String table : tables)
        {
            execute("p", psqlConn, "ALTER TABLE " + table + " ENABLE TRIGGER ALL");
        }

        execute("p", psqlConn, "SET CONSTRAINTS ALL IMMEDIATE");

        execute("p", psqlConn, "DROP FUNCTION IF EXISTS fixup_sequences()");
        execute("p", psqlConn,
                "CREATE FUNCTION fixup_sequences() RETURNS TABLE (table_name TEXT, column_name TEXT, new_sequence BIGINT) AS $$\n" +
                "  DECLARE\n" +
                "    target_seq RECORD;\n" +
                "    max_result RECORD;\n" +
                "  BEGIN\n" +
                "    FOR target_seq IN select s.relname as seq, n.nspname as sch, t.relname as tab, a.attname as col\n" +
                "          from pg_class s\n" +
                "          join pg_depend d on d.objid=s.oid and d.classid='pg_class'::regclass and d.refclassid='pg_class'::regclass\n" +
                "          join pg_class t on t.oid=d.refobjid\n" +
                "          join pg_namespace n on n.oid=t.relnamespace\n" +
                "          join pg_attribute a on a.attrelid=t.oid and a.attnum=d.refobjsubid\n" +
                "          where s.relkind='S' and d.deptype='a'\n" +
                "    LOOP\n" +
                "      FOR max_result IN EXECUTE 'SELECT cast((coalesce(MAX(' || quote_ident(target_seq.col) || '), 0)+1) as BIGINT) as last FROM ' || quote_ident(target_seq.tab)\n" +
                "      LOOP\n" +
//                "        RAISE NOTICE 'new sequence value for column %.%: %', target_seq.tab, target_seq.col, max_result.last;\n" +
                "        EXECUTE 'SELECT setval(''' || quote_ident(target_seq.seq) || ''', ' || max_result.last || ', false)';\n" +
                "        table_name := target_seq.tab;\n" +
                "        column_name := target_seq.col;\n" +
                "        new_sequence := max_result.last;\n" +
                "        RETURN NEXT;\n" +
                "      END LOOP;\n" +
                "    END LOOP;\n" +
                "  END;\n" +
                "$$ LANGUAGE plpgsql");
        executeSelect("p", psqlConn, "SELECT * FROM fixup_sequences()");
        execute("p", psqlConn, "DROP FUNCTION fixup_sequences()");

        execute("p", psqlConn, "DROP FUNCTION IF EXISTS fix_up_manual_sequences()");
        execute("p", psqlConn,
                "CREATE FUNCTION fix_up_manual_sequences() RETURNS TABLE (table_name TEXT, new_sequence BIGINT) AS $$\n" +
                "DECLARE\n" +
                "  target_seq RECORD;\n" +
                "  max_result RECORD;\n" +
                "BEGIN\n" +
                "  FOR target_seq IN SELECT s.relname as seq, substr(s.relname, 0, length(s.relname) - 3) AS tab\n" +
                "                    FROM pg_class s\n" +
                "                      LEFT JOIN pg_depend d on d.objid=s.oid and d.classid='pg_class'::regclass and d.refclassid='pg_class'::regclass\n" +
                "                    WHERE s.relkind='S' AND d.objid IS NULL\n" +
                "  LOOP\n" +
                "    FOR max_result IN EXECUTE 'SELECT cast((coalesce(MAX(id), 0)+1) as BIGINT) as last FROM ' || quote_ident(target_seq.tab)\n" +
                "    LOOP\n" +
                "      EXECUTE 'SELECT setval(''' || quote_ident(target_seq.seq) || ''', ' || max_result.last || ', false)';\n" +
                "      table_name := target_seq.tab;\n" +
                "      new_sequence := max_result.last;\n" +
                "      RETURN NEXT;\n" +
                "    END LOOP;\n" +
                "  END LOOP;\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql");
        executeSelect("p", psqlConn, "SELECT * FROM fix_up_manual_sequences()");
        execute("p", psqlConn, "DROP FUNCTION fix_up_manual_sequences()");
    }

    private void copyData(Connection derbyConn, Connection psqlConn, String table) throws SQLException, IOException
    {
        execute("p", psqlConn, "DELETE FROM " + table);

        List<Integer> dataTypes = new ArrayList<>();
        StringBuilder columnCsv = new StringBuilder();
        StringBuilder questionCsv = new StringBuilder();
        try (ResultSet result = derbyConn.getMetaData().getColumns(null, schemaName.toUpperCase(), table, "%"))
        {
            while (result.next())
            {
                columnCsv.append(result.getString("COLUMN_NAME")).append(',');
                questionCsv.append("?,");

                dataTypes.add(result.getInt("DATA_TYPE"));
            }
        }

        if (dataTypes.isEmpty())
        {
            throw new RuntimeException("No columns found for table: " + table);
        }

        // trim final comma
        columnCsv.setLength(columnCsv.length() - 1);
        questionCsv.setLength(questionCsv.length() - 1);

        List<Integer> targetTypes = new ArrayList<>();
        try (ResultSet result = psqlConn.getMetaData().getColumns(null, null, table.toLowerCase(), "%"))
        {
            while (result.next())
            {
                targetTypes.add(result.getInt("DATA_TYPE"));
            }
        }

        if (dataTypes.size() != targetTypes.size())
        {
            throw new RuntimeException("Number of columns doesn't match for table " + table);
        }

        String sql = "SELECT " + columnCsv + " FROM " + table;
        System.out.println("d> " + sql);
        System.out.println(" - types: " + String.join(",", dataTypes.stream().map(String::valueOf).collect(Collectors.toList())));
        try (PreparedStatement stmt = derbyConn.prepareStatement(sql);
             ResultSet result = stmt.executeQuery())
        {
            String insertSql = "INSERT INTO " + table + " (" + columnCsv + ") VALUES (" + questionCsv + ")";

            System.out.println("p> " + insertSql);
            System.out.println(" - types: " + String.join(",", targetTypes.stream().map(String::valueOf).collect(Collectors.toList())));
            try (PreparedStatement insert = psqlConn.prepareStatement(insertSql))
            {
                while (result.next())
                {
                    for (int i = 0; i < dataTypes.size(); i++)
                    {
                        copyColumn(result, insert, i+1, dataTypes.get(i), targetTypes.get(i));
                    }
                    insert.executeUpdate();
                }
            }
        }
    }

    private void copyColumn(ResultSet result, PreparedStatement insert, int columnIndex, int type, int targetType) throws SQLException, IOException
    {
        switch (type)
        {
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.BLOB:
            Blob blob = result.getBlob(columnIndex);
            if (blob != null)
            {
                insert.setBinaryStream(columnIndex, blob.getBinaryStream(), blob.length());
            }
            else
            {
                insert.setBytes(columnIndex, null);
            }

            return;

        case Types.CLOB:
        case Types.CHAR:
        case Types.LONGNVARCHAR:
        case Types.VARCHAR:
            String varchar = result.getString(columnIndex);
            insert.setString(columnIndex, varchar);
            return;

        case Types.TIME:
            Time time = result.getTime(columnIndex);
            insert.setTime(columnIndex, time);
            break;

        case Types.DATE:
            Date date = result.getDate(columnIndex);
            insert.setDate(columnIndex, date);
            break;

        case Types.TIMESTAMP:
            Timestamp tstamp = result.getTimestamp(columnIndex);
            insert.setTimestamp(columnIndex, tstamp);
            break;

        case Types.SMALLINT:
        case Types.INTEGER:
            int smallInt = result.getInt(columnIndex);
            if (targetType == Types.BIT)
            {
                insert.setBoolean(columnIndex, smallInt != 0);
            }
            else
            {
                insert.setInt(columnIndex, smallInt);
            }

            break;

        case Types.BOOLEAN:
            boolean b = result.getBoolean(columnIndex);
            insert.setBoolean(columnIndex, b);
            break;

        case Types.BIGINT:
            long bigInt = result.getLong(columnIndex);
            insert.setLong(columnIndex, bigInt);
            break;

        case Types.NUMERIC:
        case Types.DECIMAL:
            BigDecimal bigDec = result.getBigDecimal(columnIndex);
            insert.setBigDecimal(columnIndex, bigDec);
            break;

        case Types.REAL:
        case Types.FLOAT:
            Float dec = result.getFloat(columnIndex);
            insert.setFloat(columnIndex, dec);
            break;

        case Types.DOUBLE:
            Double dbl = result.getDouble(columnIndex);
            insert.setDouble(columnIndex, dbl);
            break;

        default:
            System.out.println("Unknown type: " + type);
            Object obj = result.getObject(columnIndex);
            insert.setObject(columnIndex, obj);
            break;
        }

        if (result.wasNull())
        {
            insert.setNull(columnIndex, targetType);
        }
    }

    private void executeSelect(String prefix, Connection conn, String sql) throws SQLException
    {
        System.out.println(prefix + "> " + sql);

        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet result = stmt.executeQuery())
        {
            System.out.print("< ");

            ResultSetMetaData md = stmt.getMetaData();
            int count = md.getColumnCount();
            for (int i = 1; i <= count; i++)
            {
                if (i > 1)
                {
                    System.out.print(", ");
                }

                System.out.print(md.getColumnName(i));
            }
            System.out.println();

            while (result.next())
            {
                System.out.print("< ");
                for (int i = 1; i <= count; i++)
                {
                    if (i > 1)
                    {
                        System.out.print(", ");
                    }

                    System.out.print(result.getString(i));
                }

                System.out.println();
            }
        }
    }

    private int execute(String prefix, Connection conn, String sql) throws SQLException
    {
        System.out.println(prefix + "> " + sql);
        try (PreparedStatement stmt = conn.prepareStatement(sql))
        {
            return stmt.executeUpdate();
        }
    }

    public Iterable<String> getTablesFromDerby(Connection derbyConn) throws SQLException
    {
        ArrayList<String> tables = new ArrayList<>();
        DatabaseMetaData dmd = derbyConn.getMetaData();
        try (ResultSet result = dmd.getTables(null, schemaName.toUpperCase(), null, new String[] { "TABLE" }))
        {
            while (result.next())
            {
                tables.add(result.getString("TABLE_NAME"));
            }
        }

        return tables;
    }
}
