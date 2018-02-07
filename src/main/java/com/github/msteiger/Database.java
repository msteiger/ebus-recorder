package com.github.msteiger;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Map.Entry;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.InsertSetStep;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.tools.jdbc.JDBCUtils;
import org.jooq.types.UInteger;
import org.mariadb.jdbc.MariaDbPoolDataSource;

/**
 * TODO: describe
 */
public class Database implements Closeable {

    /**
     * The column <code>ebus_recorder.betrd.id</code>.
     */
    public final Field<UInteger> ID = DSL.field("id", SQLDataType.INTEGERUNSIGNED.nullable(false).identity(true));

    /**
     * The column <code>ebus_recorder.betrd.timestamp</code>.
     */
    public final Field<Timestamp> TIMESTAMP = DSL.field("timestamp", SQLDataType.TIMESTAMP.nullable(false).defaultValue(DSL.field("CURRENT_TIMESTAMP", SQLDataType.TIMESTAMP)));

    /**
     * The column <code>ebus_recorder.betrd.status</code>.
     */
    public final Field<Short> STATUS = DSL.field("status", SQLDataType.SMALLINT.nullable(false));

    /**
     * The column <code>ebus_recorder.betrd.zustand</code>.
     */
    public final Field<Short> ZUSTAND = DSL.field("zustand", SQLDataType.SMALLINT.nullable(false));

    /**
     * The column <code>ebus_recorder.betrd.stellgrad</code>.
     */
    public final Field<Short> STELLGRAD = DSL.field("stellgrad", SQLDataType.SMALLINT.nullable(false));

    /**
     * The column <code>ebus_recorder.betrd.kesseltemp</code>.
     */
    public final Field<Double> KESSELTEMP = DSL.field("kesseltemp", SQLDataType.FLOAT.nullable(false));

    /**
     * The column <code>ebus_recorder.betrd.ruecklauftemp</code>.
     */
    public final Field<Double> RUECKLAUFTEMP = DSL.field("ruecklauftemp", SQLDataType.FLOAT.nullable(false));

    /**
     * The column <code>ebus_recorder.betrd.aussentemp</code>.
     */
    public final Field<Double> AUSSENTEMP = DSL.field("aussentemp", SQLDataType.FLOAT.nullable(false));

    /**
     * The column <code>ebus_recorder.betrd.boilertemp</code>.
     */
    public final Field<Double> BOILERTEMP = DSL.field("boilertemp", SQLDataType.FLOAT.nullable(false));

    private MariaDbPoolDataSource pool;
    private DSLContext context;
    private Table<Record> table;

    public Database(String jdbc) throws SQLException {
        SQLDialect dialect = JDBCUtils.dialect(jdbc);

        pool = new MariaDbPoolDataSource(jdbc);
        context = DSL.using(pool, dialect);
        table = DSL.table(DSL.name("betrd"));
    }

    public void insert(Map<String, String> data) throws SQLException {
        long time = System.nanoTime();
        InsertSetStep<Record> set = context.insertInto(table);

//        .set(AUSSENTEMP, Double.valueOf(data.get("aussentemp")));
//
//        set.execute();
        for (Entry<String, String> entry : data.entrySet()) {
            Field<Object> field = DSL.field(DSL.name(entry.getKey()));
            set.set(field, entry.getValue());
        }
        System.out.println(set.columns());
        set.columns().execute();
        context.close();
        System.out.println("Took " + (System.nanoTime() - time) * 0.000001);
    }

    @Override
    public void close() throws IOException {
        pool.close();
    }
}
