package com.example.tsid;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.hypersistence.tsid.TSID;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.types.ObjectId;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.function.LongFunction;
import java.util.function.ToLongFunction;

/**
 * Scaling benchmark — runs the Postgres and MongoDB insert + read comparisons at
 * N = 100k, 200k, …, 1M and prints trend tables.
 *
 * <p>Compares four id types where applicable:
 * <ul>
 *   <li>TSID (8 bytes, time-ordered)</li>
 *   <li>UUIDv4 ({@code UUID.randomUUID()}, 16 bytes, random — the typical default)</li>
 *   <li>UUIDv7 (16 bytes, time-ordered; in PG generated server-side via the native {@code uuidv7()} DEFAULT)</li>
 *   <li>ObjectId (Mongo only, 12 bytes, time-ordered)</li>
 * </ul>
 *
 * <p>Phase order rotates per N so no single id type always runs first against a cold cache.
 *
 * <p>Tables and collections are dropped and recreated per iteration — no pre-seeded state.
 * Expect ~6 minutes end-to-end on a local Podman stack.
 */
public final class TsidScalingBench {

    private static final String PG_URL  = "jdbc:postgresql://localhost:5432/tsiddemo";
    private static final String PG_USER = "demouser";
    private static final String PG_PASS = "demopass";

    private static final String MONGO_URI = "mongodb://demouser:demopass@localhost:27017/?authSource=admin";
    private static final String MONGO_DB  = "tsiddemo";

    private static final int BATCH        = 1_000;
    private static final int READ_WARMUP  = 10_000;
    private static final int READ_SAMPLES = 100_000;

    private static final int[] SCALE = {
        100_000, 200_000, 300_000, 400_000, 500_000,
        600_000, 700_000, 800_000, 900_000, 1_000_000
    };

    public static void main(String[] args) throws Exception {
        banner("TSID scaling benchmark — 100k to 1M in 100k increments");
        System.out.printf("Per iteration: %,d rows per source, %,d warmup + %,d measured reads%n",
                SCALE[SCALE.length - 1], READ_WARMUP, READ_SAMPLES);
        System.out.println("Phase order rotates per N so no id type always faces a cold cache.");

        PgRow[] pgResults = new PgRow[SCALE.length];
        MongoRow[] mongoResults = new MongoRow[SCALE.length];

        System.out.println();
        System.out.println("Running Postgres iterations...");
        for (int i = 0; i < SCALE.length; i++) {
            long t0 = System.nanoTime();
            pgResults[i] = benchPostgres(SCALE[i], i);
            long elapsed = (System.nanoTime() - t0) / 1_000_000;
            System.out.printf("  N = %,9d  done in %,5d ms%n", SCALE[i], elapsed);
        }

        System.out.println();
        System.out.println("Running MongoDB iterations...");
        for (int i = 0; i < SCALE.length; i++) {
            long t0 = System.nanoTime();
            mongoResults[i] = benchMongo(SCALE[i], i);
            long elapsed = (System.nanoTime() - t0) / 1_000_000;
            System.out.printf("  N = %,9d  done in %,5d ms%n", SCALE[i], elapsed);
        }

        header("Postgres scaling — TSID (BIGINT) vs UUIDv4 vs UUIDv7 (native uuidv7())");
        printPgStorageTable(pgResults);
        System.out.println();
        printPgReadTable(pgResults);

        header("MongoDB scaling — TSID (NumberLong) vs UUIDv4 vs UUIDv7 vs ObjectId (12B)");
        printMongoStorageTable(mongoResults);
        System.out.println();
        printMongoReadTable(mongoResults);

        banner("Scaling benchmark complete");
    }

    // ============================================================================
    // Postgres
    // ============================================================================

    record PgRow(
            int n,
            long tsidInsertMs,  long uuid4InsertMs,  long uuid7InsertMs,
            long tsidIdxKB,     long uuid4IdxKB,     long uuid7IdxKB,
            long tsidTotalKB,   long uuid4TotalKB,   long uuid7TotalKB,
            long tsidP95Ns, long tsidP99Ns,
            long uuid4P95Ns, long uuid4P99Ns,
            long uuid7P95Ns, long uuid7P99Ns) {}

    private static PgRow benchPostgres(int rowCount, int iteration) throws Exception {
        Random rand = new Random(42);

        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS records_tsid");
                stmt.execute("DROP TABLE IF EXISTS records_uuid4");
                stmt.execute("DROP TABLE IF EXISTS records_uuid7");
                stmt.execute("""
                    CREATE TABLE records_tsid (
                        id         BIGINT       PRIMARY KEY,
                        first_name VARCHAR(50)  NOT NULL,
                        last_name  VARCHAR(50)  NOT NULL,
                        email      VARCHAR(100) NOT NULL
                    )
                    """);
                stmt.execute("""
                    CREATE TABLE records_uuid4 (
                        id         UUID         PRIMARY KEY,
                        first_name VARCHAR(50)  NOT NULL,
                        last_name  VARCHAR(50)  NOT NULL,
                        email      VARCHAR(100) NOT NULL
                    )
                    """);
                stmt.execute("""
                    CREATE TABLE records_uuid7 (
                        id         UUID         PRIMARY KEY DEFAULT uuidv7(),
                        first_name VARCHAR(50)  NOT NULL,
                        last_name  VARCHAR(50)  NOT NULL,
                        email      VARCHAR(100) NOT NULL
                    )
                    """);
                conn.commit();
            }

            TSID.Factory factory = TSID.Factory.builder().withNode(42).build();

            long[] insertMs = new long[3];
            long[] tsidIds = new long[rowCount];
            UUID[] uuid4Ids = new UUID[rowCount];
            UUID[] uuid7Ids = new UUID[rowCount];

            // Phase index: 0 = TSID, 1 = UUIDv4, 2 = UUIDv7. Rotated per N to avoid cache-warmth bias.
            int[] order = rotate(3, iteration % 3);

            for (int phase : order) {
                switch (phase) {
                    case 0 -> insertMs[0] = pgInsertExplicit(conn, "records_tsid", rowCount,
                            (ps, i) -> {
                                tsidIds[i] = factory.generate().toLong();
                                ps.setLong(1, tsidIds[i]);
                            });
                    case 1 -> insertMs[1] = pgInsertExplicit(conn, "records_uuid4", rowCount,
                            (ps, i) -> {
                                uuid4Ids[i] = UUID.randomUUID();
                                ps.setObject(1, uuid4Ids[i]);
                            });
                    case 2 -> insertMs[2] = pgInsertUuid7(conn, rowCount, uuid7Ids);
                }
            }

            long[] tsidSize  = TsidDemo.pgStorage(conn, "records_tsid");
            long[] uuid4Size = TsidDemo.pgStorage(conn, "records_uuid4");
            long[] uuid7Size = TsidDemo.pgStorage(conn, "records_uuid7");

            // Reads in the same rotation order so cache state mirrors the insert sequence.
            long[] readP95 = new long[3];
            long[] readP99 = new long[3];
            for (int phase : order) {
                long[] reads = switch (phase) {
                    case 0 -> TsidDemo.measurePgReadsLong(conn, "records_tsid", tsidIds, READ_SAMPLES, READ_WARMUP, rand);
                    case 1 -> TsidDemo.measurePgReadsUuid(conn, "records_uuid4", uuid4Ids, READ_SAMPLES, READ_WARMUP, rand);
                    case 2 -> TsidDemo.measurePgReadsUuid(conn, "records_uuid7", uuid7Ids, READ_SAMPLES, READ_WARMUP, rand);
                    default -> throw new IllegalStateException();
                };
                long[] p = TsidDemo.percentiles(reads, 95.0, 99.0);
                readP95[phase] = p[0];
                readP99[phase] = p[1];
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE records_tsid");
                stmt.execute("DROP TABLE records_uuid4");
                stmt.execute("DROP TABLE records_uuid7");
                conn.commit();
            }

            return new PgRow(
                    rowCount,
                    insertMs[0], insertMs[1], insertMs[2],
                    tsidSize[1] / 1024, uuid4Size[1] / 1024, uuid7Size[1] / 1024,
                    tsidSize[2] / 1024, uuid4Size[2] / 1024, uuid7Size[2] / 1024,
                    readP95[0], readP99[0],
                    readP95[1], readP99[1],
                    readP95[2], readP99[2]);
        }
    }

    /** Binds (id, first_name, last_name, email) where the caller supplies the id. The 4-column INSERT shape that TSID and UUIDv4 share. */
    @FunctionalInterface
    private interface IdBinder {
        void bind(PreparedStatement ps, int i) throws SQLException;
    }

    private static long pgInsertExplicit(Connection conn, String table, int rowCount, IdBinder bindId) throws Exception {
        String sql = "INSERT INTO " + table + " (id, first_name, last_name, email) VALUES (?, ?, ?, ?)";
        long start = System.nanoTime();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < rowCount; i++) {
                bindId.bind(ps, i);
                ps.setString(2, "first_" + i);
                ps.setString(3, "last_" + i);
                ps.setString(4, "user_" + i + "@example.com");
                ps.addBatch();
                if ((i + 1) % BATCH == 0) ps.executeBatch();
            }
            ps.executeBatch();
            conn.commit();
        }
        return (System.nanoTime() - start) / 1_000_000;
    }

    private static long pgInsertUuid7(Connection conn, int rowCount, UUID[] uuid7Ids) throws Exception {
        long start = System.nanoTime();
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO records_uuid7 (first_name, last_name, email) VALUES (?, ?, ?)")) {
            for (int i = 0; i < rowCount; i++) {
                ps.setString(1, "first_" + i);
                ps.setString(2, "last_" + i);
                ps.setString(3, "user_" + i + "@example.com");
                ps.addBatch();
                if ((i + 1) % BATCH == 0) ps.executeBatch();
            }
            ps.executeBatch();
            conn.commit();
        }
        long ms = (System.nanoTime() - start) / 1_000_000;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT id FROM records_uuid7")) {
            int i = 0;
            while (rs.next()) uuid7Ids[i++] = (UUID) rs.getObject(1);
        }
        return ms;
    }

    private static void printPgStorageTable(PgRow[] rows) {
        System.out.println("Storage (index size is the primary comparison metric):");
        System.out.printf("  %-10s  %-10s  %-10s  %-10s  %-10s  %-10s  %-10s  %-7s  %-7s%n",
                "N", "TSID ins", "UUID4 ins", "UUID7 ins", "TSID idx", "UUID4 idx", "UUID7 idx", "v4/T×", "v7/T×");
        System.out.printf("  %-10s  %-10s  %-10s  %-10s  %-10s  %-10s  %-10s  %-7s  %-7s%n",
                "-", "--------", "---------", "---------", "--------", "---------", "---------", "------", "------");
        for (PgRow r : rows) {
            double v4Ratio = (double) r.uuid4IdxKB / r.tsidIdxKB;
            double v7Ratio = (double) r.uuid7IdxKB / r.tsidIdxKB;
            System.out.printf("  %,10d  %,7d ms  %,7d ms  %,7d ms  %,7d KB  %,7d KB  %,7d KB  %5.2fx  %5.2fx%n",
                    r.n, r.tsidInsertMs, r.uuid4InsertMs, r.uuid7InsertMs,
                    r.tsidIdxKB, r.uuid4IdxKB, r.uuid7IdxKB,
                    v4Ratio, v7Ratio);
        }
        System.out.println();
        printSpread("TSID idx KB",   pluck(rows, r -> r.tsidIdxKB),  AS_COUNT);
        printSpread("UUIDv4 idx KB", pluck(rows, r -> r.uuid4IdxKB), AS_COUNT);
        printSpread("UUIDv7 idx KB", pluck(rows, r -> r.uuid7IdxKB), AS_COUNT);
    }

    private static void printPgReadTable(PgRow[] rows) {
        System.out.println("Random PK reads (P95 / P99):");
        System.out.printf("  %-10s  %-9s  %-9s  %-10s  %-10s  %-10s  %-10s%n",
                "N", "TSID P95", "TSID P99", "UUID4 P95", "UUID4 P99", "UUID7 P95", "UUID7 P99");
        System.out.printf("  %-10s  %-9s  %-9s  %-10s  %-10s  %-10s  %-10s%n",
                "-", "--------", "--------", "---------", "---------", "---------", "---------");
        for (PgRow r : rows) {
            System.out.printf("  %,10d  %-9s  %-9s  %-10s  %-10s  %-10s  %-10s%n",
                    r.n,
                    TsidDemo.formatMs(r.tsidP95Ns),
                    TsidDemo.formatMs(r.tsidP99Ns),
                    TsidDemo.formatMs(r.uuid4P95Ns),
                    TsidDemo.formatMs(r.uuid4P99Ns),
                    TsidDemo.formatMs(r.uuid7P95Ns),
                    TsidDemo.formatMs(r.uuid7P99Ns));
        }
        System.out.println();
        printSpread("TSID P99",   pluck(rows, r -> r.tsidP99Ns),  TsidDemo::formatMs);
        printSpread("UUIDv4 P99", pluck(rows, r -> r.uuid4P99Ns), TsidDemo::formatMs);
        printSpread("UUIDv7 P99", pluck(rows, r -> r.uuid7P99Ns), TsidDemo::formatMs);
    }

    // ============================================================================
    // MongoDB
    // ============================================================================

    record MongoRow(
            int n,
            long tsidInsertMs, long uuid4InsertMs, long uuid7InsertMs, long objIdInsertMs,
            long tsidIdxKB,    long uuid4IdxKB,    long uuid7IdxKB,    long objIdIdxKB,
            long tsidP95Ns, long tsidP99Ns,
            long uuid4P95Ns, long uuid4P99Ns,
            long uuid7P95Ns, long uuid7P99Ns,
            long objIdP95Ns, long objIdP99Ns) {}

    private static MongoRow benchMongo(int count, int iteration) {
        Random rand = new Random(42);

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO_URI))
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .build();

        try (MongoClient client = MongoClients.create(settings)) {
            MongoDatabase db = client.getDatabase(MONGO_DB);
            MongoCollection<Document> tsidColl  = db.getCollection("records_tsid");
            MongoCollection<Document> uuid4Coll = db.getCollection("records_uuid4");
            MongoCollection<Document> uuid7Coll = db.getCollection("records_uuid7");
            MongoCollection<Document> objIdColl = db.getCollection("records_objectid");
            tsidColl.drop();
            uuid4Coll.drop();
            uuid7Coll.drop();
            objIdColl.drop();

            TSID.Factory factory = TSID.Factory.builder().withNode(42).build();

            long[] insertMs = new long[4];
            Long[] tsidIds = new Long[count];
            UUID[] uuid4Ids = new UUID[count];
            UUID[] uuid7Ids = new UUID[count];
            ObjectId[] objIds = new ObjectId[count];
            List<Document> buf = new ArrayList<>(BATCH);

            // Phase index: 0 = TSID, 1 = UUIDv4, 2 = UUIDv7, 3 = ObjectId. Rotated per N.
            int[] order = rotate(4, iteration % 4);

            for (int phase : order) {
                switch (phase) {
                    case 0 -> insertMs[0] = mongoInsert(tsidColl, count, tsidIds,
                            i -> factory.generate().toLong(), buf);
                    case 1 -> insertMs[1] = mongoInsert(uuid4Coll, count, uuid4Ids,
                            i -> UUID.randomUUID(), buf);
                    case 2 -> insertMs[2] = mongoInsert(uuid7Coll, count, uuid7Ids,
                            i -> UUID.ofEpochMillis(System.currentTimeMillis()), buf);
                    case 3 -> insertMs[3] = mongoInsert(objIdColl, count, objIds,
                            i -> new ObjectId(), buf);
                }
            }

            // Force WiredTiger checkpoint so collStats reflects all writes.
            client.getDatabase("admin").runCommand(new Document("fsync", 1));

            long[] idxBytes = new long[4];
            idxBytes[0] = mongoIndexBytes(db, "records_tsid");
            idxBytes[1] = mongoIndexBytes(db, "records_uuid4");
            idxBytes[2] = mongoIndexBytes(db, "records_uuid7");
            idxBytes[3] = mongoIndexBytes(db, "records_objectid");

            long[] readP95 = new long[4];
            long[] readP99 = new long[4];
            for (int phase : order) {
                long[] reads = switch (phase) {
                    case 0 -> TsidDemo.measureMongoReads(tsidColl, tsidIds, READ_SAMPLES, READ_WARMUP, rand);
                    case 1 -> TsidDemo.measureMongoReads(uuid4Coll, uuid4Ids, READ_SAMPLES, READ_WARMUP, rand);
                    case 2 -> TsidDemo.measureMongoReads(uuid7Coll, uuid7Ids, READ_SAMPLES, READ_WARMUP, rand);
                    case 3 -> TsidDemo.measureMongoReads(objIdColl, objIds,  READ_SAMPLES, READ_WARMUP, rand);
                    default -> throw new IllegalStateException();
                };
                long[] p = TsidDemo.percentiles(reads, 95.0, 99.0);
                readP95[phase] = p[0];
                readP99[phase] = p[1];
            }

            tsidColl.drop();
            uuid4Coll.drop();
            uuid7Coll.drop();
            objIdColl.drop();

            return new MongoRow(
                    count,
                    insertMs[0], insertMs[1], insertMs[2], insertMs[3],
                    idxBytes[0] / 1024, idxBytes[1] / 1024, idxBytes[2] / 1024, idxBytes[3] / 1024,
                    readP95[0], readP99[0],
                    readP95[1], readP99[1],
                    readP95[2], readP99[2],
                    readP95[3], readP99[3]);
        }
    }

    private static <T> long mongoInsert(MongoCollection<Document> coll, int count,
                                        T[] ids, IntFunction<T> nextId, List<Document> buf) {
        buf.clear();
        long start = System.nanoTime();
        for (int i = 0; i < count; i++) {
            ids[i] = nextId.apply(i);
            buf.add(orderRecord(ids[i], i));
            if (buf.size() == BATCH) { coll.insertMany(buf); buf.clear(); }
        }
        if (!buf.isEmpty()) coll.insertMany(buf);
        return (System.nanoTime() - start) / 1_000_000;
    }

    private static Document orderRecord(Object id, int i) {
        return new Document("_id", id)
                .append("first_name", "first_" + i)
                .append("last_name", "last_" + i)
                .append("email", "user_" + i + "@example.com");
    }

    private static long mongoIndexBytes(MongoDatabase db, String coll) {
        Document stats = db.runCommand(new Document("collStats", coll));
        return ((Number) stats.get("totalIndexSize")).longValue();
    }

    private static void printMongoStorageTable(MongoRow[] rows) {
        System.out.println("Storage (totalIndexSize is the primary comparison metric):");
        System.out.printf("  %-10s  %-9s  %-9s  %-9s  %-9s  %-9s  %-9s  %-9s  %-9s  %-7s  %-7s  %-7s%n",
                "N", "TSID ins", "v4 ins", "v7 ins", "OID ins", "TSID idx", "v4 idx", "v7 idx", "OID idx", "v4/T×", "v7/T×", "OID/T×");
        System.out.printf("  %-10s  %-9s  %-9s  %-9s  %-9s  %-9s  %-9s  %-9s  %-9s  %-7s  %-7s  %-7s%n",
                "-", "--------", "------", "------", "-------", "--------", "------", "------", "-------", "------", "------", "------");
        for (MongoRow r : rows) {
            double v4   = (double) r.uuid4IdxKB / r.tsidIdxKB;
            double v7   = (double) r.uuid7IdxKB / r.tsidIdxKB;
            double oid  = (double) r.objIdIdxKB / r.tsidIdxKB;
            System.out.printf("  %,10d  %,6d ms  %,3d ms  %,3d ms  %,4d ms  %,6d KB  %,3d KB  %,3d KB  %,4d KB  %5.2fx  %5.2fx  %5.2fx%n",
                    r.n,
                    r.tsidInsertMs, r.uuid4InsertMs, r.uuid7InsertMs, r.objIdInsertMs,
                    r.tsidIdxKB, r.uuid4IdxKB, r.uuid7IdxKB, r.objIdIdxKB,
                    v4, v7, oid);
        }
        System.out.println();
        printSpread("TSID idx KB",     pluck(rows, r -> r.tsidIdxKB),  AS_COUNT);
        printSpread("UUIDv4 idx KB",   pluck(rows, r -> r.uuid4IdxKB), AS_COUNT);
        printSpread("UUIDv7 idx KB",   pluck(rows, r -> r.uuid7IdxKB), AS_COUNT);
        printSpread("ObjectId idx KB", pluck(rows, r -> r.objIdIdxKB), AS_COUNT);
    }

    private static void printMongoReadTable(MongoRow[] rows) {
        System.out.println("Random _id reads (P95 / P99):");
        System.out.printf("  %-10s  %-9s  %-9s  %-9s  %-9s  %-9s  %-9s  %-9s  %-9s%n",
                "N", "TSID P95", "TSID P99", "v4 P95", "v4 P99", "v7 P95", "v7 P99", "OID P95", "OID P99");
        System.out.printf("  %-10s  %-9s  %-9s  %-9s  %-9s  %-9s  %-9s  %-9s  %-9s%n",
                "-", "--------", "--------", "------", "------", "------", "------", "-------", "-------");
        for (MongoRow r : rows) {
            System.out.printf("  %,10d  %-9s  %-9s  %-9s  %-9s  %-9s  %-9s  %-9s  %-9s%n",
                    r.n,
                    TsidDemo.formatMs(r.tsidP95Ns),  TsidDemo.formatMs(r.tsidP99Ns),
                    TsidDemo.formatMs(r.uuid4P95Ns), TsidDemo.formatMs(r.uuid4P99Ns),
                    TsidDemo.formatMs(r.uuid7P95Ns), TsidDemo.formatMs(r.uuid7P99Ns),
                    TsidDemo.formatMs(r.objIdP95Ns), TsidDemo.formatMs(r.objIdP99Ns));
        }
        System.out.println();
        printSpread("TSID P99",     pluck(rows, r -> r.tsidP99Ns),  TsidDemo::formatMs);
        printSpread("UUIDv4 P99",   pluck(rows, r -> r.uuid4P99Ns), TsidDemo::formatMs);
        printSpread("UUIDv7 P99",   pluck(rows, r -> r.uuid7P99Ns), TsidDemo::formatMs);
        printSpread("ObjectId P99", pluck(rows, r -> r.objIdP99Ns), TsidDemo::formatMs);
    }

    // ============================================================================
    // Helpers
    // ============================================================================

    private static <T> long[] pluck(T[] rows, ToLongFunction<T> p) {
        long[] out = new long[rows.length];
        for (int i = 0; i < rows.length; i++) out[i] = p.applyAsLong(rows[i]);
        return out;
    }

    /** {@code rotate(3, 1)} → {@code [1, 2, 0]}. {@code rotate(4, 2)} → {@code [2, 3, 0, 1]}. */
    private static int[] rotate(int n, int start) {
        int[] r = new int[n];
        for (int i = 0; i < n; i++) r[i] = (start + i) % n;
        return r;
    }

    private static final LongFunction<String> AS_COUNT = v -> String.format("%,8d", v);

    private static void printSpread(String label, long[] values, LongFunction<String> fmt) {
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        System.out.printf("  %-18s  min %s   median %s   max %s%n",
                label, fmt.apply(sorted[0]), fmt.apply(sorted[sorted.length / 2]), fmt.apply(sorted[sorted.length - 1]));
    }

    private static void banner(String text) {
        String bar = "═".repeat(Math.max(50, text.length() + 4));
        System.out.println();
        System.out.println(bar);
        System.out.println("  " + text);
        System.out.println(bar);
    }

    private static void header(String text) {
        System.out.println();
        System.out.println("─── " + text + " " + "─".repeat(Math.max(3, 80 - text.length() - 5)));
    }

    private TsidScalingBench() {}
}
