package com.example.tsid;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import io.hypersistence.tsid.TSID;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.types.ObjectId;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 *
 * Live demo for TSID primary keys in Postgres and MongoDB. Insert + storage
 * comparisons run at N = 100,000; read percentiles come from TsidScalingBench
 * (N = 100K → 1M, 100,000 samples per N) and are summarized in the README.
 *
 * One main(), twelve sections:
 *   1.  TSID — 8 bytes, time-sortable, fits in BIGINT
 *   2.  Human form — short, prefix-friendly, reversible (vs UUID/ObjectId)
 *   3.  Sequential B-tree inserts — pages append at the right edge
 *   4.  Why TSID is Base32 on the wire — the JSON 64-bit trap
 *   5.  Time-range queries on the TSID primary key (Postgres)
 *   6.  Cursor pagination on the TSID primary key (keyset, no OFFSET)
 *   7.  Postgres insert + storage comparison — TSID vs UUIDv7
 *   8.  MongoDB time-range queries on the TSID _id
 *   9.  MongoDB insert + storage comparison — TSID vs ObjectId vs UUIDv7
 *  10.  Idempotent event ingestion via SHA-256 content hash
 *  11.  Concurrent id-generation throughput — TSID vs UUID.randomUUID()
 *  12.  Node ID resolution on Kubernetes: from pod name to withNode(N)
 *
 * Every section drops and recreates its tables or collections. No pre-seeded state.
 *
 * Stack:
 *   - Java 26 (UUID.ofEpochMillis for UUIDv7 generation in section 9)
 *   - Postgres 18 (native uuidv7() function as a column DEFAULT in section 7)
 *   - MongoDB 8.2 (WiredTiger with Snappy compression)
 *
 * Prerequisites: ./infra/start.sh brings the containers up locally.
 */
public final class TsidDemo {

    private static final String PG_URL  = "jdbc:postgresql://localhost:5432/tsiddemo";
    private static final String PG_USER = "demouser";
    private static final String PG_PASS = "demopass";

    private static final String MONGO_URI = "mongodb://demouser:demopass@localhost:27017/?authSource=admin";
    private static final String MONGO_DB  = "tsiddemo";

    private static final DateTimeFormatter TS_UTC =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter TS_LOCAL =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    /** Order lifecycle states, rotated as {@code STATUSES[i % STATUSES.length]} so SELECT * looks varied. */
    private static final String[] STATUSES = {
            "pending", "paid", "shipped", "in_transit", "delivered", "returned"
    };

    /** Set {@code PAUSE=1} to halt before each section drops its tables/collections — useful for showing rows live during a demo. */
    private static final boolean PAUSE_BEFORE_DROP = "1".equals(System.getenv("PAUSE"));

    private static final Scanner STDIN = new Scanner(System.in);

    public static void main(String[] args) throws Exception {
        banner("TSID demo — primary keys in Postgres and MongoDB");
        cleanSlate();

        section1_basicLayout();
        section2_humanIdDerivation();
        section3_withinMillisecondBatch();
        section4_serializationBoundary();
        section5_postgresRoundTrip();
        section6_cursorPagination();
        section7_postgresComparison();
        section8_mongoRoundTrip();
        section9_mongoComparison();
        section10_contentHash();
        section11_concurrentGeneration();
        section12_nodeIdOnEks();

        banner("Demo complete");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 1. TSID layout — 8 bytes, time embedded
    // ─────────────────────────────────────────────────────────────────────────
    private static void section1_basicLayout() {
        header("1. TSID — 8 bytes, time-sortable, fits in BIGINT");
        say("Layout: 42-bit ms timestamp | 10-bit node | 12-bit counter.");
        say("Counter range: 0–4095 within a single ms per node — 4,096 ids/ms ceiling.");
        say("Storage: BIGINT in Postgres, NumberLong in MongoDB.");

        TSID tsid = TSID.fast();
        Instant created = tsid.getInstant();

        System.out.println();
        System.out.println("Same id, three views:");
        printf("  Long       %d%n", tsid.toLong());
        printf("  Base32     %s   (13 chars on the wire)%n", tsid);
        printf("  Created    %s UTC   /   %s %s   (decoded from the long)%n",
                TS_UTC.format(created), TS_LOCAL.format(created), ZoneId.systemDefault().getId());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 2. Human form — prefix-friendly, reversible, much shorter than UUID/ObjectId
    // ─────────────────────────────────────────────────────────────────────────
    private static void section2_humanIdDerivation() {
        header("2. Human form — short, prefix-friendly, reversible");
        say("TSID.format(\"ORD-%S\") prefixes the 13-char Base32 string.");
        say("TSID.unformat() decodes the prefixed string back to the long.");
        say("Same value in storage, on the wire, in the logs — no separate display id.");

        TSID tsid = TSID.fast();
        long longId = tsid.toLong();
        String prefixed = tsid.format("ORD-%S");

        // Correctness: the round-trip preserves the long. Throw if it doesn't; don't print a boolean.
        if (TSID.unformat(prefixed, "ORD-%S").toLong() != longId) {
            throw new AssertionError("Human identifier round-trip failed.");
        }

        System.out.println();
        System.out.println("Same id, three forms:");
        printf("  Long  (storage):    %d%n", longId);
        printf("  Base32 (wire):      %s%n", tsid);
        printf("  Prefixed (UI/URL):  %s%n", prefixed);

        // Prefixed form across id types. This is the external representation shown
        // in UIs, support tools, and logs. UUIDs shown as 32-char hex
        // (URL form); canonical hyphenated form adds 4 chars.
        String uuid4Hex = UUID.randomUUID().toString().replace("-", "");
        String uuid7Hex = UUID.ofEpochMillis(System.currentTimeMillis()).toString().replace("-", "");
        String objectIdHex = new ObjectId().toHexString();
        String prefTsid     = prefixed;
        String prefUuid4    = "ORD-" + uuid4Hex;
        String prefUuid7    = "ORD-" + uuid7Hex;
        String prefObjectId = "ORD-" + objectIdHex;
        System.out.println();
        System.out.println("Same prefixed id in other id types — external representation shown in UIs, support tools, and logs:");
        printf("  TSID:      %s   (%d chars)%n", prefTsid,     prefTsid.length());
        printf("  UUIDv4:    %s   (%d chars)%n", prefUuid4,    prefUuid4.length());
        printf("  UUIDv7:    %s   (%d chars)%n", prefUuid7,    prefUuid7.length());
        printf("  ObjectId:  %s   (%d chars)%n", prefObjectId, prefObjectId.length());
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 3. Sequential B-tree inserts
    // ─────────────────────────────────────────────────────────────────────────
    private static void section3_withinMillisecondBatch() throws InterruptedException {
        header("3. Sequential B-tree inserts — pages append at the right edge");
        say("Each new TSID is greater than the last (per node).");
        say("Sequential keys concentrate inserts on the right edge of the B-tree,");
        say("reducing random-page churn compared with random keys.");

        int count = 12;
        TSID.Factory factory = TSID.Factory.builder().withNode(42).build();

        long[] longs = new long[count];
        String[] strings = new String[count];
        long[] timestamps = new long[count];
        for (int i = 0; i < count; i++) {
            if (i == count / 2) Thread.sleep(1);   // guarantee one visible ms boundary
            TSID t = factory.generate();
            longs[i] = t.toLong();
            strings[i] = t.toString();
            timestamps[i] = t.getInstant().toEpochMilli();
        }

        System.out.println();
        printf("%-22s  %-15s  %-14s%n", "Long", "Base32", "Timestamp (ms)");
        printf("%-22s  %-15s  %-14s%n", "----", "------", "--------------");
        long prevMs = timestamps[0];
        for (int i = 0; i < count; i++) {
            String marker = (i > 0 && timestamps[i] != prevMs) ? "  <- new millisecond" : "";
            printf("%-22d  %-15s  %-14d%s%n", longs[i], strings[i], timestamps[i], marker);
            prevMs = timestamps[i];
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 4. Why TSID is Base32 on the wire — the JSON 64-bit trap
    // ─────────────────────────────────────────────────────────────────────────
    private static void section4_serializationBoundary() throws IOException {
        header("4. Why TSID is Base32 on the wire — the JSON 64-bit trap");
        say("64-bit longs are unsafe in JSON: JavaScript's Number is IEEE 754 double, only 53-bit safe.");
        say("TSIDs exceed 2^53 — sending the long directly can lose precision when parsed as a JavaScript Number.");
        say("Send the 13-char Base32 string instead; convert to/from the long at the API boundary.");

        long storedId = TSID.fast().toLong();
        long throughDouble = (long) (double) storedId;     // simulates what a JS Number sees
        String wireId = TSID.from(storedId).toString();

        System.out.println();
        System.out.println("The precision trap (don't put a 64-bit long in JSON):");
        printf("  TSID as long (Java/Postgres BIGINT/Mongo NumberLong):  %d%n", storedId);
        printf("  Same value through IEEE 754 double (a JS Number):     %d%n", throughDouble);
        printf("  Difference:                                            %d   (lost in the bottom bits)%n",
                storedId - throughDouble);

        System.out.println();
        System.out.println("Solution — convert at the API boundary, send a string:");
        printf("  Outbound  (storage long → wire string):  TSID.from(%dL).toString()%n", storedId);
        printf("                                        →  \"%s\"%n", wireId);
        printf("  Inbound   (wire string → storage long):  TSID.from(\"%s\").toLong()%n", wireId);
        printf("                                        →  %dL%n", TSID.from(wireId).toLong());

        // Round-trip a DTO through Jackson to prove wire/storage symmetry — no custom module needed,
        // the DTO just carries `id` as a String.
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        Note outgoing = new Note(wireId, "Week 17 status update", Instant.now());
        String json = mapper.writeValueAsString(outgoing);
        Note incoming = mapper.readValue(json, Note.class);
        if (TSID.from(incoming.id()).toLong() != storedId) {
            throw new AssertionError("Round-trip failed.");
        }

        System.out.println();
        System.out.println("Outgoing JSON DTO (`id` is the Base32 string and does not require 64-bit integer support):");
        System.out.println(json);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 5. Time-range queries on the TSID primary key
    // ─────────────────────────────────────────────────────────────────────────
    private static void section5_postgresRoundTrip() throws Exception {
        header("5. Time-range queries on the TSID primary key");
        say("\"Find orders between two timestamps\" — convert each timestamp to a TSID, query the PK directly.");
        say("No created_at column, no second index. The PK B-tree already orders by time.");
        say("A BIGSERIAL key does not encode wall-clock time; a created_at column would need its own B-tree index.");

        TSID.Factory factory = TSID.Factory.builder().withNode(42).build();
        long[] ids = new long[500];
        for (int i = 0; i < ids.length; i++) {
            if (i > 0 && i % 100 == 0) Thread.sleep(1);
            ids[i] = factory.generate().toLong();
        }

        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS orders");
                stmt.execute("""
                    CREATE TABLE orders (
                        order_id      BIGINT       PRIMARY KEY,
                        order_number  VARCHAR(20)  NOT NULL UNIQUE,
                        item_count    INT          NOT NULL,
                        total_cents   BIGINT       NOT NULL,
                        status        VARCHAR(20)  NOT NULL
                    )
                    """);
                conn.commit();
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO orders (order_id, order_number, item_count, total_cents, status) "
                  + "VALUES (?, ?, ?, ?, ?)")) {
                for (int i = 0; i < ids.length; i++) {
                    long id = ids[i];
                    ps.setLong(1, id);
                    ps.setString(2, TSID.from(id).format("ORD-%S"));
                    ps.setInt(3, 1 + (i * 7) % 8);
                    ps.setLong(4, 1_999 + (i * 137L) % 50_000);
                    ps.setString(5, STATUSES[i % STATUSES.length]);
                    ps.addBatch();
                }
                ps.executeBatch();
                conn.commit();
            }

            // The "user" wants orders in a wall-clock window. In production those bounds
            // come from request params; here we derive them from the dataset so the count
            // is non-zero in the demo.
            Instant windowStart = TSID.from(ids[0]).getInstant();
            Instant windowEnd   = TSID.from(ids[ids.length / 2]).getInstant();
            long lowerBound = tsidStartOfMs(windowStart);
            long upperBound = tsidStartOfMs(windowEnd);

            long count;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT count(*) FROM orders WHERE order_id >= ? AND order_id < ?")) {
                ps.setLong(1, lowerBound);
                ps.setLong(2, upperBound);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    count = rs.getLong(1);
                }
            }

            Instant lastInserted = TSID.from(ids[ids.length - 1]).getInstant();
            System.out.println();
            printf("Inserted %,d orders, spread across %s to %s UTC.%n",
                    ids.length, TS_UTC.format(windowStart), TS_UTC.format(lastInserted));
            System.out.println();
            printf("Example query: \"How many orders between %s and %s UTC?\"%n",
                    TS_UTC.format(windowStart), TS_UTC.format(windowEnd));
            System.out.println();
            System.out.println("Step 1 — convert each wall-clock bound to a TSID (start-of-ms, counter=0):");
            printf("  %s UTC  ->  %d%n", TS_UTC.format(windowStart), lowerBound);
            printf("  %s UTC  ->  %d%n", TS_UTC.format(windowEnd),   upperBound);
            System.out.println();
            System.out.println("Step 2 — query the PK with those TSID bounds:");
            System.out.println("  SELECT count(*) FROM orders WHERE order_id >= ? AND order_id < ?");
            printf("  -> %d orders%n", count);

            try (PreparedStatement ps = conn.prepareStatement(
                    "EXPLAIN (COSTS OFF) SELECT count(*) FROM orders WHERE order_id >= ? AND order_id < ?")) {
                ps.setLong(1, lowerBound);
                ps.setLong(2, upperBound);
                try (ResultSet rs = ps.executeQuery()) {
                    System.out.println();
                    System.out.println("Step 3 — query plan (orders_pkey alone, no created_at index):");
                    while (rs.next()) {
                        System.out.println("  " + rs.getString(1));
                    }
                }
            }

            pauseBeforeDrop("psql -h localhost -U demouser -d tsiddemo  →  EXPLAIN SELECT * FROM orders WHERE order_id BETWEEN ? AND ?;");
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS orders");
                conn.commit();
            }
        }
    }

    /** TSID epoch is 2020-01-01T00:00:00Z. Bit layout: 42-bit ms | 10-bit node | 12-bit counter. */
    private static final long TSID_EPOCH_MS = 1_577_836_800_000L;

    /** Smallest TSID for the millisecond containing {@code t}: counter=0, node=0. Useful as a half-open lower bound. */
    private static long tsidStartOfMs(Instant t) {
        return (t.toEpochMilli() - TSID_EPOCH_MS) << 22;
    }


    // ─────────────────────────────────────────────────────────────────────────
    // 6. Cursor pagination on the TSID primary key
    // ─────────────────────────────────────────────────────────────────────────
    private static void section6_cursorPagination() throws Exception {
        header("6. Cursor pagination on the TSID primary key");
        say("PK is monotonic-per-node — ORDER BY order_id alone is a stable cursor.");
        say("No composite (created_at, id) keyset, no OFFSET, no second index.");
        say("Across multi-node generation, ties within the same ms break deterministically by node ID.");

        final int rowCount     = 5_000;
        final int pageSize     = 5;
        final int pagesToShow  = 3;

        TSID.Factory factory = TSID.Factory.builder().withNode(42).build();

        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS orders");
                stmt.execute("""
                    CREATE TABLE orders (
                        order_id      BIGINT       PRIMARY KEY,
                        order_number  VARCHAR(20)  NOT NULL UNIQUE,
                        item_count    INT          NOT NULL,
                        total_cents   BIGINT       NOT NULL,
                        status        VARCHAR(20)  NOT NULL
                    )
                    """);
                conn.commit();
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO orders (order_id, order_number, item_count, total_cents, status) "
                  + "VALUES (?, ?, ?, ?, ?)")) {
                for (int i = 0; i < rowCount; i++) {
                    long id = factory.generate().toLong();
                    ps.setLong(1, id);
                    ps.setString(2, TSID.from(id).format("ORD-%S"));
                    ps.setInt(3, 1 + (i * 7) % 8);
                    ps.setLong(4, 1_999 + (i * 137L) % 50_000);
                    ps.setString(5, STATUSES[i % STATUSES.length]);
                    ps.addBatch();
                    if ((i + 1) % 1_000 == 0) ps.executeBatch();
                }
                ps.executeBatch();
                conn.commit();
            }

            // Keyset paginate: cursor = last order_id of previous page.
            String sql = "SELECT order_id, order_number FROM orders WHERE order_id > ? ORDER BY order_id LIMIT ?";
            long cursor = 0L;
            String[] nextCursorBase32 = new String[pagesToShow];

            System.out.println();
            printf("Rows: %,d   Page size: %d%n", rowCount, pageSize);
            System.out.println();
            printf("  %-4s  %-15s  %-15s  %-4s%n", "Page", "First id", "Last id (cursor)", "Rows");
            printf("  %-4s  %-15s  %-15s  %-4s%n", "----", "--------", "----------------", "----");

            for (int page = 1; page <= pagesToShow; page++) {
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setLong(1, cursor);
                    ps.setInt(2, pageSize);
                    try (ResultSet rs = ps.executeQuery()) {
                        long first = -1, last = -1;
                        int rows = 0;
                        while (rs.next()) {
                            long id = rs.getLong(1);
                            if (first == -1) first = id;
                            last = id;
                            rows++;
                        }
                        if (rows == 0) {
                            printf("  %-4d  %-15s  %-15s  %-4d%n", page, "—", "—", 0);
                            nextCursorBase32[page - 1] = null;
                        } else {
                            String lastBase32 = TSID.from(last).toString();
                            printf("  %-4d  %-15s  %-15s  %-4d%n",
                                    page, TSID.from(first), lastBase32, rows);
                            cursor = last;
                            nextCursorBase32[page - 1] = lastBase32;
                        }
                    }
                }
            }

            System.out.println();
            System.out.println("As REST requests (cursor = last order_id from the previous response):");
            System.out.printf("  GET /orders?limit=%d%n", pageSize);
            for (int i = 0; i < pagesToShow - 1; i++) {
                if (nextCursorBase32[i] != null) {
                    System.out.printf("  GET /orders?cursor=%s&limit=%d%n",
                            nextCursorBase32[i], pageSize);
                }
            }
            System.out.println();
            System.out.println("Server SQL: SELECT order_id, order_number, ... FROM orders "
                             + "WHERE order_id > :cursor ORDER BY order_id LIMIT :limit");

            pauseBeforeDrop("psql -h localhost -U demouser -d tsiddemo  →  SELECT order_id, order_number, status FROM orders ORDER BY order_id LIMIT 10;");
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS orders");
                conn.commit();
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 7. Postgres insert + storage comparison — TSID vs UUIDv7
    // ─────────────────────────────────────────────────────────────────────────
    private static void section7_postgresComparison() throws Exception {
        header("7. Postgres insert + storage comparison — TSID vs UUIDv7");
        say("8-byte BIGINT vs 16-byte UUID PK, same row payload.");
        say("UUIDv7 generated server-side via Postgres 18's native uuidv7() DEFAULT.");
        say("Read percentiles need >100K samples to stabilize; see TsidScalingBench output or benchmark.md.");

        final int rowCount = 100_000;
        final int batch    = 1_000;

        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            conn.setAutoCommit(false);

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS orders_tsid");
                stmt.execute("DROP TABLE IF EXISTS orders_uuid7");
                stmt.execute("""
                    CREATE TABLE orders_tsid (
                        order_id      BIGINT       PRIMARY KEY,
                        order_number  VARCHAR(20)  NOT NULL UNIQUE,
                        item_count    INT          NOT NULL,
                        total_cents   BIGINT       NOT NULL,
                        status        VARCHAR(20)  NOT NULL
                    )
                    """);
                stmt.execute("""
                    CREATE TABLE orders_uuid7 (
                        order_id      UUID         PRIMARY KEY DEFAULT uuidv7(),
                        order_number  VARCHAR(20)  NOT NULL UNIQUE,
                        item_count    INT          NOT NULL,
                        total_cents   BIGINT       NOT NULL,
                        status        VARCHAR(20)  NOT NULL
                    )
                    """);
                conn.commit();
            }

            // TSID: generated app-side, inserted explicitly.
            TSID.Factory factory = TSID.Factory.builder().withNode(42).build();
            long tsidStart = System.nanoTime();
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO orders_tsid (order_id, order_number, item_count, total_cents, status) "
                  + "VALUES (?, ?, ?, ?, ?)")) {
                for (int i = 0; i < rowCount; i++) {
                    ps.setLong(1, factory.generate().toLong());
                    ps.setString(2, String.format("ORD-2026-%06d", i));
                    ps.setInt(3, 1 + (i * 7) % 8);
                    ps.setLong(4, 1_999 + (i * 137L) % 50_000);
                    ps.setString(5, STATUSES[i % STATUSES.length]);
                    ps.addBatch();
                    if ((i + 1) % batch == 0) ps.executeBatch();
                }
                ps.executeBatch();
                conn.commit();
            }
            long tsidInsertMs = (System.nanoTime() - tsidStart) / 1_000_000;

            // UUIDv7: generated server-side by Postgres 18's native uuidv7() DEFAULT.
            long uuid7Start = System.nanoTime();
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO orders_uuid7 (order_number, item_count, total_cents, status) "
                  + "VALUES (?, ?, ?, ?)")) {
                for (int i = 0; i < rowCount; i++) {
                    ps.setString(1, String.format("ORD-2026-%06d", i));
                    ps.setInt(2, 1 + (i * 7) % 8);
                    ps.setLong(3, 1_999 + (i * 137L) % 50_000);
                    ps.setString(4, STATUSES[i % STATUSES.length]);
                    ps.addBatch();
                    if ((i + 1) % batch == 0) ps.executeBatch();
                }
                ps.executeBatch();
                conn.commit();
            }
            long uuid7InsertMs = (System.nanoTime() - uuid7Start) / 1_000_000;

            long[] tsidSize  = pgStorage(conn, "orders_tsid");
            long[] uuid7Size = pgStorage(conn, "orders_uuid7");
            long tsidPkIdx   = pgIndexSize(conn, "orders_tsid_pkey");
            long uuid7PkIdx  = pgIndexSize(conn, "orders_uuid7_pkey");

            System.out.println();
            printf("Rows per table:  %,d%n", rowCount);
            System.out.println();
            System.out.println("Storage:");
            printf("  %-18s  %-12s  %-12s  %-12s  %-12s  %-12s%n",
                    "ID type", "Insert time", "Data size", "PK idx", "Total idx", "Total size");
            printf("  %-18s  %-12s  %-12s  %-12s  %-12s  %-12s%n",
                    "-------", "-----------", "---------", "------", "---------", "----------");
            printf("  %-18s  %,9d ms  %,9d KB  %,9d KB  %,9d KB  %,9d KB%n",
                    "TSID (BIGINT)", tsidInsertMs, tsidSize[0] / 1024,
                    tsidPkIdx / 1024, tsidSize[1] / 1024, tsidSize[2] / 1024);
            printf("  %-18s  %,9d ms  %,9d KB  %,9d KB  %,9d KB  %,9d KB%n",
                    "UUIDv7 (UUID)", uuid7InsertMs, uuid7Size[0] / 1024,
                    uuid7PkIdx / 1024, uuid7Size[1] / 1024, uuid7Size[2] / 1024);
            System.out.println();
            System.out.println("Index ratio (UUIDv7 / TSID):");
            printf("  PK only:    %.2fx   (16B UUID vs 8B BIGINT key)%n",
                    (double) uuid7PkIdx / tsidPkIdx);
            printf("  All idxs:   %.2fx   (total includes the order_number unique index, same size on both sides)%n",
                    (double) uuid7Size[1] / tsidSize[1]);
            printf("Total size ratio (UUIDv7 / TSID):  %.2fx%n",
                    (double) uuid7Size[2] / tsidSize[2]);

            pauseBeforeDrop("psql -h localhost -U demouser -d tsiddemo  →  \\dt+ orders_*  /  SELECT * FROM orders_tsid LIMIT 5;");
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS orders_tsid");
                stmt.execute("DROP TABLE IF EXISTS orders_uuid7");
                conn.commit();
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 8. MongoDB round-trip — TSID as NumberLong _id
    // ─────────────────────────────────────────────────────────────────────────
    private static void section8_mongoRoundTrip() throws InterruptedException {
        header("8. MongoDB time-range queries on the TSID _id");
        say("Same workflow as §5, on Mongo: convert wall-clock bounds to TSIDs, query the _id index directly.");
        say("_id is a NumberLong (8 bytes). Time-extractable: TSID.from(_id).getInstant().");
        say("No created_at field, no second index — the _id index already orders by time.");

        TSID.Factory factory = TSID.Factory.builder().withNode(42).build();
        long[] ids = new long[500];
        for (int i = 0; i < ids.length; i++) {
            if (i > 0 && i % 100 == 0) Thread.sleep(1);
            ids[i] = factory.generate().toLong();
        }

        try (MongoClient client = MongoClients.create(MONGO_URI)) {
            MongoDatabase db = client.getDatabase(MONGO_DB);
            MongoCollection<Document> coll = db.getCollection("orders");
            coll.drop();

            for (int i = 0; i < ids.length; i++) {
                long id = ids[i];
                Document doc = new Document("_id", id)
                        .append("order_number", TSID.from(id).format("ORD-%S"))
                        .append("item_count", 1 + (i * 7) % 8)
                        .append("total_cents", 1_999L + (i * 137L) % 50_000)
                        .append("status", STATUSES[i % STATUSES.length]);
                coll.insertOne(doc);
            }

            // Pick a wall-clock window from the dataset (in production, request params).
            Instant windowStart = TSID.from(ids[0]).getInstant();
            Instant windowEnd   = TSID.from(ids[ids.length / 2]).getInstant();
            long lowerBound = tsidStartOfMs(windowStart);
            long upperBound = tsidStartOfMs(windowEnd);

            long count = coll.countDocuments(
                    new Document("_id", new Document("$gte", lowerBound).append("$lt", upperBound)));

            System.out.println();
            printf("Inserted %,d documents.%n", coll.countDocuments());
            System.out.println();
            printf("Example query: \"How many orders between %s and %s UTC?\"%n",
                    TS_UTC.format(windowStart), TS_UTC.format(windowEnd));
            System.out.println();
            System.out.println("Step 1 — convert each wall-clock bound to a TSID (start-of-ms, counter=0):");
            printf("  %s UTC  ->  %d%n", TS_UTC.format(windowStart), lowerBound);
            printf("  %s UTC  ->  %d%n", TS_UTC.format(windowEnd),   upperBound);
            System.out.println();
            System.out.println("Step 2 — query the _id index with those bounds (NumberLong(...) wraps the long for mongosh, see §4):");
            printf("  db.orders.countDocuments({ _id: { $gte: NumberLong(\"%d\"), $lt: NumberLong(\"%d\") } })%n",
                    lowerBound, upperBound);
            printf("  -> %d documents%n", count);

            System.out.println();
            System.out.println("One document as BSON (note _id is NumberLong, 8 bytes):");
            System.out.println("  " + coll.find().first().toJson());

            pauseBeforeDrop("mongosh \"mongodb://demouser:demopass@localhost:27017/tsiddemo?authSource=admin\"  →  db.orders.find({ _id: { $gte: NumberLong(\"...\"), $lt: NumberLong(\"...\") } })");
            coll.drop();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 9. MongoDB insert + storage comparison — TSID vs ObjectId vs UUIDv7
    // ─────────────────────────────────────────────────────────────────────────
    private static void section9_mongoComparison() {
        header("9. MongoDB insert + storage comparison — TSID vs ObjectId vs UUIDv7");
        say("8-byte NumberLong vs 12-byte ObjectId vs 16-byte BSON UUID (subtype 4).");
        say("All three collections have a UNIQUE index on order_number, like the Postgres tables in §7.");
        say("Read percentiles need >100K samples to stabilize; see TsidScalingBench output or benchmark.md.");

        final int count = 100_000;
        final int batch = 1_000;

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(MONGO_URI))
                .uuidRepresentation(UuidRepresentation.STANDARD)
                .build();

        try (MongoClient client = MongoClients.create(settings)) {
            MongoDatabase db = client.getDatabase(MONGO_DB);
            MongoCollection<Document> tsidColl  = db.getCollection("orders_tsid");
            MongoCollection<Document> objIdColl = db.getCollection("orders_objectid");
            MongoCollection<Document> uuid7Coll = db.getCollection("orders_uuid7");
            tsidColl.drop();
            objIdColl.drop();
            uuid7Coll.drop();

            // Match the Postgres §7 schema: a unique index on order_number.
            IndexOptions unique = new IndexOptions().unique(true);
            Document orderNumberKey = new Document("order_number", 1);
            tsidColl.createIndex(orderNumberKey, unique);
            objIdColl.createIndex(orderNumberKey, unique);
            uuid7Coll.createIndex(orderNumberKey, unique);

            TSID.Factory factory = TSID.Factory.builder().withNode(42).build();

            // TSID inserts (NumberLong _id).
            long tsidStart = System.nanoTime();
            List<Document> buf = new ArrayList<>(batch);
            for (int i = 0; i < count; i++) {
                buf.add(orderDoc(factory.generate().toLong(), i));
                if (buf.size() == batch) {
                    tsidColl.insertMany(buf);
                    buf.clear();
                }
            }
            if (!buf.isEmpty()) tsidColl.insertMany(buf);
            long tsidInsertMs = (System.nanoTime() - tsidStart) / 1_000_000;

            // ObjectId inserts (Mongo-native 12-byte _id, generated client-side).
            long objIdStart = System.nanoTime();
            buf.clear();
            for (int i = 0; i < count; i++) {
                buf.add(orderDoc(new ObjectId(), i));
                if (buf.size() == batch) {
                    objIdColl.insertMany(buf);
                    buf.clear();
                }
            }
            if (!buf.isEmpty()) objIdColl.insertMany(buf);
            long objIdInsertMs = (System.nanoTime() - objIdStart) / 1_000_000;

            // UUIDv7 inserts via Java 26's UUID.ofEpochMillis (BSON Binary subtype 4).
            long uuid7Start = System.nanoTime();
            buf.clear();
            for (int i = 0; i < count; i++) {
                buf.add(orderDoc(UUID.ofEpochMillis(System.currentTimeMillis()), i));
                if (buf.size() == batch) {
                    uuid7Coll.insertMany(buf);
                    buf.clear();
                }
            }
            if (!buf.isEmpty()) uuid7Coll.insertMany(buf);
            long uuid7InsertMs = (System.nanoTime() - uuid7Start) / 1_000_000;

            // Force a WiredTiger checkpoint so storageSize and totalIndexSize
            // reflect the writes. Without this, the most recently written
            // collection reports near-zero on-disk numbers.
            client.getDatabase("admin").runCommand(new Document("fsync", 1));

            Document tsidStats  = db.runCommand(new Document("collStats", "orders_tsid"));
            Document objIdStats = db.runCommand(new Document("collStats", "orders_objectid"));
            Document uuid7Stats = db.runCommand(new Document("collStats", "orders_uuid7"));

            long tsidData  = ((Number) tsidStats.get("size")).longValue();
            long tsidDisk  = ((Number) tsidStats.get("storageSize")).longValue();
            long tsidIdx   = ((Number) tsidStats.get("totalIndexSize")).longValue();
            long objIdData = ((Number) objIdStats.get("size")).longValue();
            long objIdDisk = ((Number) objIdStats.get("storageSize")).longValue();
            long objIdIdx  = ((Number) objIdStats.get("totalIndexSize")).longValue();
            long uuid7Data = ((Number) uuid7Stats.get("size")).longValue();
            long uuid7Disk = ((Number) uuid7Stats.get("storageSize")).longValue();
            long uuid7Idx  = ((Number) uuid7Stats.get("totalIndexSize")).longValue();

            // Pull out the _id index alone (Mongo's reserved PK index name is "_id_").
            long tsidPkIdx  = ((Number) ((Document) tsidStats.get("indexSizes")).get("_id_")).longValue();
            long objIdPkIdx = ((Number) ((Document) objIdStats.get("indexSizes")).get("_id_")).longValue();
            long uuid7PkIdx = ((Number) ((Document) uuid7Stats.get("indexSizes")).get("_id_")).longValue();

            System.out.println();
            printf("Documents per collection:  %,d%n", count);
            System.out.println();
            System.out.println("Storage:");
            printf("  %-24s  %-12s  %-12s  %-12s  %-12s  %-12s%n",
                    "_id type", "Insert time", "Data size", "On-disk", "PK idx", "Total idx");
            printf("  %-24s  %-12s  %-12s  %-12s  %-12s  %-12s%n",
                    "--------", "-----------", "---------", "-------", "------", "---------");
            printf("  %-24s  %,9d ms  %,9d KB  %,9d KB  %,9d KB  %,9d KB%n",
                    "TSID (NumberLong, 8B)", tsidInsertMs,
                    tsidData / 1024, tsidDisk / 1024, tsidPkIdx / 1024, tsidIdx / 1024);
            printf("  %-24s  %,9d ms  %,9d KB  %,9d KB  %,9d KB  %,9d KB%n",
                    "ObjectId (12B)", objIdInsertMs,
                    objIdData / 1024, objIdDisk / 1024, objIdPkIdx / 1024, objIdIdx / 1024);
            printf("  %-24s  %,9d ms  %,9d KB  %,9d KB  %,9d KB  %,9d KB%n",
                    "UUIDv7 (BSON UUID, 16B)", uuid7InsertMs,
                    uuid7Data / 1024, uuid7Disk / 1024, uuid7PkIdx / 1024, uuid7Idx / 1024);
            System.out.println();
            System.out.println("Index ratio vs TSID:");
            printf("  PK only:    ObjectId %.2fx,  UUIDv7 %.2fx%n",
                    (double) objIdPkIdx / tsidPkIdx, (double) uuid7PkIdx / tsidPkIdx);
            printf("  All idxs:   ObjectId %.2fx,  UUIDv7 %.2fx   (totals include the order_number unique index, same size in all three)%n",
                    (double) objIdIdx / tsidIdx, (double) uuid7Idx / tsidIdx);

            pauseBeforeDrop("mongosh ... tsiddemo  →  show collections; db.orders_tsid.stats().indexSizes");
            tsidColl.drop();
            objIdColl.drop();
            uuid7Coll.drop();
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 10. Idempotent event ingestion via SHA-256 content hash
    // ─────────────────────────────────────────────────────────────────────────
    private static void section10_contentHash() throws Exception {
        header("10. Idempotent event ingestion via SHA-256 content hash");
        say("Use case: an event log where producers may retry.");
        say("UNIQUE(content_hash) blocks the duplicate INSERT at the DB.");
        say("Recomputing the hash on stored rows detects post-hoc tampering.");
        say("Pattern is id-type-agnostic — TSID is the surrogate PK, not the dedup key.");

        TSID originatingEntityId = TSID.fast();
        Instant occurredAt = Instant.parse("2026-04-21T14:22:07.331Z");

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("originating_service", "inventory-service");
        event.put("originating_entity_id", originatingEntityId.toLong());
        event.put("timestamp", occurredAt.toString());
        event.put("event_type", "stock_adjusted");
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("sku", "W-4821");
        payload.put("delta", -5);
        payload.put("warehouse", "east-2");
        event.put("payload", payload);

        byte[] hash = contentHash128(event);
        byte[] hashAgain = contentHash128(event);

        Map<String, Object> modified = new LinkedHashMap<>(event);
        Map<String, Object> modifiedPayload = new LinkedHashMap<>(payload);
        modifiedPayload.put("delta", -6);
        modified.put("payload", modifiedPayload);
        byte[] modifiedHash = contentHash128(modified);

        System.out.println();
        System.out.println("Event JSON (sorted keys, no whitespace):");
        System.out.println("  " + canonicalJson(event));
        System.out.println();
        printf("SHA-256 content hash (128-bit, hex):  %s%n", toHex(hash));
        System.out.println();
        printf("Producer retry — re-publishing same event yields same hash:  %s%n",
                Arrays.equals(hash, hashAgain));
        System.out.println("  -> INSERT ... ON CONFLICT (content_hash) DO NOTHING drops the duplicate.");
        System.out.println();
        printf("Tamper detection — delta changed -5 to -6 in the stored row:  hash differs: %s%n",
                !Arrays.equals(hash, modifiedHash));
        System.out.println("  -> a row whose recomputed hash no longer matches its stored hash has been altered.");
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 11. Concurrent id-generation throughput — pure JVM, no DB
    // ─────────────────────────────────────────────────────────────────────────
    private static void section11_concurrentGeneration() throws Exception {
        header("11. Concurrent id-generation throughput — TSID vs UUID.randomUUID()");
        say("Pure JVM, no database.");
        say("Each generator called from N virtual threads; threads released simultaneously to maximize contention.");

        final int totalIds       = 1_000_000;
        final int[] threadCounts = { 1, 16, 64, 256 };

        System.out.println();
        System.out.printf("Each row: %,d ids total, divided evenly across N virtual threads.%n", totalIds);
        System.out.println("Reference: a 10K req/s service generates ~10 ids/ms.");

        TSID.Factory tsidFactory = TSID.Factory.builder().withNode(42).build();
        printConcurrentHeader("TSID (per-node 4,096 ids/ms counter ceiling, lock-protected counter):");
        for (int t : threadCounts) runConcurrent(t, totalIds, () -> tsidFactory.generate().toLong());

        printConcurrentHeader("UUID.randomUUID() (UUIDv4, no ceiling, SecureRandom-backed):");
        for (int t : threadCounts) runConcurrent(t, totalIds, UUID::randomUUID);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // 12. Node ID resolution on Kubernetes: from pod name to withNode(N)
    // ─────────────────────────────────────────────────────────────────────────
    private static void section12_nodeIdOnEks() {
        header("12. Node ID resolution on Kubernetes: from pod name to withNode(N)");
        say("The 10-bit node field needs a unique value per pod writing to the same table or collection.");
        say("Uniqueness scope is per shared write target, not global. See kubernetes.md.");
        say("Inject pod identity via the Downward API as POD_NAME; HOSTNAME can be truncated to 64 chars.");

        // Show what the runtime sees so engineers can map this to their own environment.
        String localPodName  = System.getenv().getOrDefault("POD_NAME", "(unset)");
        String localHostname = System.getenv().getOrDefault("HOSTNAME", "(unset)");
        System.out.println();
        printf("Reading from the environment right now:%n");
        printf("  System.getenv(\"POD_NAME\") -> \"%s\"   (Downward API: fieldRef metadata.name)%n", localPodName);
        printf("  System.getenv(\"HOSTNAME\") -> \"%s\"%n", localHostname);

        // ── Pattern A: StatefulSet ─────────────────────────────────────────
        // Pod names are <statefulset-name>-<ordinal>, e.g. "orders-3". Validate range to fail fast on
        // replicas > 1024 or a configured .spec.ordinals.start.
        String[] statefulSetExamples = { "orders-0", "orders-3", "orders-42", "orders-99", "orders-1500" };
        System.out.println();
        System.out.println("Pattern A. StatefulSet (recommended):");
        System.out.println("  String podName = System.getenv(\"POD_NAME\");                     // \"orders-3\"");
        System.out.println("  int nodeId = Integer.parseInt(podName.substring(podName.lastIndexOf('-') + 1));");
        System.out.println("  if (nodeId < 0 || nodeId > 1023) throw new IllegalArgumentException(\"...\");");
        System.out.println("  TSID.Factory factory = TSID.Factory.builder().withNode(nodeId).build();");
        System.out.println();
        printf("  %-13s   ->    withNode(%s)%n", "$POD_NAME", "node id");
        printf("  %-13s   ->    %-7s%n", "-------------", "-------");
        for (String podName : statefulSetExamples) {
            try {
                int nodeId = resolveStatefulSetNodeId(podName);
                printf("  %-13s   ->    withNode(%d)%n", podName, nodeId);
            } catch (IllegalArgumentException e) {
                printf("  %-13s   ->    %s%n", podName, e.getMessage());
            }
        }

        // ── Pattern B: Deployment with platform-injected TSID_NODE ─────────
        // Something outside the static pod template (controller, mutating webhook, single-replica
        // workload) assigns a distinct TSID_NODE per pod before JVM startup. The app just reads it.
        String[] tsidNodeExamples = { "0", "7", "247", "1023", "1500" };
        System.out.println();
        System.out.println("Pattern B. Deployment with platform-injected TSID_NODE:");
        System.out.println("  String s = System.getenv(\"TSID_NODE\");   // assigned per pod by a controller / webhook / single-replica workload");
        System.out.println("  int nodeId = Integer.parseInt(s);");
        System.out.println("  if (nodeId < 0 || nodeId > 1023) throw new IllegalArgumentException(\"...\");");
        System.out.println("  TSID.Factory factory = TSID.Factory.builder().withNode(nodeId).build();");
        System.out.println();
        printf("  %-13s   ->    withNode(%s)%n", "$TSID_NODE", "node id");
        printf("  %-13s   ->    %-7s%n", "-------------", "-------");
        for (String tsidNode : tsidNodeExamples) {
            try {
                int nodeId = resolveTsidNode(tsidNode);
                printf("  %-13s   ->    withNode(%d)%n", tsidNode, nodeId);
            } catch (IllegalArgumentException e) {
                printf("  %-13s   ->    %s%n", tsidNode, e.getMessage());
            }
        }
        System.out.println();
        System.out.println("  Do not hash HOSTNAME. At 100 pods on 1024 slots P(duplicate node id) is ~99%.");
        System.out.println("  See kubernetes.md for the full pattern and the controllers/webhooks that set TSID_NODE.");

        // ── Pattern C: Shared write target across services — bit-partitioning ─
        // Reserve part of the node field for the service, the rest for the pod ordinal.
        // Example: 4 bits service (0..15) + 6 bits pod ordinal (0..63) = 16 services × 64 pods.
        int[][] partitionExamples = { {0, 0}, {0, 3}, {1, 0}, {3, 17}, {15, 63} };
        System.out.println();
        System.out.println("Pattern C. Shared write target across services: bit-partitioning.");
        System.out.println("  4 bits service ID (0..15) + 6 bits pod ordinal (0..63) = 16 services × 64 pods.");
        System.out.println("  int nodeId = (serviceId << 6) | podOrdinal;   // bounds-checked");
        System.out.println();
        printf("  %-25s   ->    withNode(%s)%n", "(serviceId, podOrdinal)", "node id");
        printf("  %-25s   ->    %-7s%n", "-------------------------", "-------");
        for (int[] pair : partitionExamples) {
            int nodeId = (pair[0] << 6) | pair[1];
            printf("  (%2d, %2d)                    ->    withNode(%d)%n", pair[0], pair[1], nodeId);
        }
    }

    /** Parse the trailing ordinal from a StatefulSet pod name and validate against the 10-bit node range. */
    private static int resolveStatefulSetNodeId(String podName) {
        int nodeId = Integer.parseInt(podName.substring(podName.lastIndexOf('-') + 1));
        if (nodeId < 0 || nodeId > 1023) {
            throw new IllegalArgumentException("TSID node id out of range: " + nodeId);
        }
        return nodeId;
    }

    /** Parse a platform-injected TSID_NODE env var and validate against the 10-bit node range. */
    private static int resolveTsidNode(String tsidNode) {
        int nodeId = Integer.parseInt(tsidNode);
        if (nodeId < 0 || nodeId > 1023) {
            throw new IllegalArgumentException("TSID node id out of range: " + nodeId);
        }
        return nodeId;
    }

    private static void printConcurrentHeader(String label) {
        System.out.println();
        System.out.println(label);
        System.out.printf("  %4s  %14s  %9s  %9s  %9s  %s%n",
                "T", "Throughput", "P50", "P95", "P99", "Collisions");
        System.out.printf("  %4s  %14s  %9s  %9s  %9s  %s%n",
                "---", "-------------", "--------", "--------", "--------", "----------");
    }

    private static void runConcurrent(int threadCount, int totalIds,
                                      Supplier<Object> generator) throws Exception {
        int perThread = totalIds / threadCount;
        int total     = perThread * threadCount;

        Object[][] results   = new Object[threadCount][perThread];
        long[][]   latencies = new long[threadCount][perThread];

        CountDownLatch start  = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(threadCount);

        long wallNs;
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int t = 0; t < threadCount; t++) {
                final int tIdx = t;
                pool.submit(() -> {
                    try {
                        start.await();
                        Object[] r = results[tIdx];
                        long[]   l = latencies[tIdx];
                        for (int i = 0; i < perThread; i++) {
                            long t0 = System.nanoTime();
                            Object id = generator.get();
                            long t1 = System.nanoTime();
                            r[i] = id;
                            l[i] = t1 - t0;
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        finish.countDown();
                    }
                });
            }
            Thread.sleep(50);                // let threads park on the start latch
            long wallStart = System.nanoTime();
            start.countDown();               // release all threads simultaneously
            finish.await();
            wallNs = System.nanoTime() - wallStart;
        }

        // Collisions: every id generated must be unique.
        Set<Object> seen = new HashSet<>(total);
        for (Object[] r : results) for (Object id : r) seen.add(id);
        int collisions = total - seen.size();

        // Latency percentiles across all threads' samples.
        long[] all = new long[total];
        int idx = 0;
        for (long[] tl : latencies) {
            System.arraycopy(tl, 0, all, idx, tl.length);
            idx += tl.length;
        }
        Arrays.sort(all);
        long p50 = all[(int) (all.length * 0.50)];
        long p95 = all[(int) (all.length * 0.95)];
        long p99 = all[(int) (all.length * 0.99)];

        double seconds   = wallNs / 1e9;
        double idsPerSec = total / seconds;
        printf("  %4d  %,11.0f/s  %,6d ns  %,6d ns  %,6d ns  %d%n",
                threadCount, idsPerSec, p50, p95, p99, collisions);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers
    // ─────────────────────────────────────────────────────────────────────────

    /** No-op unless {@code PAUSE=1}. Prints an inspection hint and waits for Enter before the caller drops state. */
    static void pauseBeforeDrop(String inspectionExample) {
        if (!PAUSE_BEFORE_DROP) return;
        System.out.println();
        System.out.println("─── Demo paused. State still in DB.");
        System.out.println("    Try: " + inspectionExample);
        System.out.print("    Press Enter to drop and continue ");
        if (STDIN.hasNextLine()) STDIN.nextLine();
    }

    /**
     * Build a 5-field order document. The {@code orderId} value lands in Mongo's
     * reserved {@code _id} field — that's how Postgres's {@code order_id} column
     * maps to a Mongo collection. {@code order_number} uses a synthetic,
     * fixed-width form so byte counts stay fair across _id types in §9.
     */
    static Document orderDoc(Object orderId, int i) {
        return new Document("_id", orderId)
                .append("order_number", String.format("ORD-2026-%06d", i))
                .append("item_count", 1 + (i * 7) % 8)
                .append("total_cents", 1_999L + (i * 137L) % 50_000)
                .append("status", STATUSES[i % STATUSES.length]);
    }

    static long[] pgStorage(Connection conn, String table) throws SQLException {
        String sql = "SELECT pg_relation_size('" + table + "'),"
                   + "       pg_indexes_size('" + table + "'),"
                   + "       pg_total_relation_size('" + table + "')";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            rs.next();
            return new long[] { rs.getLong(1), rs.getLong(2), rs.getLong(3) };
        }
    }

    /** Size in bytes of a single index (e.g. {@code "orders_tsid_pkey"}). */
    static long pgIndexSize(Connection conn, String indexName) throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT pg_relation_size('" + indexName + "')")) {
            rs.next();
            return rs.getLong(1);
        }
    }

    /** Nearest-rank percentile in nanoseconds. Used by TsidScalingBench. */
    static long[] percentiles(long[] latencies, double... pcts) {
        long[] sorted = latencies.clone();
        Arrays.sort(sorted);
        long[] result = new long[pcts.length];
        for (int i = 0; i < pcts.length; i++) {
            int idx = (int) Math.ceil(pcts[i] / 100.0 * sorted.length) - 1;
            if (idx < 0) idx = 0;
            if (idx >= sorted.length) idx = sorted.length - 1;
            result[i] = sorted[idx];
        }
        return result;
    }

    static String formatMs(long nanos) {
        return String.format("%.3f ms", nanos / 1_000_000.0);
    }

    static long[] measurePgReadsLong(Connection conn, String table, long[] ids,
                                     int samples, int warmup, Random rand) throws SQLException {
        long[] latencies = new long[samples];
        try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM " + table + " WHERE id = ?")) {
            for (int i = 0; i < warmup; i++) {
                ps.setLong(1, ids[rand.nextInt(ids.length)]);
                try (ResultSet rs = ps.executeQuery()) { rs.next(); }
            }
            for (int i = 0; i < samples; i++) {
                ps.setLong(1, ids[rand.nextInt(ids.length)]);
                long start = System.nanoTime();
                try (ResultSet rs = ps.executeQuery()) { rs.next(); }
                latencies[i] = System.nanoTime() - start;
            }
        }
        return latencies;
    }

    static long[] measurePgReadsUuid(Connection conn, String table, UUID[] ids,
                                     int samples, int warmup, Random rand) throws SQLException {
        long[] latencies = new long[samples];
        try (PreparedStatement ps = conn.prepareStatement("SELECT id FROM " + table + " WHERE id = ?")) {
            for (int i = 0; i < warmup; i++) {
                ps.setObject(1, ids[rand.nextInt(ids.length)]);
                try (ResultSet rs = ps.executeQuery()) { rs.next(); }
            }
            for (int i = 0; i < samples; i++) {
                ps.setObject(1, ids[rand.nextInt(ids.length)]);
                long start = System.nanoTime();
                try (ResultSet rs = ps.executeQuery()) { rs.next(); }
                latencies[i] = System.nanoTime() - start;
            }
        }
        return latencies;
    }

    static <T> long[] measureMongoReads(MongoCollection<Document> coll, T[] ids,
                                        int samples, int warmup, Random rand) {
        long[] latencies = new long[samples];
        for (int i = 0; i < warmup; i++) {
            coll.find(new Document("_id", ids[rand.nextInt(ids.length)])).first();
        }
        for (int i = 0; i < samples; i++) {
            long start = System.nanoTime();
            coll.find(new Document("_id", ids[rand.nextInt(ids.length)])).first();
            latencies[i] = System.nanoTime() - start;
        }
        return latencies;
    }

    private static byte[] contentHash128(Map<String, Object> content) throws Exception {
        String json = canonicalJson(content);
        byte[] full = MessageDigest.getInstance("SHA-256").digest(json.getBytes(StandardCharsets.UTF_8));
        return Arrays.copyOfRange(full, 0, 16);
    }

    private static String canonicalJson(Map<String, Object> content) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
        return mapper.writeValueAsString(sortedCopy(content));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> sortedCopy(Map<String, Object> input) {
        Map<String, Object> sorted = new TreeMap<>();
        for (Map.Entry<String, Object> entry : input.entrySet()) {
            Object value = entry.getValue();
            sorted.put(entry.getKey(),
                    (value instanceof Map<?, ?> m) ? sortedCopy((Map<String, Object>) m) : value);
        }
        return sorted;
    }

    private static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) sb.append(String.format("%02x", b));
        return sb.toString();
    }

    /**
     * Drop every Postgres table and MongoDB collection the demo has ever used.
     * Defensive against state left by a previous run that was killed mid-pause
     * (PAUSE_BEFORE_DROP halts before each section's cleanup; Ctrl-C at that
     * prompt skips the drop). Also handles tables from older versions of the
     * demo where section names changed.
     */
    private static void cleanSlate() {
        String[] pgTables = {
                "orders", "orders_tsid", "orders_uuid7", "orders_uuid4",
                "records", "records_tsid", "records_uuid7", "records_uuid4",
                "events"
        };
        String[] mongoColls = {
                "orders", "orders_tsid", "orders_objectid", "orders_uuid7", "orders_uuid4",
                "records", "records_tsid", "records_objectid", "records_uuid7", "records_uuid4"
        };

        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS);
             Statement stmt = conn.createStatement()) {
            for (String t : pgTables) {
                stmt.execute("DROP TABLE IF EXISTS " + t);
            }
        } catch (SQLException e) {
            throw new RuntimeException("clean-slate Postgres drop failed", e);
        }

        try (MongoClient client = MongoClients.create(MONGO_URI)) {
            MongoDatabase db = client.getDatabase(MONGO_DB);
            for (String c : mongoColls) {
                db.getCollection(c).drop();
            }
        }
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
        System.out.println("─── " + text + " " + "─".repeat(Math.max(3, 72 - text.length() - 5)));
    }

    private static void say(String text) {
        System.out.println(text);
    }

    private static void printf(String fmt, Object... args) {
        System.out.printf(fmt, args);
    }

    /** API DTO for the serialization boundary demo. id is the Base32 wire form. */
    public record Note(String id, String title, Instant createdAt) {}

    private TsidDemo() {}
}
