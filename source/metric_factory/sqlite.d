/**
Copyright: Copyright (c) 2017, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)
*/
module metric_factory.sqlite3;

import std.datetime : SysTime;

import logger = std.experimental.logger;

import d2sqlite3 : sqlDatabase = Database;

import metric_factory.metric.collector : Collector;
import metric_factory.metric.types : TestHost;
import metric_factory.types : Timestamp;
import metric_factory.plugin : Plugin;

struct MetricId {
    long payload;
    alias payload this;
}

struct BucketId {
    long payload;
    alias payload this;
}

struct TestHostId {
    long payload;
    alias payload this;
}

/**
 *
 * From the sqlite3 manual $(LINK https://www.sqlite.org/datatype3.html):
 * Each value stored in an SQLite database (or manipulated by the database
 * engine) has one of the following storage classes:
 *
 * NULL. The value is a NULL value.
 *
 * INTEGER. The value is a signed integer, stored in 1, 2, 3, 4, 6, or 8 bytes
 * depending on the magnitude of the value.
 *
 * REAL. The value is a floating point value, stored as an 8-byte IEEE floating
 * point number.
 *
 * TEXT. The value is a text string, stored using the database encoding (UTF-8,
 * UTF-16BE or UTF-16LE).
 *
 * BLOB. The value is a blob of data, stored exactly as it was input.
 *
 * A storage class is more general than a datatype. The INTEGER storage class,
 * for example, includes 6 different integer datatypes of different lengths.
 * This makes a difference on disk. But as soon as INTEGER values are read off
 * of disk and into memory for processing, they are converted to the most
 * general datatype (8-byte signed integer). And so for the most part, "storage
 * class" is indistinguishable from "datatype" and the two terms can be used
 * interchangeably.
 */
struct Database {
    import std.typecons : Tuple, Nullable;
    import metric_factory.metric.types;

    private sqlDatabase db;
    private immutable sql_find_bucket = "SELECT id FROM bucket WHERE name == :name";

    static auto make() {
        return Database(initializeDB);
    }

    /// Insert a Collector into the DB.
    /// Returns: the ID of the metric that where added
    MetricId put(const Timestamp ts, Collector coll) {
        import std.format : format;
        import d2sqlite3;

        auto stmt = db.prepare("INSERT INTO metric (timestamp) VALUES (:timestamp)");
        stmt.bind(":timestamp", format("%04s-%02s-%02sT%02s:%02s:%02s.%s",
                ts.year, cast(ushort) ts.month, ts.day, ts.hour, ts.minute,
                ts.second, ts.fracSecs.total!"msecs"));
        stmt.execute;
        const long metric_id = db.lastInsertRowid;
        stmt.reset;

        stmt = db.prepare(
                "INSERT INTO timer_t (metricid, bucketid, elapsed_ms) VALUES (:mid, :bid, :ms)");
        foreach (v; coll.timerRange) {
            const long bucket_id = put(v.name);
            stmt.bindAll(metric_id, bucket_id, v.value.total!"msecs");
            stmt.execute;
            stmt.reset;
        }

        stmt = db.prepare(
                "INSERT INTO gauge_t (metricid, bucketid, value) VALUES (:mid, :bid, :value)");
        foreach (v; coll.gaugeRange) {
            const long bucket_id = put(v.name);
            stmt.bindAll(metric_id, bucket_id, v.value.payload);
            stmt.execute;
            stmt.reset;
        }

        stmt = db.prepare(
                "INSERT INTO counter_t (metricid, bucketid, change, sampleRate) VALUES (:mid, :bid, :value, :sample_r)");
        foreach (v; coll.counterRange) {
            const long bucket_id = put(v.name);
            stmt.bindAll(metric_id, bucket_id, v.change.payload,
                    v.sampleRate.isNull ? 1.0 : v.sampleRate.get.payload);
            stmt.execute;
            stmt.reset;
        }

        stmt = db.prepare(
                "INSERT INTO set_t (metricid, bucketid, value) VALUES (:mid, :bid, :value)");
        foreach (v; coll.setRange) {
            const long bucket_id = put(v.name);
            stmt.bindAll(metric_id, bucket_id, v.value.payload);
            stmt.execute;
            stmt.reset;
        }

        return MetricId(metric_id);
    }

    /// Put a bucket into the database.
    /// Returns: its primary key.
    BucketId put(const BucketName b) {
        auto bucket_stmt = db.prepare(sql_find_bucket);
        bucket_stmt.bind(":name", b.payload);
        auto res = bucket_stmt.execute;

        if (res.empty) {
            auto bucket_insert_stmt = db.prepare("INSERT INTO bucket (name) VALUES (:name)");
            bucket_insert_stmt.bind(":name", b.payload);
            bucket_insert_stmt.execute;
            return db.lastInsertRowid.BucketId;
        } else {
            return res.oneValue!long.BucketId;
        }
    }

    BucketName get(BucketId bid) {
        auto bucket_stmt = db.prepare("SELECT name FROM bucket WHERE bucket.id == :bid");
        bucket_stmt.bind(":bid", bid.payload);
        auto res = bucket_stmt.execute;
        return BucketName(res.oneValue!string);
    }

    void put(const TestHost host, const Timestamp ts, Collector coll) {
        import d2sqlite3;

        auto metric_id = this.put(ts, coll);

        auto stmt = db.prepare("INSERT INTO test_host (host) VALUES (:host)");
        stmt.bind(":host", cast(string) host.name);
        stmt.execute;
        const long test_hostid = db.lastInsertRowid;
        stmt.reset;

        stmt = db.prepare("UPDATE metric SET test_hostid = :thid WHERE metric.id == :mid");
        stmt.bindAll(test_hostid, cast(long) metric_id);
        stmt.execute;
    }

    alias GetMetricResult = Tuple!(MetricId, "id", TestHostId, "testHostId",
            Timestamp, "timestamp");

    /// Returns: all stored metric ID.
    GetMetricResult[] getMetrics() {
        import std.array;
        import std.algorithm : map;
        import d2sqlite3;

        // maybe order by timestamp?

        auto stmt = db.prepare("SELECT id,test_hostid,timestamp FROM metric ORDER BY id");
        auto res = stmt.execute;

        return res.map!(a => GetMetricResult(MetricId(a.peek!long(0)),
                TestHostId(a.peek!long(1)), Timestamp(a.peek!string(2).fromSqLiteDateTime))).array();
    }

    Nullable!TestHost getTestHost(TestHostId id) {
        import d2sqlite3;

        typeof(return) rval;

        auto stmt = db.prepare("SELECT host FROM test_host WHERE id == :id");
        stmt.bind(":id", id.payload);
        auto res = stmt.execute;

        if (!res.empty) {
            rval = TestHost(TestHost.Value(res.oneValue!string));
        }

        return rval;
    }

    Collector get(MetricId mid) {
        import core.time : dur;
        import d2sqlite3;
        import metric_factory.metric.collector : Collector;

        auto coll = new Collector;

        auto stmt = db.prepare("SELECT bucketid,value FROM set_t WHERE set_t.metricid == :mid");
        stmt.bind(":mid", mid.payload);
        foreach (v; stmt.execute) {
            auto bucket = this.get(BucketId(v.peek!long(0)));
            coll.put(Set(bucket, Set.Value(v.peek!long(1))));
        }
        stmt.reset;

        stmt = db.prepare(
                "SELECT bucketid,change,sampleRate FROM counter_t WHERE counter_t.metricid == :mid");
        stmt.bind(":mid", mid.payload);
        foreach (v; stmt.execute) {
            auto bucket = this.get(BucketId(v.peek!long(0)));
            coll.put(Counter(bucket, Counter.Change(v.peek!long(1)),
                    Counter.SampleRate(v.peek!double(2))));
        }
        stmt.reset;

        stmt = db.prepare("SELECT bucketid,value FROM gauge_t WHERE gauge_t.metricid == :mid");
        stmt.bind(":mid", mid.payload);
        foreach (v; stmt.execute) {
            auto bucket = this.get(BucketId(v.peek!long(0)));
            coll.put(Gauge(bucket, Gauge.Value(v.peek!long(1))));
        }
        stmt.reset;

        stmt = db.prepare("SELECT bucketid,elapsed_ms FROM timer_t WHERE timer_t.metricid == :mid");
        stmt.bind(":mid", mid.payload);
        foreach (v; stmt.execute) {
            auto bucket = this.get(BucketId(v.peek!long(0)));
            coll.put(Timer(bucket, Timer.Value(v.peek!long(1).dur!"msecs")));
        }
        stmt.reset;

        return coll;
    }
}

private:

sqlDatabase initializeDB() {
    import d2sqlite3;

    try {
        auto db = sqlDatabase("metric_factory.sqlite3", SQLITE_OPEN_READWRITE);
        return db;
    }
    catch (Exception e) {
        logger.trace(e.msg);
        logger.trace("Initializing a new sqlite3 database");
    }

    auto db = sqlDatabase("metric_factory.sqlite3", SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE);
    initializeTables(db);
    return db;
}

void initializeTables(ref sqlDatabase db) {
    // the timestamp shall be in UTC time.

    db.run("CREATE TABLE metric (
        id              INTEGER PRIMARY KEY,
        test_hostid     INTEGER,
        timestamp       DATETIME NOT NULL
        )");

    db.run("CREATE TABLE bucket (
        id      INTEGER PRIMARY KEY,
        name    TEXT NOT NULL
        )");

    db.run("CREATE TABLE test_host (
        id          INTEGER PRIMARY KEY,
        host        TEXT
        )");

    db.run("CREATE TABLE counter_t (
        id          INTEGER PRIMARY KEY,
        metricid    INTEGER NOT NULL,
        bucketid    INTEGER NOT NULL,
        change      INTEGER,
        sampleRate  REAL,
        FOREIGN KEY(bucketid) REFERENCES plugin(id),
        FOREIGN KEY(metricid) REFERENCES metric(id)
        )");

    db.run("CREATE TABLE gauge_t (
        id          INTEGER PRIMARY KEY,
        metricid    INTEGER NOT NULL,
        bucketid    INTEGER NOT NULL,
        value       INTEGER,
        FOREIGN KEY(bucketid) REFERENCES plugin(id),
        FOREIGN KEY(metricid) REFERENCES metric(id)
        )");

    db.run("CREATE TABLE timer_t (
        id          INTEGER PRIMARY KEY,
        metricid    INTEGER NOT NULL,
        bucketid    INTEGER NOT NULL,
        elapsed_ms  INTEGER,
        FOREIGN KEY(bucketid) REFERENCES plugin(id),
        FOREIGN KEY(metricid) REFERENCES metric(id)
        )");

    db.run("CREATE TABLE set_t (
        id          INTEGER PRIMARY KEY,
        metricid    INTEGER NOT NULL,
        bucketid    INTEGER NOT NULL,
        value       INTEGER,
        FOREIGN KEY(bucketid) REFERENCES plugin(id),
        FOREIGN KEY(metricid) REFERENCES metric(id)
        )");
}

SysTime fromSqLiteDateTime(string raw_dt) {
    import core.time : dur;
    import std.datetime : DateTime, UTC;
    import std.format : formattedRead;

    int year, month, day, hour, minute, second, msecs;
    formattedRead(raw_dt, "%s-%s-%sT%s:%s:%s.%s", year, month, day, hour, minute, second, msecs);
    auto dt = DateTime(year, month, day, hour, minute, second);

    return SysTime(dt, msecs.dur!"msecs", UTC());
}
