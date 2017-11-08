/**
Copyright: Copyright (c) 2017, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)

This module contains functions for generating csv files.
*/
module metric_factory.csv;

import metric_factory.metric : ProcessResult, HostResult;
import metric_factory.types : Timestamp;

/// Write a line as CSV
void writeCSV(Writer, T...)(scope Writer w, auto ref T args) {
    import std.ascii : newline;
    import std.format : formattedWrite;
    import std.range.primitives : put;

    bool first = true;
    foreach (a; args) {
        if (!first)
            put(w, ",");

        static if (__traits(hasMember, a, "isNull")) {
            if (!a.isNull) {
                formattedWrite(w, `"%s"`, a);
            }
        } else {
            formattedWrite(w, `"%s"`, a);
        }

        first = false;
    }

    put(w, newline);
}

void putCSVHeader(Writer)(scope Writer w) {
    writeCSV(w, "index", "description", "host", "date", "time", "value",
            "change", "min (ms)", "max (ms)", "sum (ms)", "mean (ms)");
}

void putCSV(Writer)(scope Writer w, ProcessResult res, ref size_t index) {
    import std.datetime : Clock;

    auto curr_d = Clock.currTime.Timestamp;
    putCSV(w, curr_d, res.globalResult, index, "");

    foreach (ref host; res.hostResult.byKeyValue) {
        if (auto host_name = host.key in res.testHosts) {
            putCSV(w, curr_d, host.value, index, cast(string)*host_name);
        }
    }
}

/** Write the result to a .csv-file.
 *
 * #SPC-collection_to_csv
 */
void putCSV(Writer)(scope Writer w, const Timestamp raw_ts, HostResult res,
        ref size_t index, string host) {
    import std.ascii : newline;
    import std.format : formattedWrite, format;
    import std.range.primitives : put;

    auto ts = raw_ts.toLocalTime;

    auto curr_d_txt = format("%04s-%02s-%02s", ts.year, cast(ushort) ts.month, ts.day);
    auto curr_t_txt = format("%02s:%02s:%02s", ts.hour, ts.minute, ts.second);

    foreach (kv; res.timers.byKeyValue) {
        index++;
        writeCSV(w, index, kv.key, host, curr_d_txt, curr_t_txt, "", "",
                kv.value.min.total!"msecs", kv.value.max.total!"msecs",
                kv.value.sum.total!"msecs", kv.value.mean.total!"msecs");
    }

    foreach (kv; res.counters.byKeyValue) {
        index++;
        // TODO currently the changePerSecond isn't useful because this empties directly.
        //writeCSV(w, kv.key, kv.value.change, kv.value.changePerSecond);
        writeCSV(w, index, kv.key, host, curr_d_txt, curr_t_txt, "", kv.value.change);
    }

    foreach (kv; res.gauges.byKeyValue) {
        index++;
        writeCSV(w, index, kv.key, host, curr_d_txt, curr_t_txt, kv.value.value);
    }

    foreach (kv; res.sets.byKeyValue) {
        index++;
        writeCSV(w, index, kv.key, host, curr_d_txt, curr_t_txt, kv.value.count);
    }
}

private:

class Fixture {
    import std.array : Appender;
    import metric_factory.metric.collector;
    import metric_factory.metric.types;

    CollectorAggregate coll;
    TestHost h;
    BucketName b;
    Appender!string app;

    this() {
        coll = new CollectorAggregate;
        h = TestHost(TestHost.Value("test_host"));
        b = BucketName("bucket");
    }

    void compare(string[] expected) {
        import std.algorithm;
        import std.range;

        foreach (idx, line; app.data.splitter("\n").enumerate) {
            assert(line == expected[idx], line ~ ":" ~ expected[idx]);
        }
    }
}

@("shall produce a CSV file with all results")
unittest {
    import std.datetime;
    import metric_factory.metric.collector;
    import metric_factory.metric.types;

    // arrange
    auto f = new Fixture;

    f.coll.put(Timer(f.b, Timer.Value(42.dur!"msecs")), f.h);
    f.coll.put(Gauge(f.b, Gauge.Value(43)), f.h);
    f.coll.put(Set(f.b, Set.Value(44)), f.h);
    f.coll.put(Counter(f.b, Counter.Change(45)), f.h);

    auto res = process(f.coll);

    // act
    {
        size_t idx;
        putCSV(f.app, Timestamp(SysTime(0)), res.hostResult[f.h.toHash], idx, f.h.name);
    }

    // assert
    // dfmt off
    string[] expected = [
        `"1","bucket","test_host","0001-01-01","01:12:12","","","42","42","42","42"`,
        `"2","bucket","test_host","0001-01-01","01:12:12","","45"`,
        `"3","bucket","test_host","0001-01-01","01:12:12","43"`,
        `"4","bucket","test_host","0001-01-01","01:12:12","1"`,
        ``];
    // dfmt on

    f.compare(expected);
}

// #TST-min_max_metric_type_in_csv
@("shall produce a CSV file with the max result")
unittest {
    import std.datetime;
    import metric_factory.metric.collector;
    import metric_factory.metric.types;

    // arrange
    auto f = new Fixture;

    f.coll.put(Max(f.b, Max.Value(46)), f.h);
    f.coll.put(Max(f.b, Max.Value(47)), f.h);
    f.coll.put(Max(f.b, Max.Value(42)), f.h);

    auto res = process(f.coll);

    // act
    {
        size_t idx;
        putCSV(f.app, Timestamp(SysTime(0)), res.hostResult[f.h.toHash], idx, f.h.name);
    }

    // assert
    // dfmt off
    string[] expected = [
        `"1","bucket","test_host","0001-01-01","01:12:12","47"`,
        ``];
    // dfmt on

    f.compare(expected);
}

@("shall produce a CSV file with the min result")
unittest {
    import std.datetime;
    import metric_factory.metric.collector;
    import metric_factory.metric.types;

    // arrange
    auto f = new Fixture;

    f.coll.put(Min(f.b, Min.Value(46)), f.h);
    f.coll.put(Min(f.b, Min.Value(42)), f.h);
    f.coll.put(Min(f.b, Min.Value(47)), f.h);

    auto res = process(f.coll);

    // act
    {
        size_t idx;
        putCSV(f.app, Timestamp(SysTime(0)), res.hostResult[f.h.toHash], idx, f.h.name);
    }

    // assert
    // dfmt off
    string[] expected = [
        `"1","bucket","test_host","0001-01-01","01:12:12","42"`,
        ``];
    // dfmt on

    f.compare(expected);
}
