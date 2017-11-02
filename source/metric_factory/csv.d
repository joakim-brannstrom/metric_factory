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
}
