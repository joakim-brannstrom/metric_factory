/**
Copyright: Copyright (c) 2017, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)

This module contains functions for generating csv files.
*/
module metric_factory.csv;

import metric_factory.metric : ProcessResult, HostResult;

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

void putCSV(Writer)(scope Writer w, ProcessResult res) {
    size_t index;

    writeCSV(w, "index", "description", "host", "date", "time", "value",
            "change", "min (ms)", "max (ms)", "sum (ms)", "mean (ms)");
    putCSV(w, res.globalResult, index, "");

    foreach (ref host; res.hostResult.byKeyValue) {
        if (auto host_name = host.key in res.testHosts) {
            putCSV(w, host.value, index, cast(string)*host_name);
        }
    }
}

/** Write the result to a .csv-file.
 *
 * #SPC-collection_to_csv
 */
void putCSV(Writer)(scope Writer w, HostResult res, ref size_t index, string host) {
    import std.ascii : newline;
    import std.datetime;
    import std.format : formattedWrite, format;
    import std.range.primitives : put;

    auto curr_d = Clock.currTime;
    auto curr_d_txt = format("%s-%s-%s", curr_d.year, cast(ushort) curr_d.month, curr_d.day);
    auto curr_t_txt = format("%s:%s:%s", curr_d.hour, curr_d.minute, curr_d.second);

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
