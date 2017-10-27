/**
Copyright: Copyright (c) 2017, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)
*/
module metric_factory.dataformat.statsd;

import logger = std.experimental.logger;

import metric_factory.metric.types;
import metric_factory.metric.collector : Collector;

void serialize(Writer, T)(scope Writer w, const ref T v) {
    import std.format : formattedWrite;
    import std.range.primitives : put;

    static if (is(T == Counter)) {
        if (v.sampleRate.isNull) {
            formattedWrite(w, "%s:%s|c", v.name, v.change);
        } else {
            formattedWrite(w, "%s:%s|c|@%f", v.name, v.change, v.sampleRate);
        }
    } else static if (is(T == Gauge)) {
        formattedWrite(w, "%s:%s|g", v.name, v.value);
    } else static if (is(T == Timer)) {
        formattedWrite(w, "%s:%s|ms", v.name, v.value.total!"msecs");
    } else static if (is(T == Set)) {
        formattedWrite(w, "%s:%s|s", v.name, v.value);
    } else {
        static assert(0, "Unsupported type " ~ T.stringof);
    }

    put(w, "\n");
}

// #SPC-collection_serialiser
void serialize(Writer)(Collector coll, scope Writer w) {
    import std.algorithm : map, joiner;
    import metric_factory.dataformat.statsd : serialize;

    foreach (const ref v; coll.timers.byValue.map!(a => a.data).joiner) {
        serialize(w, v);
    }

    foreach (const ref v; coll.counters.byValue.map!(a => a.data).joiner) {
        serialize(w, v);
    }

    foreach (const ref v; coll.gauges.byValue) {
        serialize(w, v);
    }

    foreach (const ref kv; coll.sets.byKeyValue) {
        auto s = Set(kv.key, Set.Value(kv.value.countUnique));
        serialize(w, s);
    }
}

/** Deserialise a line.
 *
 * Params:
 *  line =
 *  coll = collector to store the results in
 *
 * #SPC-collector_deserialize
 */
void deserialise(const(char)[] line, Collector coll) nothrow {
    import std.conv : to;
    import std.exception : Exception, collectException;
    import std.format : formattedRead;
    import std.algorithm;
    import std.regex;

    static bool tryParseCounter(Collector coll, const string rest, BucketName name) nothrow {
        auto re1 = ctRegex!(`(.*)\|c`);
        auto re2 = ctRegex!(`(.*)\|c\|@(.*)`);

        try {
            auto m = matchFirst(rest, re2);
            if (!m.empty) {
                auto value = m[1].to!long;
                auto sample_r = m[2].to!double;
                coll.put(Counter(name, Counter.Change(value), Counter.SampleRate(sample_r)));
                return true;
            }
        }
        catch (Exception e) {
        }

        try {
            auto m = matchFirst(rest, re1);
            if (!m.empty) {
                auto value = m[1].to!long;
                coll.put(Counter(name, Counter.Change(value)));
                return true;
            }
        }
        catch (Exception e) {
        }

        return false;
    }

    static bool tryParseTimer(Collector coll, string rest, BucketName name) nothrow {
        import core.time : dur;

        auto re = ctRegex!(`(.*)\|ms`);

        try {
            auto m = matchFirst(rest, re);
            if (!m.empty) {
                auto ms = m[1].to!long.dur!"msecs";
                coll.put(Timer(name, Timer.Value(ms)));
                return true;
            }
        }
        catch (Exception e) {
        }

        return false;
    }

    static bool tryParseGauge(Collector coll, string rest, BucketName name) nothrow {
        auto re = ctRegex!(`(.*)\|g`);

        try {
            auto m = matchFirst(rest, re);
            if (!m.empty) {
                auto value = m[1].to!long;
                coll.put(Gauge(name, Gauge.Value(value)));
                return true;
            }
        }
        catch (Exception e) {
        }

        return false;
    }

    static bool tryParseSet(Collector coll, string rest, BucketName name) nothrow {
        auto re = ctRegex!(`(.*)\|s`);

        try {
            auto m = matchFirst(rest, re);
            if (!m.empty) {
                auto value = m[1].to!ulong;
                coll.put(Set(name, Set.Value(value)));
                return true;
            }
        }
        catch (Exception e) {
        }

        return false;
    }

    try {
        BucketName name;
        string rest;

        if (formattedRead(line, "%s:%s", name.payload, rest) != 2) {
            // invalid entry, skipping
        } else if (tryParseCounter(coll, rest, name)) {
        } else if (tryParseTimer(coll, rest, name)) {
        } else if (tryParseGauge(coll, rest, name)) {
        } else if (tryParseSet(coll, rest, name)) {
        } else {
            debug logger.trace("Unable to parse: ", rest);
        }
    }
    catch (Exception e) {
        debug collectException(logger.trace(e.msg));
    }
}

@("shall parse a string representing serialized metric types")
unittest {
    import std.math : approxEqual;

    auto coll = new Collector;
    // test counters
    deserialise("foo1:75|c", coll);
    deserialise("foo2:63|c|@0.1", coll);
    assert(coll.counters.length == 2);
    assert(!coll.counters[BucketName("foo2")].data[0].sampleRate.isNull);
    assert(coll.counters[BucketName("foo2")].data[0].sampleRate.approxEqual(0.1));

    // test gauge
    deserialise("foo:81|g", coll);
    assert(coll.gauges.length == 1);

    // test timer
    deserialise("bar:1000|ms", coll);
    assert(coll.timers.length == 1);

    // test set
    deserialise("gav:32|s", coll);
    assert(coll.sets.length == 1);
}
