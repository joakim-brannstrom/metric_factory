/**
Copyright: Copyright (c) 2017, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)

Maybe add a default flush interval?
*/
module metric_factory.metric.collector;

import core.time : Duration;

import logger = std.experimental.logger;

import metric_factory.metric.types;

immutable Duration flushInterval = dur!"seconds"(10);

/**
 *
 * Is a class because it is passed around as a reference.
 *
 * A unique bucket kind for each kind that is collected.
 */
class Collector {
    private {
        Bucket!(Counter)[BucketName] counters;
        Gauge[BucketName] gauges;
        Bucket!(Timer)[BucketName] timers;
        SetBucket[BucketName] sets;
    }

    void put(Counter a) {
        debug logger.trace(a);

        if (auto v = a.name in counters) {
            v.put(a);
        } else {
            auto b = Bucket!Counter();
            b.put(a);
            counters[a.name] = b;
        }
    }

    void put(Gauge a) {
        debug logger.trace(a);

        if (auto v = a.name in gauges) {
            *v = a;
        } else {
            gauges[a.name] = a;
        }
    }

    void put(Timer a) {
        debug logger.trace(a);

        if (auto v = a.name in timers) {
            v.put(a);
        } else {
            auto b = Bucket!Timer();
            b.put(a);
            timers[a.name] = b;
        }
    }

    void put(Set a) {
        debug logger.trace(a);

        if (auto v = a.name in sets) {
            v.put(a);
        } else {
            auto b = SetBucket();
            b.put(a);
            sets[a.name] = b;
        }
    }

    /// Clear all buckets of data.
    void clear() {
        counters.clear;
        gauges.clear;
        timers.clear;
        sets.clear;
    }
}

struct ProcessResult {
    TimerResult[BucketName] timers;
    Gauge[BucketName] gauges;
    CounterResult[BucketName] counters;
    SetResult[BucketName] sets;
}

struct TimerResult {
    import core.time : Duration;

    Duration min;
    Duration max;
    Duration sum;
    Duration mean;
}

struct CounterResult {
    Counter.Change change;
    double changePerSecond;
}

struct SetResult {
    // number of unique elements
    size_t count;
}

ProcessResult process(Collector coll) {
    import std.array;
    import std.algorithm : sort, reduce, map;
    import core.time : dur, to;
    import std.conv : to;

    ProcessResult res;

    foreach (kv; coll.timers.byKeyValue) {
        const auto cnt = kv.value.data.length;
        if (cnt == 0)
            continue;

        auto values = kv.value.data.sort();

        const auto min_ = values[0].value;
        const auto max_ = values[$ - 1].value;
        const auto sum_ = reduce!((a, b) => a + b.value)(0.dur!"seconds", values);
        const auto mean_ = sum_ / cnt;

        auto r = TimerResult(min_, max_, sum_, mean_);
        debug logger.trace(r);
        res.timers[kv.key] = r;
    }

    foreach (kv; coll.counters.byKeyValue) {
        // dfmt off
        const auto sum_ = reduce!((a,b) => a+b)(Counter.Change(0),
            kv.value.data
            .map!((a) {
                  if (a.sampleRate.isNull || a.sampleRate == 0) return a.change;
                  else return Counter.Change(cast(long) (a.change / a.sampleRate));
                  }
            ));
        // dfmt on

        const auto val_per_sec = cast(double) sum_ / cast(double) flushInterval.total!"seconds";
        auto r = CounterResult(sum_, val_per_sec);
        debug logger.trace(r);
        res.counters[kv.key] = r;
    }

    res.gauges = coll.gauges;
    foreach (kv; coll.gauges) {
        debug logger.tracef("Gauge(%s, %s)", kv.name, kv.value);
    }

    foreach (kv; coll.sets.byKeyValue) {
        auto r = SetResult(kv.value.countUnique);
        debug logger.tracef("Set(%s, %s)", kv.key, r.count);
        res.sets[kv.key] = r;
    }

    return res;
}

// TODO maybe change to "serialiser"?
// #SPC-collection_serialiser
void writeCollection(Writer)(Collector coll, scope Writer w) {
    import std.format : formattedWrite;
    import metric_factory.csv;

    // TODO fix code duplication

    foreach (kv; coll.timers.byKeyValue) {
        formattedWrite(w, "%(%s\n%)\n", kv.value.data);
    }

    foreach (kv; coll.counters.byKeyValue) {
        formattedWrite(w, "%(%s\n%)\n", kv.value.data);
    }

    foreach (kv; coll.gauges.byKeyValue) {
        formattedWrite(w, "%s\n", kv.value);
    }

    foreach (kv; coll.sets.byKeyValue) {
        formattedWrite(w, "%s\n", Set(kv.key, Set.Value(kv.value.countUnique)));
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
    import std.exception : Exception, collectException;
    import std.format : formattedRead;
    import std.algorithm;

    static bool tryParseCounter(Collector coll, const string rest, BucketName name) nothrow {
        long value;
        double sample_r;

        try {
            auto txt = rest[];
            if (formattedRead(txt, "%s|c|@%f", value, sample_r) == 2) {
                coll.put(Counter(name, Counter.Change(value), Counter.SampleRate(sample_r)));
                return true;
            }
        }
        catch (Exception e) {
        }

        try {
            auto txt = rest[];
            if (formattedRead(txt, "%s|c", value) == 1) {
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

        long ms;

        try {
            if (formattedRead(rest, "%s|ms", ms) == 1) {
                coll.put(Timer(name, Timer.Value(ms.dur!"msecs")));
                return true;
            }
        }
        catch (Exception e) {
        }

        return false;
    }

    static bool tryParseGauge(Collector coll, string rest, BucketName name) nothrow {
        long value;

        try {
            if (formattedRead(rest, "%s|g", value) == 1) {
                coll.put(Gauge(name, Gauge.Value(value)));
                return true;
            }
        }
        catch (Exception e) {
        }

        return false;
    }

    static bool tryParseSet(Collector coll, string rest, BucketName name) nothrow {
        ulong value;

        try {
            if (formattedRead(rest, "%s|s", value) != 0) {
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

private:

struct Bucket(T) {
    import std.array : Appender;

    Appender!(T[]) payload;
    alias payload this;
}

struct SetBucket {
    private bool[typeof(Set.value)] payload;

    void put(Set v) {
        if (v.value !in payload)
            payload[v.value] = true;
    }

    /// Count unique elements
    size_t countUnique() {
        return payload.length;
    }

    void clear() {
        payload.clear;
    }
}

@("shall parse a string representing serialized metric types")
unittest {
    auto coll = new Collector;
    // test counters
    deserialise("foo1:75|c", coll);
    deserialise("foo2:63|c|@0.1", coll);
    // test gauge
    deserialise("foo:81|g", coll);
    // test timer
    deserialise("bar:1000|ms", coll);
    // test set
    deserialise("gav:32|s", coll);

    assert(coll.counters.length == 2);
    assert(coll.gauges.length == 1);
    assert(coll.timers.length == 1);
    assert(coll.sets.length == 1);
}
