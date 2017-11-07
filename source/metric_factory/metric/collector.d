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

interface MetricValueStore {
    void put(const Counter a);
    void put(const Gauge a);
    void put(const Timer a);
    void put(const Set a);
    void put(const Max a);
    void put(const Min a);
}

final class HostCollector : MetricValueStore {
    private CollectorAggregate coll;
    private TestHost host;

    this(CollectorAggregate coll, const TestHost host) {
        this.coll = coll;
        this.host = host;
    }

    template Put(T) {
        override void put(const T a) {
            coll.put(a, host);
        }
    }

    mixin Put!Gauge;
    mixin Put!Min;
    mixin Put!Max;

    mixin Put!Counter;
    mixin Put!Timer;
    mixin Put!Set;
}

final class Collector : MetricValueStore {
    import std.algorithm : map, joiner;

    Bucket!(Counter)[BucketName] counters;
    Bucket!(Timer)[BucketName] timers;
    SetBucket[BucketName] sets;

    template SimpleBucket(T, string name) {
        import std.format : format;

        mixin(T.stringof ~ "[BucketName] " ~ name ~ "s;");
        mixin("auto " ~ name ~ "Range() { return " ~ name ~ "s.byValue; }");
    }

    mixin SimpleBucket!(Gauge, "gauge");
    mixin SimpleBucket!(Max, "max");
    mixin SimpleBucket!(Min, "min");

    auto timerRange() {
        return timers.byValue.map!(a => a.data).joiner;
    }

    auto counterRange() {
        return counters.byValue.map!(a => a.data).joiner;
    }

    auto setRange() {
        return sets.byKeyValue.map!(a => Set(a.key, Set.Value(a.value.countUnique)));
    }

    override void put(const Counter a) {
        debug logger.trace(a);

        if (auto v = a.name in counters) {
            v.put(a);
        } else {
            auto b = Bucket!Counter();
            b.put(a);
            counters[a.name] = b;
        }
    }

    override void put(const Gauge a) {
        debug logger.trace(a);

        if (auto v = a.name in gauges) {
            *v = a;
        } else {
            gauges[a.name] = a;
        }
    }

    override void put(const Max a) {
        debug logger.trace(a);

        if (auto v = a.name in maxs) {
            if (a.value > v.value) {
                *v = a;
            }
        } else {
            maxs[a.name] = a;
        }
    }

    override void put(const Min a) {
        debug logger.trace(a);

        if (auto v = a.name in mins) {
            if (a.value < v.value) {
                *v = a;
            }
        } else {
            mins[a.name] = a;
        }
    }

    override void put(const Timer a) {
        debug logger.trace(a);

        if (auto v = a.name in timers) {
            v.put(a);
        } else {
            auto b = Bucket!Timer();
            b.put(a);
            timers[a.name] = b;
        }
    }

    override void put(const Set a) {
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

/** Aggregation of multiple collections.
 *
 * Is a class because it is passed around as a reference.
 *
 * A unique bucket kind for each kind that is collected.
 */
final class CollectorAggregate : MetricValueStore {
    TestHost[TestHost.Hash] testHosts;

    /// Aggregation of all data received.
    Collector globalAggregate;

    /// Test data for a specific host.
    Collector[TestHost.Hash] hostAggregate;

    this() {
        globalAggregate = new Collector;
    }

    static string makePut(string t) {
        import std.format : format;

        return format(q{override void put(const %s a) {
            globalAggregate.put(a);
        }

        void put(const %s a, const TestHost host) {
            put(host);
            globalAggregate.put(a);
            hostAggregate[host.toHash].put(a);
        }}, t, t);
    }

    mixin(makePut("Counter"));
    mixin(makePut("Gauge"));
    mixin(makePut("Max"));
    mixin(makePut("Min"));
    mixin(makePut("Timer"));
    mixin(makePut("Set"));

    void put(const TestHost a) {
        if (a.toHash !in testHosts) {
            debug logger.trace(a);
            testHosts[a.toHash] = a;
            hostAggregate[a.toHash] = new Collector;
        }
    }
}

struct ProcessResult {
    /// Reverse map to translate a hash to a string.
    TestHost.Value[TestHost.Hash] testHosts;

    HostResult globalResult;
    HostResult[TestHost.Hash] hostResult;
}

struct HostResult {
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

ProcessResult process(CollectorAggregate coll) {
    ProcessResult res;

    foreach (kv; coll.testHosts.byKeyValue) {
        res.testHosts[kv.key] = kv.value.name;
    }

    res.globalResult = process(coll.globalAggregate);

    foreach (host; coll.hostAggregate.byKeyValue) {
        res.hostResult[host.key] = process(host.value);
    }

    return res;
}

HostResult process(Collector coll) {
    import std.array;
    import std.algorithm : sort, reduce, map;
    import core.time : dur, to;
    import std.conv : to;
    import std.math : ceil, approxEqual;

    HostResult res;

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
        const auto sum_ = reduce!((a,b) => a+b)(0L,
            kv.value.data
            .map!((a) {
                  if (a.sampleRate.isNull || approxEqual(a.sampleRate.get, 0.0, double.min_normal, double.min_normal)) return cast(long) a.change;
                  else return cast(long) ceil(cast(double) a.change / a.sampleRate.get);
                  }
            ));
        // dfmt on

        const auto val_per_sec = cast(double) sum_ / cast(double) flushInterval.total!"seconds";
        auto r = CounterResult(Counter.Change(sum_), val_per_sec);
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

private:

struct Bucket(T) {
    import std.array : Appender;

    Appender!(T[]) payload;
    alias payload this;
}

struct SetBucket {
    bool[typeof(Set.value)] data;

    void put(Set v) {
        if (v.value !in data)
            data[v.value] = true;
    }

    /// Count unique elements
    size_t countUnique() {
        return data.length;
    }

    void clear() {
        data.clear;
    }
}
