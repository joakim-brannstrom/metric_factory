/**
Copyright: Copyright (c) 2017, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)

This file contains the function to transform to/from the binary format that a
collection can be represented in.

# Layers
0. ubyte stream.
1. PacketType - Packets
    The PacketType is a uint8 so only 256 different packets are supported.

# Stream specification
This is the specification for the binary stream.

x_0     PacketKind:TestHost
x_1     TestHost
x_n     PacketKind
x_n+1   Packet matching the PacketKind.

# Packets
this are the packets and their specification.

Counter
    str32 name
    int64 change

CounterWithSampleRate
    str32 name
    int64 change
    float64 sampleRate

Timer
    str32 name
    int64 ms

Gauge
    str32 name
    int64 value

Set
    str32 name
    uint64 value

TestHost
    str32 name
*/
module metric_factory.dataformat.mfbin;

import std.range.primitives : put;

import logger = std.experimental.logger;

import msgpack_ll : MsgpackType;

import metric_factory.metric.collector : Collector, CollectorAggregate;
import metric_factory.metric.types;

void serialize(Writer)(scope Writer w, Collector coll, TestHost host) {
    serialize(w, host);
    serialize(w, coll);
}

/// Serialize the metric types in the Collector to a stream of bytes.
void serialize(Writer)(scope Writer w, Collector coll) {
    import std.algorithm : map, joiner;
    import std.range.primitives : put;

    foreach (a; coll.counters.byValue.map!(a => a.data).joiner)
        serialize(w, a);
    foreach (a; coll.gauges.byValue)
        serialize(w, a);
    foreach (a; coll.timers.byValue.map!(a => a.data).joiner)
        serialize(w, a);

    // serialize as a stream of Set.
    // dfmt off
    foreach (a; coll.sets
             .byKeyValue
             .map!(a => a.value.data.byKey
                   .map!(b => Set(a.key, Set.Value(b))))
             .joiner) {
        serialize(w, a);
    }
    // dfmt on
}

void deserialize(ubyte[] buf, CollectorAggregate coll) {
    import std.exception;
    import msgpack_ll;

    debug logger.trace(buf);

    auto raw_kind = demux!(MsgpackType.uint8, ubyte)(buf);
    if (raw_kind != PacketKind.testHost) {
        throw new Exception("Malformed aggregate packet");
    }

    auto host = demuxTestHost(buf);
    coll.put(host);

    static struct HostAgg {
        CollectorAggregate coll;
        TestHost host;

        void put(T)(const T v) {
            coll.put(v, host);
        }
    }

    auto host_agg = HostAgg(coll, host);

    deserialize(buf, host_agg);
}

void deserialize(T)(ubyte[] buf, T coll) {
    import std.conv : to;
    import msgpack_ll;

    debug logger.trace(buf);

    while (buf.length != 0) {
        debug logger.trace("bytes left:", buf.length);

        auto raw_kind = demux!(MsgpackType.uint8, ubyte)(buf);
        if (raw_kind > PacketKind.max) {
            throw new Exception("Malformed packet kind: " ~ raw_kind.to!string);
        }
        auto kind = cast(PacketKind) raw_kind;

        debug logger.trace("pkgkind: ", kind);

        final switch (kind) {
        case PacketKind.counter:
            coll.put(demuxCounter(buf));
            break;
        case PacketKind.counterWithSampleRate:
            coll.put(demuxCounterWithSampleRate(buf));
            break;
        case PacketKind.timer:
            coll.put(demuxTimer(buf));
            break;
        case PacketKind.gauge:
            coll.put(demuxGauge(buf));
            break;
        case PacketKind.set:
            coll.put(demuxSet(buf));
            break;
        case PacketKind.testHost:
            // not supported mid stream. throwing away the value.
            demuxTestHost(buf);
            break;
        }
    }
}

private:

enum PacketKind : ubyte {
    counter,
    counterWithSampleRate,
    timer,
    gauge,
    set,
    testHost,
}

void serialize(Writer)(scope Writer w, const Timer v) {
    import msgpack_ll;

    mux(w, PacketKind.timer);
    mux(w, v.name);
    mux!(MsgpackType.int64)(w, v.value.total!"msecs");
}

void serialize(Writer)(scope Writer w, const Counter v) {
    import msgpack_ll;

    if (v.sampleRate.isNull) {
        mux(w, PacketKind.counter);
    } else {
        mux(w, PacketKind.counterWithSampleRate);
    }

    mux(w, v.name);

    mux!(MsgpackType.int64)(w, v.change);

    if (!v.sampleRate.isNull) {
        mux!(MsgpackType.float64)(w, v.sampleRate.get);
    }
}

void serialize(Writer)(scope Writer w, const Gauge v) {
    import msgpack_ll;

    mux(w, PacketKind.gauge);
    mux(w, v.name);
    mux!(MsgpackType.int64)(w, v.value);
}

void serialize(Writer)(scope Writer w, const Set v) {
    import msgpack_ll;

    mux(w, PacketKind.set);
    mux(w, v.name);
    mux!(MsgpackType.uint64)(w, v.value);
}

void serialize(Writer)(scope Writer w, const TestHost v) {
    import msgpack_ll;

    mux(w, PacketKind.testHost);
    mux(w, v.name);
}

void mux(Writer)(scope Writer w, string name) {
    import msgpack_ll;

    ubyte[5] name_m;
    // TODO a uint is potentially too big. standard says 2^32-1
    formatType!(MsgpackType.str32)(cast(uint) name.length, name_m);
    put(w, name_m[]);
    put(w, cast(ubyte[]) name);
}

void mux(MsgpackType type, T, Writer)(scope Writer w, T v) {
    import msgpack_ll;

    ubyte[DataSize!type] buf;
    formatType!type(v, buf);
    put(w, buf[]);
}

void mux(Writer)(scope Writer w, PacketKind kind) {
    import msgpack_ll;

    ubyte[DataSize!(MsgpackType.uint8)] pkgtype;
    formatType!(MsgpackType.uint8)(kind, pkgtype);
    put(w, pkgtype[]);
}

Counter demuxCounter(ref ubyte[] buf) {
    import msgpack_ll;

    auto name = BucketName(demux!string(buf));
    auto change = Counter.Change(demux!(MsgpackType.int64, long)(buf));

    return Counter(name, change);
}

Counter demuxCounterWithSampleRate(ref ubyte[] buf) {
    import msgpack_ll;

    auto name = BucketName(demux!string(buf));
    auto change = Counter.Change(demux!(MsgpackType.int64, long)(buf));
    auto sample_r = Counter.SampleRate(demux!(MsgpackType.float64, double)(buf));

    return Counter(name, change, sample_r);
}

Timer demuxTimer(ref ubyte[] buf) {
    import msgpack_ll;

    auto name = BucketName(demux!string(buf));
    auto ms = Timer.Value(demux!(MsgpackType.int64, long)(buf).dur!"msecs");

    return Timer(name, ms);
}

Gauge demuxGauge(ref ubyte[] buf) {
    import msgpack_ll;

    auto name = BucketName(demux!string(buf));
    auto v = Gauge.Value(demux!(MsgpackType.int64, long)(buf));

    return Gauge(name, v);
}

Set demuxSet(ref ubyte[] buf) {
    import msgpack_ll;

    auto name = BucketName(demux!string(buf));
    auto v = Set.Value(demux!(MsgpackType.uint64, ulong)(buf));

    return Set(name, v);
}

TestHost demuxTestHost(ref ubyte[] buf) {
    auto name = demux!string(buf);
    return TestHost(TestHost.Value(name));
}

void consume(MsgpackType type)(ref ubyte[] buf) {
    import msgpack_ll : DataSize;

    buf = buf[DataSize!type .. $];
}

void consume(ref ubyte[] buf, size_t len) {
    buf = buf[len .. $];
}

string demux(T)(ref ubyte[] buf) if (is(T == string)) {
    import std.exception : enforce;
    import std.utf : validate;
    import msgpack_ll;

    enforce(getType(buf[0]) == MsgpackType.str32);
    auto len = parseType!(MsgpackType.str32)(buf[0 .. DataSize!(MsgpackType.str32)]);
    consume!(MsgpackType.str32)(buf);

    // 2^32-1 according to the standard
    enforce(len < int.max);

    char[] raw_name = cast(char[]) buf[0 .. len];
    consume(buf, len);
    validate(raw_name);

    return raw_name.idup;
}

T demux(MsgpackType type, T)(ref ubyte[] buf) {
    import std.exception : enforce;
    import msgpack_ll;

    enforce(getType(buf[0]) == type);
    T v = parseType!type(buf[0 .. DataSize!type]);
    consume!type(buf);

    return v;
}

@("shall be the binary serialization of a collection")
unittest {
    import std.array : appender;

    // Arrange
    auto coll = new Collector;
    coll.put(Counter(BucketName("foo1"), Counter.Change(75)));
    coll.put(Counter(BucketName("foo2"), Counter.Change(63), Counter.SampleRate(0.1)));
    // gauge
    coll.put(Gauge(BucketName("foo"), Gauge.Value(81)));
    // timer
    coll.put(Timer(BucketName("bar"), Timer.Value(1000.dur!"msecs")));
    // set
    coll.put(Set(BucketName("gav"), Set.Value(32)));

    auto app = appender!(ubyte[]);

    // Act
    serialize(app, coll, TestHost(TestHost.Value("barf.some")));

    auto coll_dec = new CollectorAggregate;
    deserialize(app.data, coll_dec);

    // Assert
}

@("shall gracefully ignore invalid data when deserializing")
unittest {
    import std.array : appender;
    import metric_factory.dataformat.statsd : statdDeserialise = deserialize;

    auto coll = new Collector;
    auto app = appender!(ubyte[]);
    //app.put(cast(ubyte) 0);
    //statdDeserialise("foo1:75|c", coll);
    //
    //serialize(app, coll);
    //
    //auto coll_dec = new Collector;
    //deserialize(app.data, coll_dec);
}

@("shall be a serialized/deserialized TestHost")
unittest {
    import std.array : appender;

    // arrange
    auto app = appender!(ubyte[]);
    auto in_host = TestHost(TestHost.Value("barf.some"));

    // act
    serialize(app, in_host);
    auto buf = app.data;
    auto kind = demux!(MsgpackType.uint8, ubyte)(buf);
    auto out_host = demuxTestHost(buf);

    assert(kind == PacketKind.testHost);
    assert(in_host.toHash == out_host.toHash);
}
