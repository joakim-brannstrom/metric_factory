/**
Copyright: Copyright (c) 2017, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)
*/
module metric_factory.metric.types;

public import core.time : Duration, dur;

import std.typecons : Nullable;

struct BucketName {
    string payload;
    alias payload this;

    this(string p) nothrow {
        import std.algorithm : map;
        import std.array : array;
        import std.uni : isAlphaNum;
        import std.utf : byDchar, toUTF8;

        try {
            this.payload = p.byDchar.map!(a => a.isAlphaNum ? cast(dchar) a
                    : cast(dchar) '_').array().toUTF8;
        }
        catch (Exception e) {
            this.payload = p;
        }
    }
}

/** Counters count the number of times an event occurs.
 *
 * #SPC-concept-counters
 *
 * # SampleRate
 * How often the counter is sampled.
 *
 * Useful when the events are reduced to limit flooding. On the receiving end
 * the change is amplified by the inverse of the sampling rate.
 *
 * If only every tenth event is recorded the sampling rate would be 0.1
 * If the change sent to the server is 1.
 * It is amplified to 1 / 0.1 = 10.
 */
struct Counter {
    struct SampleRate {
        double payload;
        alias payload this;
    }

    struct Change {
        long payload;
        alias payload this;
    }

    private BucketName name_;

    /// How much the counter is changed.
    private Change change_;

    private Nullable!SampleRate sample_r;

    this(BucketName name_, Change change_, SampleRate sample_r) {
        this.name_ = name_;
        this.change_ = change_;
        this.sample_r = sample_r;
    }

    this(BucketName name_, Change change_) {
        this.name_ = name_;
        this.change_ = change_;
    }

    this(BucketName name_) {
        this(name_, Change(1));
    }

    @property auto name() const {
        return name_;
    }

    @property auto change() const {
        return change_;
    }

    @property auto sampleRate() const {
        return sample_r;
    }

    import std.format : FormatSpec;

    void toString(Writer, Char)(scope Writer w, FormatSpec!Char fmt) @safe const {
        import std.format : formattedWrite;

        if (sample_r.isNull) {
            formattedWrite(w, "Counter(%s, %s)", name, change);
        } else {
            formattedWrite(w, "Counter(%s, %s, %s)", name, change, sample_r.get);
        }
    }

    string toString() @safe const {
        import std.exception : assumeUnique;
        import std.format : FormatSpec;

        char[] buf;
        buf.reserve(100);
        auto fmt = FormatSpec!char("%s");
        toString((const(char)[] s) { buf ~= s; }, fmt);
        auto trustedUnique(T)(T t) @trusted {
            return assumeUnique(t);
        }

        return trustedUnique(buf);
    }
}

/// #SPC-concept-gauges
struct Gauge {
    struct Value {
        long payload;
        alias payload this;
    }

    private BucketName name_;
    private Value value_;

    this(BucketName n, Value v) {
        this.name_ = n;
        this.value_ = v;
    }

    @property auto name() const {
        return name_;
    }

    @property auto value() const {
        return value_;
    }
}

/// #SPC-concept-timers
struct Timer {
    import std.datetime : StopWatch;

    // How long a certain task took to complete.
    struct Value {
        Duration payload;
        alias payload this;
    }

    private BucketName name_;
    private Value value_;

    this(BucketName n, Value v) {
        this.name_ = n;
        this.value_ = v;
    }

    @property auto name() const {
        return name_;
    }

    @property auto value() const {
        return value_;
    }

    int opCmp(this rhs) @safe pure nothrow const {
        return this.value_.opCmp(rhs.value_);
    }

    static Timer from(ref StopWatch sw, string name) {
        import std.conv : to;

        return Timer(BucketName(name), Timer.Value(sw.peek.to!Duration));
    }
}

/** Sets track the number of unique elements belonging to a group.
 *
 * #SPC-concept-sets
 */
struct Set {
    struct Value {
        long payload;
        alias payload this;
    }

    private BucketName name_;
    private Value value_;

    this(BucketName name_, Value value_) {
        this.name_ = name_;
        this.value_ = value_;
    }

    @property auto name() const {
        return name_;
    }

    @property auto value() const {
        return value_;
    }
}

/// The name of a host the tests where ran on.
struct TestHost {
    private Value name_;
    private Hash hash_;

    struct Value {
        string payload;
        alias payload this;
    }

    struct Hash {
        ulong payload;
        alias payload this;
    }

    this(Value n) {
        this.name_ = n;
        this.hash_ = TestHost.makeHash(n);
    }

    @property auto name() @safe pure nothrow const @nogc {
        return name_;
    }

    @property auto toHash() @safe pure nothrow const @nogc {
        return hash_;
    }

    /// Make a hash out of the raw data.
    static private auto makeHash(string raw) @safe pure nothrow @nogc {
        import std.digest.crc;

        ulong value = 0;

        if (raw is null)
            return Hash(0);
        ubyte[4] hash = crc32Of(raw);
        return Hash(value ^ ((hash[0] << 24) | (hash[1] << 16) | (hash[2] << 8) | hash[3]));
    }
}
