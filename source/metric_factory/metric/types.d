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
    import std.format : FormatSpec;

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

    void toString(Writer, Char)(scope Writer w, FormatSpec!Char fmt) const {
        import std.format : formattedWrite;

        if (sample_r.isNull) {
            formattedWrite(w, "%s:%s|c", name_, change_);
        } else {
            formattedWrite(w, "%s:%s|c|@%f", name_, change_, sample_r);
        }
    }
}

/// #SPC-concept-gauges
struct Gauge {
    import std.format : FormatSpec;

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

    void toString(Writer, Char)(scope Writer w, FormatSpec!Char fmt) const {
        import std.format : formattedWrite;

        formattedWrite(w, "%s:%s|g", name, value);
    }
}

/// #SPC-concept-timers
struct Timer {
    import std.format : FormatSpec;
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

    void toString(Writer, Char)(scope Writer w, FormatSpec!Char fmt) const {
        import std.format : formattedWrite;

        formattedWrite(w, "%s:%s|ms", name_, value_.total!"msecs");
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
    import std.format : FormatSpec;

    struct Value {
        ulong payload;
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

    void toString(Writer, Char)(scope Writer w, FormatSpec!Char fmt) const {
        import std.format : formattedWrite;
        import std.conv : to;

        formattedWrite(w, "%s:%s|s", name_, value_);
    }
}
