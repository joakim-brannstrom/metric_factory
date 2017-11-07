/**
Copyright: Copyright (c) 2017, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)

This file contains some simple tests for gathering server statistics.
*/
module metric_plugin.serverstat;

import std.datetime : StopWatch, AutoStart;
import std.exception : collectException;
import std.conv : to;
import std.string : strip;
import std.format : format;
import std.math : round;

import logger = std.experimental.logger;

import metric_factory.plugin;
import metric_factory.types;

import scriptlike;

shared static this() {
    buildPlugin.group("builtin").description("Users logged in").func(&measureUsers).register;
    buildPlugin.group("builtin").description("Loadavg").func(&measureLoadAvg).register;
    buildPlugin.group("builtin").description("Active process")
        .func(&measureActiveProcesses).register;
    buildPlugin.group("builtin").description("Program resource usage")
        .func(&measurePrograms).register;
}

void measureUsers(MetricValueStore coll) nothrow {
    try {
        auto res = runCollect(`users| tr ' ' '\n'|uniq|wc -l`);
        long cnt = res.strip.to!long;
        coll.put(Counter(BucketName("count_users"), Counter.Change(cnt)));
    }
    catch (Exception e) {
        collectException(logger.warning(e.msg));
    }
}

void measureLoadAvg(MetricValueStore coll) nothrow {
    try {
        auto res = std.file.readText(`/proc/loadavg`).split;
        if (res.length != 5) {
            logger.warning("/proc/loadavg has abnormal values: ", res);
            return;
        }

        coll.put(Gauge(BucketName("loadavg_1min"), Gauge.Value(cast(long) res[0].to!double.round)));
        coll.put(Gauge(BucketName("loadavg_5min"), Gauge.Value(cast(long) res[1].to!double.round)));
        coll.put(Gauge(BucketName("loadavg_15min"),
                Gauge.Value(cast(long) res[2].to!double.round)));
    }
    catch (Exception e) {
        collectException(logger.warning(e.msg));
    }
}

void measureActiveProcesses(MetricValueStore coll) nothrow {
    try {
        auto res = std.file.readText(`/proc/loadavg`).split;
        if (res.length != 5) {
            return;
        }

        auto procs = res[3].split('/');
        if (procs.length != 2)
            return;

        coll.put(Gauge(BucketName("active_processes"), Gauge.Value(procs[1].to!long)));
    }
    catch (Exception e) {
        collectException(logger.warning(e.msg));
    }
}

void measurePrograms(MetricValueStore coll) nothrow {
    import std.ascii : newline;

    immutable top_n = 5;
    // on linux the columns for ps aux are:
    // USER       PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND

    // memory
    try {
        auto res = runCollect(`ps aux --no-headers --sort -size`);
        debug logger.trace(res);
        foreach (line; res.splitter(newline).take(top_n)) {
            auto cols = line.split;
            if (cols.length < 11)
                break;

            auto user = cols[0];
            auto mem_percentage = cast(long) cols[3].to!double.round;
            // residential memory
            auto mem_rss = cols[4].to!long;
            auto cmd = cols[10];

            coll.put(Gauge(BucketName(format("user_mem_percentage_%s", user)),
                    Gauge.Value(mem_percentage)));
            coll.put(Gauge(BucketName(format("user_mem_rss_kbyte_%s", user)),
                    Gauge.Value(mem_rss)));
            coll.put(Gauge(BucketName(format("program_mem_percentage_%s",
                    cmd)), Gauge.Value(mem_percentage)));
            coll.put(Gauge(BucketName(format("program_mem_rss_kbyte_%s", cmd)),
                    Gauge.Value(mem_rss)));
        }
    }
    catch (Exception e) {
        collectException(logger.warning(e.msg));
    }

    // cpu
    try {
        auto res = runCollect(`ps aux --no-headers --sort -pcpu`);
        foreach (line; res.splitter(newline).take(top_n)) {
            auto cols = line.split;
            if (cols.length < 11)
                break;

            auto user = cols[0];
            auto cpu_percentage = cast(long) cols[2].to!double.round;
            auto cmd = cols[10];

            coll.put(Gauge(BucketName(format("user_cpu_percentage_%s", user)),
                    Gauge.Value(cpu_percentage)));
            coll.put(Gauge(BucketName(format("program_cpu_percentage_%s",
                    cmd)), Gauge.Value(cpu_percentage)));
        }
    }
    catch (Exception e) {
        collectException(logger.warning(e.msg));
    }
}
