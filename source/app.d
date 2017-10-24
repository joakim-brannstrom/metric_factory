/**
Copyright: Copyright (c) 2017, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)
*/
import scriptlike;

static import std.getopt;

static import core.stdc.stdlib;
import logger = std.experimental.logger;

import metric_factory.metric;

/// Run metric tests on the host.
struct TestHost {
    string payload;
    alias payload this;
}

enum OutputKind {
    dat,
    csv
}

enum RunMode {
    standalone,
    remote
}

int main(string[] args) {
    import std.traits : EnumMembers;

    const curr_t = Clock.currTime;

    bool help;
    bool debug_;
    RunMode run_mode;
    OutputKind output_kind;
    TestHost[] test_hosts;
    string output_file = format("result_%s-%s-%s_%sh_%sm_%ss.dat", curr_t.year,
            cast(ushort) curr_t.month, curr_t.day, curr_t.hour, curr_t.minute, curr_t.second);

    std.getopt.GetoptResult help_info;
    try {
        // dfmt off
        help_info = std.getopt.getopt(args,
            "d|debug", "run in debug mode when logging", &debug_,
            "host", "host(s) to run the test suite on", &test_hosts,
            "run", "mode to run the tests in. Either standalone or from a remote collector "  ~ format("[%(%s|%)]", [EnumMembers!RunMode]), &run_mode,
            "output-kind", "format to write the result in " ~ format("[%(%s|%)]", [EnumMembers!OutputKind]), &output_kind,
            "output", "file to write the result to", &output_file,
            );
        // dfmt on
        help = help_info.helpWanted;
    }
    catch (std.getopt.GetOptException ex) {
        // unknown option
        help = true;
    }
    catch (Exception ex) {
        help = true;
    }

    if (debug_) {
        static import scriptlike;

        logger.globalLogLevel(logger.LogLevel.trace);
        scriptlike.scriptlikeEcho = true;
    } else {
        logger.globalLogLevel(logger.LogLevel.info);
    }

    if (help) {
        printHelp(args, help_info);
        return 0;
    }

    auto coll = new Collector;

    if (run_mode == RunMode.standalone && test_hosts.length == 0) {
        standaloneMetrics(coll);
        auto res = process(coll);
        auto fout = File(output_file, "w");
        if (output_kind == OutputKind.dat) {
            writeCollection(coll, (const(char)[] a) { fout.write(a); });
        } else {
            writeResult(res, (const(char)[] a) { fout.write(a); });
        }
    } else if (run_mode == RunMode.remote) {
        remoteMetrics(coll);
        auto fout = File(output_file, "w");
        writeCollection(coll, (const(char)[] a) { fout.write(a); });
    } else {
        runMetricSuiteOnTestHosts(coll, test_hosts);
        auto res = process(coll);
        auto fout = File(output_file, "w");
        if (output_kind == OutputKind.dat) {
            writeCollection(coll, (const(char)[] a) { fout.write(a); });
        } else {
            writeResult(res, (const(char)[] a) { fout.write(a); });
        }
    }

    return 0;
}

void printHelp(string[] args, std.getopt.GetoptResult help_info) {
    import std.getopt : defaultGetoptPrinter;
    import std.format : format;
    import std.path : baseName;

    defaultGetoptPrinter(format("usage: %s\n", args[0].baseName), help_info.options);
}

// #SPC-parallell_test_host_execution
void runMetricSuiteOnTestHosts(Collector coll, TestHost[] test_hosts) nothrow {
    import core.sys.posix.stdlib : mkdtemp;
    import std.random : uniform;
    import std.format : format;

    string result_dir;

    {
        char[] tmp = "results_XXXXXX".dup;
        mkdtemp(tmp.ptr);
        result_dir = tmp.idup;
    }

    void cleanup() nothrow {
        try {
            tryRmdirRecurse(result_dir);
        }
        catch (Exception e) {
            collectException(logger.error(e.msg));
        }
    }

    scope (exit)
        cleanup;

    string this_bin;
    try {
        this_bin = pathToBinary;
    }
    catch (Exception e) {
        collectException(logger.error(e.msg));
        return;
    }

    foreach (host; test_hosts) {
        string rnd_hostdir;
        try {
            logger.info("gather metrics from ", host);
            // TODO /tmp/ should be configurable
            rnd_hostdir = format("/tmp/metric_factory_%s", uniform(1, 2_000_000_000));
            const rnd_hostbin = buildPath(rnd_hostdir, this_bin.baseName);
            const rnd_hostresult = buildPath(rnd_hostdir, "result.dat");
            const retrieved_result = buildPath(result_dir, format("%s_%s",
                    host, uniform(1, 2_000_000_000)));

            runCmd(["ssh", host, "mkdir", rnd_hostdir]);
            runCmd(["scp", "-B", this_bin, format("%s:%s", host, rnd_hostbin)]);
            runCmd(["ssh", host, rnd_hostbin, "--run", "remote", "--output", rnd_hostresult]);
            runCmd(["scp", "-B", format("%s:%s", host, rnd_hostresult), retrieved_result]);

            auto fin = File(retrieved_result);
            foreach (l; fin.byLine) {
                deserialise(l, coll);
            }
        }
        catch (Exception e) {
            collectException(logger.error(e.msg));
        }

        if (rnd_hostdir.length != 0) {
            try {
                runCmd(["ssh", host, "rm", "-r", rnd_hostdir]);
            }
            catch (Exception e) {
                collectException(logger.warning(e.msg));
            }
        }
    }
}

auto runCmd(string[] cmds) {
    Args a;
    foreach (c; cmds) {
        a.put(c);
    }

    return runCollect(a.data);
}

void standaloneMetrics(Collector coll) {
    import metric_factory.plugin;

    logger.info("Registered plugins: ", registeredPlugins);

    foreach (p; getPlugins) {
        logger.info("run plugin: ", p.name);
        p.func(coll);
    }
}

void remoteMetrics(Collector coll) {
    import metric_factory.plugin;

    logger.info("Registered plugins: ", registeredPlugins);

    foreach (p; getPlugins) {
        logger.info("run plugin: ", p.name);
        p.func(coll);
    }
}

void writeResult(Writer)(ProcessResult res, scope Writer w) {
    import std.ascii : newline;
    import std.datetime;
    import std.format : formattedWrite;
    import std.range.primitives : put;
    import metric_factory.csv;

    auto curr_d = Clock.currTime;
    auto curr_d_txt = format("%s-%s-%s %s:%s:%s", curr_d.year,
            cast(ushort) curr_d.month, curr_d.day, curr_d.hour, curr_d.minute, curr_d.second);

    writeCSV(w, "description", "datetime", "min", "max", "sum", "mean");
    foreach (kv; res.timers.byKeyValue) {
        writeCSV(w, kv.key, curr_d_txt, kv.value.min.total!"msecs",
                kv.value.max.total!"msecs", kv.value.sum.total!"msecs",
                kv.value.mean.total!"msecs");
    }

    writeCSV(w, "description", "datetime", "count");
    foreach (kv; res.counters.byKeyValue) {
        // TODO currently the changePerSecond isn't useful because this empties directly.
        //writeCSV(w, kv.key, kv.value.change, kv.value.changePerSecond);
        writeCSV(w, kv.key, curr_d_txt, kv.value.change);
    }
}

string pathToBinary() {
    import std.file : readLink, thisExePath;

    string path_to_binary;

    try {
        path_to_binary = std.file.readLink("/proc/self/exe");
    }
    catch (Exception ex) {
        collectException(logger.warning("Unable to read the symlink '/proc/self/exe': ", ex.msg));
    }

    if (path_to_binary.length == 0) {
        path_to_binary = thisExePath;
    }

    return path_to_binary;
}
