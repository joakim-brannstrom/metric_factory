/**
Copyright: Copyright (c) 2017, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)
*/
static import std.getopt;

static import core.stdc.stdlib;
import std.exception : collectException;
import logger = std.experimental.logger;

import metric_factory.metric;
import metric_factory.types;
import metric_factory.sqlite3;

/// Run metric tests on the host.
struct TestHost {
    string payload;
    alias payload this;
}

enum OutputKind {
    csv,
    statsd,
    mfbin,
}

enum RunMode {
    standalone,
    remote,
    plugin_id,
    plugin_group,
    plugin_list,
    master,
    db_to_csv
}

int main(string[] args) {
    import std.datetime : Clock;
    import std.format : format;
    import std.traits : EnumMembers;
    import metric_factory.plugin : registeredPlugins;

    static import metric_factory.dataformat.statsd;

    const curr_t = Clock.currTime;

    bool help;
    bool debug_;
    bool output_to_db;
    size_t plugin_id;
    RunMode run_mode;
    OutputKind output_kind;
    TestHost[] test_hosts;
    string output_file;
    string[] plugin_group;

    std.getopt.GetoptResult help_info;
    try {
        // dfmt off
        help_info = std.getopt.getopt(args,
            "d|debug", "run in debug mode when logging", &debug_,
            "host", "host(s) to run the test suite on", &test_hosts,
            "run", "mode to run the tests in. Either standalone or from a remote collector "  ~ format("[%(%s|%)]", [EnumMembers!RunMode]), &run_mode,
            "plugin-id", "run the specific plugin with the ID", &plugin_id,
            "plugin-group", "run the plugins belonging to the group", &plugin_group,
            "output", "file to write the result to", &output_file,
            "output-kind", "format to write the result in " ~ format("[%(%s|%)]", [EnumMembers!OutputKind]), &output_kind,
            "output-to-db", "put the results into a sqlite3 DB", &output_to_db,
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

    if (output_file.length == 0) {
        output_file = format("result_%s-%s-%s_%sh_%sm_%ss", curr_t.year,
                cast(ushort) curr_t.month, curr_t.day, curr_t.hour, curr_t.minute, curr_t.second);
    }

    if (run_mode == RunMode.master && test_hosts.length == 0) {
        logger.error("mode master requires at least one --host");
        return 1;
    }

    Database db;
    try {
        db = Database.make;
    }
    catch (Exception e) {
        logger.error(e.msg);
        return 1;
    }

    logger.info("Registered plugins: ", registeredPlugins);

    auto coll = new CollectorAggregate;

    final switch (run_mode) {
    case RunMode.standalone:
        standaloneMetrics(coll);
        break;
    case RunMode.plugin_id:
        standaloneMetrics(coll, plugin_id);
        break;
    case RunMode.plugin_group:
        standaloneMetrics(coll, plugin_group);
        break;
    case RunMode.remote:
        standaloneMetrics(coll, plugin_group);
        break;
    case RunMode.master:
        runMetricSuiteOnTestHosts(coll, test_hosts, plugin_group);
        break;
    case RunMode.plugin_list:
        listPlugins();
        break;
    case RunMode.db_to_csv:
        runDatabaseToCsv(Path(output_file), db);
    }

    final switch (run_mode) {
    case RunMode.standalone:
        toFile(Path(output_file), coll, output_kind);
        if (output_to_db)
            toDatabase(coll, db);
        break;
    case RunMode.plugin_id:
        toFile(Path(output_file), coll, output_kind);
        if (output_to_db)
            toDatabase(coll, db);
        break;
    case RunMode.plugin_group:
        toFile(Path(output_file), coll, output_kind);
        if (output_to_db)
            toDatabase(coll, db);
        break;
    case RunMode.remote:
        toFile(Path(output_file), coll, OutputKind.mfbin);
        break;
    case RunMode.master:
        toFile(Path(output_file), coll, output_kind);
        if (output_to_db)
            toDatabase(coll, db);
        break;
    case RunMode.plugin_list:
        break;
    case RunMode.db_to_csv:
        break;
    }

    return 0;
}

void printHelp(string[] args, std.getopt.GetoptResult help_info) {
    import std.getopt : defaultGetoptPrinter;
    import std.format : format;
    import std.path : baseName;

    defaultGetoptPrinter(format("usage: %s\n", args[0].baseName), help_info.options);
}

/** Run the test suite on test hosts and collect the result.
 *
 * #SPC-remote_test_host_execution
 */
void runMetricSuiteOnTestHosts(CollectorAggregate coll, TestHost[] test_hosts,
        const string[] in_plugin_group) nothrow {
    import core.sys.posix.stdlib : mkdtemp;
    import std.format : format;
    import std.random : uniform;
    import scriptlike;

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

    string[] plugin_group_args = in_plugin_group.map!(a => ["--plugin-group",
            a.idup]).joiner.array();

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

            runCmd(["ssh", "-oStrictHostKeyChecking=no", host, "mkdir", rnd_hostdir]);
            runCmd(["scp", "-oStrictHostKeyChecking=no", "-B", this_bin,
                    format("%s:%s", host, rnd_hostbin)]);
            runCmd(["ssh", "-oStrictHostKeyChecking=no", host, rnd_hostbin, "--run", "remote",
                    "--output-kind", "mfbin", "--output", rnd_hostresult] ~ plugin_group_args);
            runCmd(["scp", "-oStrictHostKeyChecking=no", "-B", format("%s:%s",
                    host, rnd_hostresult), retrieved_result]);

            auto file_data = cast(ubyte[]) std.file.read(retrieved_result);
            import metric_factory.dataformat.mfbin;

            deserialize(file_data, coll);
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

void listPlugins() {
    import metric_factory.plugin;

    foreach (idx, p; getPlugins) {
        logger.infof("Plugin %s (%s): %s", idx, p.group, p.name);
    }
}

void standaloneMetrics(CollectorAggregate coll) {
    import metric_factory.plugin;

    foreach (p; getPlugins) {
        logger.info("Run plugin: ", p.name);
        p.func(coll);
    }
}

void standaloneMetrics(CollectorAggregate coll, size_t plugin_id) {
    import metric_factory.plugin;

    if (plugin_id >= registeredPlugins) {
        logger.errorf("plugin id %s do not exist", plugin_id);
        return;
    }

    auto p = getPlugins()[plugin_id];
    logger.info("Run plugin: ", p.name);
    p.func(coll);
}

void standaloneMetrics(CollectorAggregate coll, string[] plugin_group) {
    import metric_factory.plugin;

    foreach (p; getPlugins(plugin_group)) {
        logger.info("run plugin: ", p.name);
        p.func(coll);
    }
}

void runDatabaseToCsv(Path output_file, ref Database db) {
    import std.conv : to;
    import std.stdio : File;
    import std.path : extension, setExtension;
    import metric_factory.csv : putCSV, putCSVHeader;
    import metric_factory.plugin : hostname;
    import metric_factory.metric.types : TestHost;

    output_file = Path(output_file.setExtension("csv"));
    auto fout = File(output_file, "w");

    putCSVHeader((const(char)[] a) { fout.write(a); });
    size_t index;

    foreach (metric; db.getMetrics) {
        auto coll = db.get(metric.id);

        auto res = process(coll);

        auto raw_test_host = db.getTestHost(metric.testHostId);

        TestHost test_host;
        if (!raw_test_host.isNull)
            test_host = raw_test_host.get;

        putCSV((const(char)[] a) { fout.write(a); }, metric.timestamp, res, index, test_host.name);
    }
}

void toFile(Path output_file, CollectorAggregate coll, const OutputKind kind) {
    import std.conv : to;
    import std.stdio : File;
    import std.path : extension, setExtension;
    import metric_factory.csv : putCSV, putCSVHeader;
    import metric_factory.plugin : hostname;

    static import metric_factory.dataformat.statsd;

    static import metric_factory.dataformat.mfbin;

    if (output_file.payload.extension is null) {
        string ext = kind.to!string;
        output_file = Path(output_file.setExtension(ext));
    }

    auto fout = File(output_file, "w");

    final switch (kind) {
    case OutputKind.csv:
        auto res = process(coll);
        putCSVHeader((const(char)[] a) { fout.write(a); });
        size_t index;
        putCSV((const(char)[] a) { fout.write(a); }, res, index);
        break;
    case OutputKind.statsd:
        metric_factory.dataformat.statsd.serialize((const(char)[] a) {
            fout.write(a);
        }, coll.globalAggregate);
        break;
    case OutputKind.mfbin:
        auto hname = metric_factory.metric.types.TestHost(
                metric_factory.metric.types.TestHost.Value(hostname));
        metric_factory.dataformat.mfbin.serialize((const(ubyte)[] a) {
            fout.rawWrite(a);
        }, coll.globalAggregate, hname);
        break;
    }
}

void toDatabase(CollectorAggregate coll, ref Database db) {
    import metric_factory.plugin : getPlugins;
    import metric_factory.types : Timestamp;

    const ts = Timestamp.make;
    db.put(ts, coll.globalAggregate);

    foreach (c; coll.hostAggregate.byKeyValue) {
        auto test_host = c.key in coll.testHosts;
        db.put(*test_host, ts, c.value);
    }
}

string pathToBinary() {
    import std.file : readLink, thisExePath;

    string path_to_binary;

    try {
        path_to_binary = readLink("/proc/self/exe");
    }
    catch (Exception ex) {
        collectException(logger.warning("Unable to read the symlink '/proc/self/exe': ", ex.msg));
    }

    if (path_to_binary.length == 0) {
        path_to_binary = thisExePath;
    }

    return path_to_binary;
}

auto runCmd(string[] cmds) {
    import scriptlike : Args, runCollect;

    Args a;
    foreach (c; cmds) {
        a.put(c);
    }

    return runCollect(a.data);
}
