/**
Copyright: Copyright (c) 2017, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)

This file contains some simple plugins with no external dependency to test the
I/O of an infrastructure.

#SPC-infrastructure_io
*/
module metric_plugin.infrastructure_io;

import std.datetime : StopWatch, AutoStart;
import logger = std.experimental.logger;

import metric_factory.plugin;
import metric_factory.types;

import scriptlike;

shared static this() {
    // dfmt off
    buildPlugin
        .description("List files in current directory")
        .func((coll) {
            simpleListdir(coll, DirPath("."));
        })
    .register;
    buildPlugin
        .description("Create and remove 10k small files")
        .func((coll) {
            testSmallFilePerformance(coll, DirPath("."), 10_000);
        })
    .register;
    // dfmt on
}

// #SPC-infrastructure_io-listdir
void simpleListdir(MetricValueStore coll, DirPath root) nothrow {
    try {
        Args a;
        a.put("ls");
        a.put(scriptlike.Path(cast(string) root));

        auto sw = StopWatch(AutoStart.yes);
        runCollect(a.data);
        sw.stop;

        coll.put(Timer.from(sw, "listdir"));
    }
    catch (Exception e) {
        collectException(logger.error(e.msg));
    }
}

/** Reuse this plugin to test different filesystems.
 *
 * #SPC-infrastructure_io-smallfile_perf
 */
void testSmallFilePerformance(MetricValueStore coll, const DirPath root, const long files_to_create) nothrow {
    import std.algorithm : map, among;
    import std.array : array;
    import std.format : format;
    import std.random : uniform;
    import std.utf;

    auto wa = WorkArea(root);
    if (!wa.isValid)
        return;

    auto rnd_testdir = wa.root;

    string path_n;

    try {
        path_n = (cast(string) root).byDchar.map!(a => a.among('.', '/')
                ? cast(dchar) '_' : a).array().toUTF8();
    }
    catch (Exception e) {
        collectException(logger.warning("Unable to create a filename"));
        return;
    }

    try {
        auto sw = StopWatch(AutoStart.yes);
        foreach (i; 0 .. files_to_create) {
            auto fout = File(buildPath(rnd_testdir, i.to!string), "w");
            foreach (_; 0 .. 7000) {
                fout.write(uniform!int);
            }
        }
        sw.stop;
        coll.put(Timer.from(sw, format("create_%s_small_files_%s", files_to_create, path_n)));
    }
    catch (Exception e) {
        collectException(logger.warning(e.msg));
    }

    try {
        auto sw = wa.cleanup;
        coll.put(Timer.from(sw, format("remove_%s_small_files_%s", files_to_create, path_n)));
    }
    catch (Exception e) {
    }
}
