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
    registerPlugin(Plugin("List files in current directory", (coll) {
            simpleListdir(coll, DirPath("."));
        }));
    registerPlugin(Plugin("Create and remove 10k small files", (coll) {
            testSmallFilePerformance(coll, DirPath("."));
        }));
}

// #SPC-infrastructure_io-listdir
void simpleListdir(Collector coll, DirPath root) nothrow {
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
void testSmallFilePerformance(Collector coll, DirPath root) nothrow {
    auto wa = WorkArea(root);
    if (!wa.isValid)
        return;

    auto rnd_testdir = wa.root;

    try {
        auto sw = StopWatch(AutoStart.yes);
        foreach (i; 0 .. 10_000) {
            auto fout = File(buildPath(rnd_testdir, i.to!string), "w");
            iota(0, 10_000).each!(a => fout.write(a));
        }
        sw.stop;
        coll.put(Timer.from(sw, "create_10k_small_files"));
    }
    catch (Exception e) {
        collectException(logger.warning(e.msg));
    }

    try {
        auto sw = wa.cleanup;
        coll.put(Timer.from(sw, "remove_10k_small_files"));
    }
    catch (Exception e) {
    }
}
