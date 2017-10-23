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

import scriptlike;

shared static this() {
    registerPlugin(Plugin("listdir", &simpleListdir));
}

// #SPC-infrastructure_io-listdir
void simpleListdir(Collector coll) nothrow {
    try {
        auto sw = StopWatch(AutoStart.yes);
        runCollect("ls").yap;
        sw.stop;
        coll.put(Timer(BucketName("listdir"), Timer.Value(sw.peek.to!Duration)));
    }
    catch (Exception e) {
        collectException(logger.error(e.msg));
    }
}
