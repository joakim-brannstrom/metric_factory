/**
Copyright: Copyright (c) 2017, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)
*/
module metric_factory.plugin;

public import metric_factory.metric;
import std.array : Appender;

alias MetricFunc = void function(Collector) nothrow;

struct Plugin {
    string name;
    MetricFunc func;
}

void registerPlugin(Plugin f) {
    synchronized {
        (cast(Appender!(Plugin[])) registered_plugins).put(f);
    }
}

size_t registeredPlugins() nothrow @nogc {
    return (cast(Appender!(Plugin[])) registered_plugins).data.length;
}

Plugin[] getPlugins() nothrow {
    return (cast(Appender!(Plugin[])) registered_plugins).data;
}

private:

shared Appender!(Plugin[]) registered_plugins;
