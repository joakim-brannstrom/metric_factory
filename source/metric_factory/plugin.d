/**
Copyright: Copyright (c) 2017, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)
*/
module metric_factory.plugin;

public import metric_factory.metric;
import std.exception : collectException;
import logger = std.experimental.logger;

import metric_factory.types : Path, DirPath;

alias MetricFunc = void function(MetricValueStore) nothrow;

struct Plugins {
    Plugin[] registered;
    alias registered this;

    @disable this(this);

    void put(Plugin p) nothrow {
        registered ~= p;
    }
}

struct Plugin {
    enum Target {
        allHost,
        single,
    }

    enum Execute {
        single,
        parallel,
    }

    string name;
    string group;
    MetricFunc func;
    Target target;
    Execute exec;
}

/// Use to build a plugin finishing with registering it.
auto buildPlugin() {
    static struct BuildPlugin {
        private {
            string desc_;
            string group_;
            MetricFunc func_;
            Plugin.Target target;
            Plugin.Execute exec;
        }

        auto description(string v) {
            desc_ = v;
            return this;
        }

        auto group(string v) {
            group_ = v;
            return this;
        }

        auto func(MetricFunc v) {
            func_ = v;
            return this;
        }

        auto singleTarget() {
            target = Plugin.Target.single;
            return this;
        }

        auto parallelExecute() {
            exec = Plugin.Execute.parallel;
            return this;
        }

        void register() {
            registerPlugin(Plugin(desc_, group_, func_, target, exec));
        }
    }

    return BuildPlugin();
}

private void registerPlugin(Plugin f) {
    synchronized {
        (cast(Plugins) registered_plugins).put(f);
    }
}

size_t registeredPlugins() nothrow @nogc {
    return (cast(Plugins) registered_plugins).length;
}

Plugin[] getPlugins() nothrow {
    return (cast(Plugins) registered_plugins);
}

Plugin[] getPlugins(const string[] groups) nothrow {
    import std.array : array;
    import std.algorithm : canFind, filter;

    if (groups.length == 0)
        return getPlugins();

    return (cast(Plugins) registered_plugins).registered.filter!(
            a => groups.canFind(a.group)).array();
}

struct ShellScriptResult {
    import std.typecons : Nullable;

    Nullable!Duration script;
    Nullable!Duration cleanup;
}

/** Run a shell script in a unique directory with cleanup.
 *
 * This is NOT thread safe because it calls chdir.
 *
 * Params:
 *  root = directory to create the unique directory in to run the script in.
 *  shell = shell to run as
 *  script = the raw script data
 */
ShellScriptResult runShellScript(DirPath root, string shell, string script) nothrow {
    import std.conv : to;
    import std.datetime : StopWatch, Duration, AutoStart;
    import std.file : mkdir, getcwd, chdir;
    import std.format : format;
    import std.path : buildPath;
    import std.random : uniform;
    import std.stdio : File;

    static import scriptlike;

    ShellScriptResult rval;

    DirPath rnd_testdir;
    try {
        rnd_testdir = buildPath(root, format("metric_factory_%s", uniform(0,
                2_000_000_000).to!string)).DirPath;
    }
    catch (Exception e) {
        collectException(logger.error(e.msg));
        return rval;
    }

    // this is to be on the safe side
    scope (exit)
        nothrowTryRmdirRecursive(rnd_testdir);

    try {
        auto orig_wd = getcwd();
        mkdir(rnd_testdir);
        chdir(rnd_testdir);
        scope (exit)
            chdir(orig_wd);

        auto testscript_file = format("metric_factory_%s", uniform(0, 2_000_000_000));
        File(testscript_file, "w").write(script);

        auto sw = StopWatch(AutoStart.yes);
        auto res = scriptlike.runCollect(format("%s %s", shell, testscript_file));
        debug logger.trace(res);
        sw.stop;
        rval.script = sw.peek.to!Duration;
    }
    catch (Exception e) {
        collectException(logger.warning(e.msg));
    }

    try {
        auto sw = StopWatch(AutoStart.yes);
        scriptlike.tryRmdirRecurse(cast(string) rnd_testdir);
        sw.stop;
        rval.cleanup = sw.peek.to!Duration;
    }
    catch (Exception e) {
        collectException(logger.warning(e.msg));
    }

    return rval;
}

void nothrowTryRmdirRecursive(DirPath p) nothrow {
    import scriptlike;

    try {
        tryRmdirRecurse(cast(string) p);
    }
    catch (Exception e) {
    }
}

struct WorkArea {
    import std.datetime : StopWatch;
    import std.path : buildPath;
    import std.random : uniform;
    import std.conv : to;

    const DirPath root;

    @disable this(this);

    this(DirPath root) nothrow {
        import std.file : mkdir;

        try {
            auto rnd_testdir = buildPath(root, "metric_factory_" ~ uniform(0,
                    2_000_000_000).to!string).DirPath;
            mkdir(rnd_testdir);
            this.root = rnd_testdir;
        }
        catch (Exception e) {
            collectException(logger.error(e.msg));
        }
    }

    ~this() nothrow {
        if (isValid) {
            // this is to be on the safe side
            nothrowTryRmdirRecursive(root);
        }
    }

    bool isValid() nothrow {
        return root.length != 0;
    }

    StopWatch cleanup() {
        StopWatch sw;
        if (isValid) {
            sw.start;
            nothrowTryRmdirRecursive(root);
            sw.stop;
        }

        return sw;
    }
}

string hostname() nothrow {
    import core.sys.posix.unistd : gethostname;

    char[1024] raw_buf;

    if (gethostname(raw_buf.ptr, raw_buf.length) == 0) {
        import std.string : fromStringz;

        char[] hname = raw_buf.ptr.fromStringz;
        return hname.idup;
    } else {
        import std.random : uniform;
        import std.format : format;

        try {
            return format("gethostname_error_%s", uniform(0, 10_000_000));
        }
        catch (Exception e) {
        }

        return "gethostname_error";
    }
}

private:

shared Plugins registered_plugins;
