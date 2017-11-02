/**
Copyright: Copyright (c) 2017, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)
*/
module metric_factory.types;

/// No guarantee regarding the path. May be absolute, relative, contain a '~'.
/// The user of this type must do all the safety checks to ensure that the
/// datacontained in valid.
struct Path {
    string payload;
    alias payload this;
}

/// ditto
struct DirPath {
    Path payload;
    alias payload this;

    this(string p) pure nothrow @nogc {
        payload = Path(p);
    }
}

/** The path is guaranteed to be the absolute path.
 *
 * The user of the type has to make an explicit judgment when using the
 * assignment operator. Either a `FileName` and then pay the cost of the path
 * expansion or an absolute which is already assured to be _ok_.
 * This divides the domain in two, one unchecked and one checked.
 */
struct AbsolutePath {
    import std.path : expandTilde, buildNormalizedPath, absolutePath,
        asNormalizedPath, asAbsolutePath;

    Path payload;
    alias payload this;

    invariant {
        import std.path : isAbsolute;

        assert(payload.length == 0 || payload.isAbsolute);
    }

    immutable this(AbsolutePath p) {
        this.payload = p.payload;
    }

    this(Path p) {
        auto p_expand = () @trusted{ return p.expandTilde; }();
        // the second buildNormalizedPath is needed to correctly resolve "."
        // otherwise it is resolved to /foo/bar/.
        payload = buildNormalizedPath(p_expand).absolutePath.buildNormalizedPath.Path;
    }

    /// Build the normalised path from workdir.
    this(Path p, DirPath workdir) {
        auto p_expand = () @trusted{ return p.expandTilde; }();
        auto workdir_expand = () @trusted{ return workdir.expandTilde; }();
        // the second buildNormalizedPath is needed to correctly resolve "."
        // otherwise it is resolved to /foo/bar/.
        payload = buildNormalizedPath(workdir_expand, p_expand).absolutePath
            .buildNormalizedPath.Path;
    }

    void opAssign(Path p) {
        payload = typeof(this)(p).payload;
    }

    pure nothrow @nogc void opAssign(AbsolutePath p) {
        payload = p.payload;
    }

    pure nothrow const @nogc Path opCast(T : Path)() {
        return FileName(payload);
    }

    pure nothrow const @nogc string opCast(T : string)() {
        return payload;
    }
}

struct Timestamp {
    import std.datetime : SysTime, Clock;

    SysTime payload;
    alias payload this;

    static auto make() {
        return Timestamp(Clock.currTime.toUTC);
    }
}
