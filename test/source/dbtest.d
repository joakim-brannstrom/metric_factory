/**
Copyright: Copyright (c) 2017, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)
*/
module dbtest;

@(
        "shall write the result of running the test suite on the remote host and local host to a sqlite3 database")
unittest {
    import scriptlike;

    runCollect("cd .. && dub build");
    runCollect("rm -f metric_factory.sqlite3");
    runCollect("rm -f *.csv");
    runCollect(
            "../build/metric_factory -d --run master --plugin-group builtin_db --output-to-db --host localhost");

    immutable cmd = "sqlite3 metric_factory.sqlite3";
    auto res = runCollect(cmd ~ " .dump");
    writeln(res);

    res = runCollect(cmd ~ ` "SELECT count(*) FROM metric"`);
    // shall be the global aggregate and localhost
    assert(res.strip == "2");

    res = runCollect(cmd ~ ` "SELECT count(*) FROM bucket"`);
    // the bucket name is shared
    assert(res.strip == "1");

    res = runCollect(cmd ~ ` "SELECT count(*) FROM test_host"`);
    // the host name is shared
    assert(res.strip == "1");

    // all metric types have 2 entries, global aggregate and localhost.
    res = runCollect(cmd ~ ` "SELECT count(*) FROM counter_t"`);
    assert(res.strip == "2");
    res = runCollect(cmd ~ ` "SELECT count(*) FROM gauge_t"`);
    assert(res.strip == "2");
    res = runCollect(cmd ~ ` "SELECT count(*) FROM timer_t"`);
    assert(res.strip == "2");
    res = runCollect(cmd ~ ` "SELECT count(*) FROM set_t"`);
    assert(res.strip == "2");
}

// This test is dependent that the previous one has executed first
@("shall convert a sqlite3 db to a CSV file")
unittest {
    import scriptlike;

    auto res = runCollect("../build/metric_factory -d --run db_to_csv");
    writeln(res);
}
