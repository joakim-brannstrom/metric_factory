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
            "../build/metric_factory -d --run plugin_group --plugin-group builtin_db --output-to-db");

    immutable cmd = "sqlite3 metric_factory.sqlite3";
    auto res = runCollect(cmd ~ " .dump");
    writeln(res);

    res = runCollect(cmd ~ ` "SELECT count(*) FROM metric"`);
    assert(res.strip == "1");

    // 3 because of the generic name and _min/_max suffixed
    res = runCollect(cmd ~ ` "SELECT count(*) FROM bucket"`);
    assert(res.strip == "3");

    res = runCollect(cmd ~ ` "SELECT count(*) FROM test_host"`);
    assert(res.strip == "0");

    res = runCollect(cmd ~ ` "SELECT count(*) FROM counter_t"`);
    assert(res.strip == "1");

    // 3 because of the basic gauge and _min/_max variant
    res = runCollect(cmd ~ ` "SELECT count(*) FROM gauge_t"`);
    assert(res.strip == "5");

    res = runCollect(cmd ~ ` "SELECT count(*) FROM timer_t"`);
    assert(res.strip == "1");
    res = runCollect(cmd ~ ` "SELECT count(*) FROM set_t"`);
    assert(res.strip == "1");
}

// This test is dependent that the previous one has executed first
@("shall convert a sqlite3 db to a CSV file")
unittest {
    import scriptlike;

    auto res = runCollect("../build/metric_factory -d --run db_to_csv");
    writeln(res);
}
