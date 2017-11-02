/**
Copyright: Copyright (c) 2017, Joakim Brännström. All rights reserved.
License: $(LINK2 http://www.boost.org/LICENSE_1_0.txt, Boost Software License 1.0)
Author: Joakim Brännström (joakim.brannstrom@gmx.com)

This module contains a plugin used to test the database.
*/
module metric_plugin.db_testing;

import metric_factory.plugin;
import metric_factory.types;

shared static this() {
    buildPlugin.description("Insert metric values of all metric types")
        .group("builtin_db").func(&testDatabase).register;
}

void testDatabase(MetricValueStore coll) nothrow {
    import core.time : MonoTime;

    try {
        coll.put(Timer(BucketName("metric_factory_test_db"),
                Timer.Value(MonoTime.currTime - MonoTime.zero)));
        coll.put(Gauge(BucketName("metric_factory_test_db"), Gauge.Value(42)));
        coll.put(Counter(BucketName("metric_factory_test_db"), Counter.Change(42)));
        coll.put(Set(BucketName("metric_factory_test_db"), Set.Value(42)));
    }
    catch (Exception e) {
    }
}
