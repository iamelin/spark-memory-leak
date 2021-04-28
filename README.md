# Spark Memory Leak Application

This application shows a memory leak when persisting datasets is called many times during a long time.

When we persist dataset, the current SparkSession is cloned in CacheManager, then it is saved as a listener
in ListenerBus. But the cloned SparkSession is not removed from the listeners list after unpersisting dataset.
It leads to a memory leak after persisting a lot of datasets since we only add new SparkSessions to listener bus
but don't delete them.

If we set the parameter `spark.sql.sources.bucketing.autoBucketedScan.enabled` to `false`, the SparkSession isn't
cloned in CacheManager, and persisting works well.

The application simulates a service that loads data from database on-demand. The real application receives
commands from another service, loads some data, processes them, and returns the result of processing.
This service have to launched all the time, so we cannot start the service on-demand.
