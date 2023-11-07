# mysql-exporter

This is a hackathon project to build a daemon to do Debezium-style MySQL snapshotting and binary log streaming, except to a variety of configurable back ends (S3 Parquet, Redshift, etc.) instead of just Kafka.

For speed, it makes the assumption that we're okay with eventual consistency. Debezium in non-incremental mode is incredibly slow at snapshotting because it uses a single connection to process any given table, so the export speed is limited by the size of your largest table. If you're willing to accept certain inconsistencies in the data (double creates, updates and deletes on non-existent records, etc.), you can get a massive speedup by parallelizing the work across a large number of workers. We can still guarantee that the state will be consistent once the binary log reader catches up to the present; it's only during the initial snapshot that weird things might occur. (Something like Debezium's incremental snapshots would probably be the best solution, but that's something to think about waaaay in the future.)

Since it's a hackathon project, don't expect complete test coverage or ideal code.

Current state:
  - Snapshotting works
  - CSV export to local files works, for testing

To do:
  - Streaming changes from the binary log
  - Add some actually useful sinks
