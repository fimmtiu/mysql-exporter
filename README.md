# mysql-exporter

Assumptions that this relies on:

* We are okay with a small amount of inaccuracy in the historical data:
  * double `create` events for a single row
  * `update` events for rows that don't exist
  * `delete` events for rows that don't exist

We do, however, guarantee that the _current_ state of the data will be correct once the binlog is caught up. (Eventual consistency.)

* We don't care about the order of rows in Parquet files. (Parquet doesn't guarantee row order anyhow, so this is not a big deal.)


First draft:

[ ] ParquetWriter
[ ] FileUploader
  [ ] Local
  [ ] S3
[ ] Integration testing setup with Docker
  [ ] MySQL 5.7, set up to do replication
  [ ] Redis, as recent as you can get
  [ ] local-s3 or localstack
[ ] TempCleaner
[ ] Snapshotter
  [ ] integration tests!
[ ] BinlogReader
[ ] ManualRequestWatcher

Later:

- Better error handling for SchemaReader so it retries and/or reconnects if its queries fail
- Better error handling for ParquetWriter so it forces a /tmp cleanup if it runs out of space
- Better error handling across the board, really.



Process:

Resources:
 - Pool of MySQL connections
 - Pool of Redis connections

On startup:
  1. Get a list of all the tables and their schemas
  2. Spawn a Snapshot, run it, wait for it to exit
  3. Spawn a BinlogReader, run it, wait for it to exit

SnapshotTableState: An object, not a separate goroutine. Access controlled by a sync.Lock.
  - Gets a MySQL connection and a list of table names in the constructor
  - Has a queue of pending chunks (`[]PendingChunk`, initial max size is number of snapshot workers)
  - Has a doneUpTo `map[string]uint64`, contains the starting offsets for the next pending chunks
  - On startup:
    - Read the last_committed_gtid from StateStorage
    - See if the MySQL server has the GTID in its binlog
    - For each table:
      - if the server had the GTID
        - reads table state from StateStorage on startup: either ["done"] or a list of chunk starts that have been completed
          - no progress yet == empty list
      - else
        - clear the table state for that table and start over with an empty list
      - if the chunk status != ["done"]
        - insert a pending chunk for SNAPSHOT_CHUNK_SIZE rows for every gap in the table
        - set doneUpTo[table] = highest chunk start + SNAPSHOT_CHUNK_SIZE

  - CreatePendingChunk(table string, startAt uint64) - inserts a new pending chunk into the work queue
    - Acquires the lock
    - DEFER: Releases the lock
    - If startAt == doneUpTo[table]
      - Adds a new pending chunk to the queue (start doneUpTo[table], end doneUpTo[table] + SNAPSHOT_CHUNK_SIZE)
      - doneUpTo[table] += SNAPSHOT_CHUNK_SIZE
    - Else
      - This is already in the queue or someone else is already handling it, so we do nothing.

  - GetNextPendingChunk(table string) *PendingChunk
    - Acquires the lock
    - DEFER: Releases the lock
    - If the queue is empty
      - return nil
    - else
      - Pops a pending chunk off the queue and returns it

SnapshotWorker: gets SnapshotTableState
  - Try to pull a pending chunk from SnapshotTableState
    - If you can't, exit
  - Get the ParquetWriter for this pending chunk's table from the ParquetWriterPool
  - From MySQL, select the highest ID in the table and the highest ID in the chunk
  - Ask the ParquetWriter to notify us when the highest ID in the chunk is successfully written
  - If there exist IDs higher than our highest offset,
    - insert a pending chunk for the first gap whose start is >= this chunk's upper limit into the queue
  - Stream batches of records from the table
    - Pass the data and the table schema to ParquetWriter
  - Tell the ParquetWriter to flush stuff
  - Wait for "highest ID in chunk successfully written" notification
  - Insert a record in StateStorage recording that this chunk is done
  - If there didn't exist higher IDs, insert a record in StateStorage indicating that the table is done
  - Repeat

  - If we get an exit signal or can't pull a pending chunk:
    - Stop reading batches
    - Until the ParquetWriter's output channel reports success or closes:
      - Monitor the ParquetWriter's output channel
      - If it reports that the highest ID in this chunk was successfully written,
        - Insert a record in Redis recording that this chunk is done
        - If there didn't exist higher IDs, insert a record in Redis indicating that the table is done
        - break
    - Exit

Snapshot:
  - Create a ParquetWriterPool
  - Create a SnapshotTableState
  - For each table, if the chunk statuses don't exist or the server doesn't have the GTID
    or the chunk list isn't marked as complete:
        insert a pending chunk for SNAPSHOT_CHUNK_SIZE rows for the first gap in the table
  - Spawn a configurable number of SnapshotWorkers to process the queue. Each worker
  - Listen for events on the ParquetWriter status channel
    -
  - Listen for events on the exit channel
    - Signal all SnapshotWorkers to exit. Wait for them to all die.
    - Signal the ParquetWriterPool to exit. Continue to read their status updates and report results to Redis
    - Exit

ParquetWriterPool:
  - Has a [table -> PW] hash `map[string]ParquetWriter`
  - Constructor takes a function which returns the output channel
  - Get(tableName) - returns PW for given table
  - Kill(tableName) - kills the PW for this table, waits for it to die
  - Exit() - signals all PWs to exit and waits for them to die

BinlogReader: gets passed the GTID to start from
  - For each event in the binlog (in batches, ideally):
    - If it's for a table we want to track:
      - If it's a regular row update:
        - Get the data
        - Pass it to the appropriate ParquetWriter
          - Create a ParquetWriter if one doesn't exist for the given table
      - If it's a schema change
        - Kill the ParquetWriter for the given table
        - Update the global TableSchema with the schema change
        - Start a new ParquetWriter with the new schema
          (warning: not the current schema, but the one at that point in time in the binlog!)
  - If we get an exit signal
    - Stop reading the binlog
    - Tell all ParquetWriters to exit
      - Wait for them to all exit (use AsyncController.Wait())
    - Signal GtidTracker to exit
      - Wait for it to exit
    - Exit

GtidTracker: gets passed the status channel shared by all ParquetWriters + an input channel
  - Gets the last_committed_gtid from Redis
  - Listens to inputs, status updates, exit signals, and a 5-second timer
    - When it gets an input:
      - If we're not tracking this table:
        - Add a record for {lowest committed offset, highest pending offset} for that table
          - Lowest committed offset is last_committed_gtid
          - Highest pending offset is the input event's offset
      - Otherwise
        - Set table's highest pending offset to input event's offset
    - When it gets a status update:
      - If the table isn't being tracked, panic.
      - Set the table's lowest committed offset to the update's offset
    - When it gets an exit signal:
      - Empty the status update channel, processing all status updates
      - Update state
      - Exit
  - Every 5 seconds:
    - Update state

  How to update state:
    - Iterate over all tables and find the smallest lowest committed offset
      - Remove all tables from the list whose lowest committed offset == highest pending offset
      - Be careful. Check how Go maps work with simultaneous iteration + deletions!
    - If the smallest LCO > last_committed_gtid
      - send it to StateStorage
      - set last_committed_gtid = the new smallest offset

# Interface, has both file and S3 versions
ParquetWriter: gets passed the table name, schema
  - Create input (data) and output (status) channels
  - Create a Go struct for the given schema that has the appropriate Parquet tags
  - Loop:
    - Listen for batches of new data from the BinlogReader or SnapshotWorker
      - Add the data to the ParquetWriter's buffer
      - If the buffer is above a certain size, flush it to storage
    - Listen for a timer, every N seconds
      - Flush the contents of the buffer to storage
    - Listen for a death signal
      - Flush the contents of the buffer to storage
      - Exit

Flushing the contents of the buffer to storage entails:
  - ...
  - Does this batch contain an ID that we've been asked to listen for?
    - If so, send a notification
  - If error, retry a few times with a wait in between
    - Too many retries? Use a death signal to kill the entire process.

StateStorage: (interface, has file and Redis versions)
  - GetLastCommittedGtid() -> string, error
  - SetLastCommittedGtid(gtid string) -> error
  - ClearLastCommittedGtid() -> error
  - GetTableSnapshotState(tableName string) -> chunk list, done flag (bool), error
  - SetTableSnapshotState(tableName string, chunkList) -> error
  - MarkTableSnapshotDone(tableName string) -> error
  - ClearTableSnapshotState(tableName string) -> error
  - ClearAllState() -> error

HIGHEST ID APPROACH: (per table)

Assumption: Only one thread ever reads from a ParquetWriter's status channel at a time.
Instead, the status channel is a `make(chan uint64, 1)` which returns the highest ID that it's successfully written.
Each time it successfully writes a chunk, it does a non-blocking read from its own status channel to clear it, then writes the new value.

I hate this. Doing dynamic array of channels sucks and is an antipattern. Let's make one single ParquetWriter status
channel that reports `[table, highest_id, gtid (maybe empty)]`.

HIGHEST GTID APPROACH: (global)

Keep a linked list of [table, highest id, gtid]] for the things you send to ParquetWriter
Whenever a ParquetWriter reports success:
  If the success report is for the [table, highest id] of the HEAD of the list
    - The new value for last_written_gtid is either highest_successful_gtid (if HEAD+1.gtid > highest_successful_gtid)
      or HEAD.gtid.
    - write the new last_written_gtid to Redis
    - update the global last_written_gtid variable
  Otherwise:
    - traverse the list looking for that [table, highest id] entry and remove it.
    - if entry.gtid > highest_successful_gtid
      - set the highest_successful_gtid to entry.gtid

The files created by ParquetWriter must always be the complete contents of the entire buffer; you can't upload only part
of a buffer or else the ID matching won't work.


Metrics we'll want:

 - Number of SnapshotWorkers
 - Number of snapshot records being read from MySQL
 - Number of binlog records being read from MySQL
 - Time per Snapshot pending chunk
 - Time per binlog batch poll?
 - Number of active ParquetWorkers (non-zero buffer size)
 - Number of bytes sent to S3
 - Time spent blocking at every stage of the process.
  - We don't want to ever be blocking on channels internally; only on network I/O.
