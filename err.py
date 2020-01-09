

[hduser@Inceptez pyspark_excersice]$
[hduser@Inceptez pyspark_excersice]$ spark-submit excersize3_test5.py
/usr/local/spark/python/lib/pyspark.zip/pyspark/sql/context.py:487: DeprecationWarning: HiveContext is deprecated in Spark 2.0.0. Please use SparkSession.builder.enableHiveSupport().getOrCreate() instead.
19/12/18 20:06:43 INFO spark.SparkContext: Running Spark version 2.0.1
19/12/18 20:06:44 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
19/12/18 20:06:44 WARN util.Utils: Your hostname, Inceptez resolves to a loopback address: 127.0.0.1; using 192.168.80.131 instead (on interface eth17)
19/12/18 20:06:44 WARN util.Utils: Set SPARK_LOCAL_IP if you need to bind to another address
19/12/18 20:06:44 INFO spark.SecurityManager: Changing view acls to: hduser
19/12/18 20:06:44 INFO spark.SecurityManager: Changing modify acls to: hduser
19/12/18 20:06:44 INFO spark.SecurityManager: Changing view acls groups to:
19/12/18 20:06:44 INFO spark.SecurityManager: Changing modify acls groups to:
19/12/18 20:06:44 INFO spark.SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(hduser); groups with view permissions: Set(); users  with modify permissions: Set(hduser); groups with modify permissions: Set()
19/12/18 20:06:45 INFO util.Utils: Successfully started service 'sparkDriver' on port 37564.
19/12/18 20:06:45 INFO spark.SparkEnv: Registering MapOutputTracker
19/12/18 20:06:45 INFO spark.SparkEnv: Registering BlockManagerMaster
19/12/18 20:06:45 INFO storage.DiskBlockManager: Created local directory at /tmp/blockmgr-1a9dd89e-3559-4d23-b66c-91bc4a099b98
19/12/18 20:06:45 INFO memory.MemoryStore: MemoryStore started with capacity 366.3 MB
19/12/18 20:06:46 INFO spark.SparkEnv: Registering OutputCommitCoordinator
19/12/18 20:06:46 INFO util.log: Logging initialized @5240ms
19/12/18 20:06:46 INFO server.Server: jetty-9.2.z-SNAPSHOT
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@746a9708{/jobs,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@4169c316{/jobs/json,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@1e51e1e6{/jobs/job,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@78ed21b0{/jobs/job/json,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@589f3b72{/stages,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@57e13204{/stages/json,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@46aeb7cf{/stages/stage,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@57dffe0b{/stages/stage/json,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@763a0b29{/stages/pool,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@281566f8{/stages/pool/json,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@1b36bcfa{/storage,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@33313631{/storage/json,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@15f4103d{/storage/rdd,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@165586dd{/storage/rdd/json,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@382a3981{/environment,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@210100b{/environment/json,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@37509d8c{/executors,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@d8996d5{/executors/json,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@648650d4{/executors/threadDump,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@66948407{/executors/threadDump/json,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@559bd9a4{/static,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@1465d2fc{/,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@42333054{/api,null,AVAILABLE}
19/12/18 20:06:46 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@c520a9{/stages/stage/kill,null,AVAILABLE}
19/12/18 20:06:46 INFO server.ServerConnector: Started ServerConnector@48e4955d{HTTP/1.1}{0.0.0.0:4040}
19/12/18 20:06:46 INFO server.Server: Started @5718ms
19/12/18 20:06:46 INFO util.Utils: Successfully started service 'SparkUI' on port 4040.
19/12/18 20:06:46 INFO ui.SparkUI: Bound SparkUI to 0.0.0.0, and started at http://192.168.80.131:4040
19/12/18 20:06:47 INFO spark.SparkContext: Added file file:/home/hduser/pyspark_excersice/excersize3_test5.py at file:/home/hduser/pyspark_excersice/excersize3_test5.py with timestamp 1576679807609
19/12/18 20:06:47 INFO util.Utils: Copying /home/hduser/pyspark_excersice/excersize3_test5.py to /tmp/spark-e8b45814-5ff3-4bb4-b217-0ba2dcb24c40/userFiles-a6784c04-99d6-4640-a5c4-7d220fec0601/excersize3_test5.py
19/12/18 20:06:48 INFO executor.Executor: Starting executor ID driver on host localhost
19/12/18 20:06:48 INFO util.Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 39883.
19/12/18 20:06:48 INFO netty.NettyBlockTransferService: Server created on 192.168.80.131:39883
19/12/18 20:06:48 INFO storage.BlockManagerMaster: Registering BlockManager BlockManagerId(driver, 192.168.80.131, 39883)
19/12/18 20:06:48 INFO storage.BlockManagerMasterEndpoint: Registering block manager 192.168.80.131:39883 with 366.3 MB RAM, BlockManagerId(driver, 192.168.80.131, 39883)
19/12/18 20:06:48 INFO storage.BlockManagerMaster: Registered BlockManager BlockManagerId(driver, 192.168.80.131, 39883)
19/12/18 20:06:48 INFO handler.ContextHandler: Started o.s.j.s.ServletContextHandler@598f408{/metrics/json,null,AVAILABLE}



 >> creating RDD from another RDD Transformation 2 - Higher Order Function <<



19/12/18 20:06:50 INFO memory.MemoryStore: Block broadcast_0 stored as values in memory (estimated size 240.2 KB, free 366.1 MB)
19/12/18 20:06:50 INFO memory.MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 23.3 KB, free 366.0 MB)
19/12/18 20:06:50 INFO storage.BlockManagerInfo: Added broadcast_0_piece0 in memory on 192.168.80.131:39883 (size: 23.3 KB, free: 366.3 MB)
19/12/18 20:06:50 INFO spark.SparkContext: Created broadcast 0 from textFile at NativeMethodAccessorImpl.java:-2
19/12/18 20:06:52 INFO mapred.FileInputFormat: Total input paths to process : 1
19/12/18 20:06:52 INFO spark.SparkContext: Starting job: foreach at /home/hduser/pyspark_excersice/excersize3_test5.py:20
19/12/18 20:06:52 INFO scheduler.DAGScheduler: Got job 0 (foreach at /home/hduser/pyspark_excersice/excersize3_test5.py:20) with 2 output partitions
19/12/18 20:06:52 INFO scheduler.DAGScheduler: Final stage: ResultStage 0 (foreach at /home/hduser/pyspark_excersice/excersize3_test5.py:20)
19/12/18 20:06:52 INFO scheduler.DAGScheduler: Parents of final stage: List()
19/12/18 20:06:52 INFO scheduler.DAGScheduler: Missing parents: List()
19/12/18 20:06:52 INFO scheduler.DAGScheduler: Submitting ResultStage 0 (PythonRDD[2] at foreach at /home/hduser/pyspark_excersice/excersize3_test5.py:20), which has no missing parents
19/12/18 20:06:52 INFO memory.MemoryStore: Block broadcast_1 stored as values in memory (estimated size 7.0 KB, free 366.0 MB)
19/12/18 20:06:52 INFO memory.MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 4.3 KB, free 366.0 MB)
19/12/18 20:06:52 INFO storage.BlockManagerInfo: Added broadcast_1_piece0 in memory on 192.168.80.131:39883 (size: 4.3 KB, free: 366.3 MB)
19/12/18 20:06:52 INFO spark.SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1012
19/12/18 20:06:52 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ResultStage 0 (PythonRDD[2] at foreach at /home/hduser/pyspark_excersice/excersize3_test5.py:20)
19/12/18 20:06:52 INFO scheduler.TaskSchedulerImpl: Adding task set 0.0 with 2 tasks
19/12/18 20:06:52 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, partition 0, PROCESS_LOCAL, 5569 bytes)
19/12/18 20:06:52 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, localhost, partition 1, PROCESS_LOCAL, 5569 bytes)
19/12/18 20:06:52 INFO executor.Executor: Running task 0.0 in stage 0.0 (TID 0)
19/12/18 20:06:52 INFO executor.Executor: Running task 1.0 in stage 0.0 (TID 1)
19/12/18 20:06:52 INFO executor.Executor: Fetching file:/home/hduser/pyspark_excersice/excersize3_test5.py with timestamp 1576679807609
19/12/18 20:06:52 INFO util.Utils: /home/hduser/pyspark_excersice/excersize3_test5.py has been previously copied to /tmp/spark-e8b45814-5ff3-4bb4-b217-0ba2dcb24c40/userFiles-a6784c04-99d6-4640-a5c4-7d220fec0601/excersize3_test5.py
19/12/18 20:06:53 INFO rdd.HadoopRDD: Input split: file:/home/hduser/sparkdata/teamdata.txt:117+118
19/12/18 20:06:53 INFO rdd.HadoopRDD: Input split: file:/home/hduser/sparkdata/teamdata.txt:0+117
19/12/18 20:06:53 INFO Configuration.deprecation: mapred.tip.id is deprecated. Instead, use mapreduce.task.id
19/12/18 20:06:53 INFO Configuration.deprecation: mapred.task.id is deprecated. Instead, use mapreduce.task.attempt.id
19/12/18 20:06:53 INFO Configuration.deprecation: mapred.task.is.map is deprecated. Instead, use mapreduce.task.ismap
19/12/18 20:06:53 INFO Configuration.deprecation: mapred.job.id is deprecated. Instead, use mapreduce.job.id
19/12/18 20:06:53 INFO Configuration.deprecation: mapred.task.partition is deprecated. Instead, use mapreduce.task.partition
/usr/local/spark/python/lib/pyspark.zip/pyspark/sql/context.py:487: DeprecationWarning: HiveContext is deprecated in Spark 2.0.0. Please use SparkSession.builder.enableHiveSupport().getOrCreate() instead.
5
5
5
5
5
5
19/12/18 20:06:53 INFO python.PythonRunner: Times: total = 507, boot = 448, init = 56, finish = 3
19/12/18 20:06:53 INFO python.PythonRunner: Times: total = 506, boot = 459, init = 45, finish = 2
19/12/18 20:06:53 INFO executor.Executor: Finished task 0.0 in stage 0.0 (TID 0). 1469 bytes result sent to driver
19/12/18 20:06:53 INFO executor.Executor: Finished task 1.0 in stage 0.0 (TID 1). 1461 bytes result sent to driver
19/12/18 20:06:53 INFO scheduler.TaskSetManager: Finished task 1.0 in stage 0.0 (TID 1) in 1147 ms on localhost (1/2)
19/12/18 20:06:53 INFO scheduler.TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 1280 ms on localhost (2/2)
19/12/18 20:06:53 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool
19/12/18 20:06:53 INFO scheduler.DAGScheduler: ResultStage 0 (foreach at /home/hduser/pyspark_excersice/excersize3_test5.py:20) finished in 1.425 s
19/12/18 20:06:53 INFO scheduler.DAGScheduler: Job 0 finished: foreach at /home/hduser/pyspark_excersice/excersize3_test5.py:20, took 1.727652 s



 >> creating RDD from another RDD Transformation 3 - filter() Higher Order Function <<



19/12/18 20:06:54 INFO memory.MemoryStore: Block broadcast_2 stored as values in memory (estimated size 240.2 KB, free 365.8 MB)
19/12/18 20:06:54 INFO memory.MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 23.3 KB, free 365.8 MB)
19/12/18 20:06:54 INFO storage.BlockManagerInfo: Added broadcast_2_piece0 in memory on 192.168.80.131:39883 (size: 23.3 KB, free: 366.3 MB)
19/12/18 20:06:54 INFO spark.SparkContext: Created broadcast 2 from textFile at NativeMethodAccessorImpl.java:-2
19/12/18 20:06:54 INFO mapred.FileInputFormat: Total input paths to process : 1
19/12/18 20:06:54 INFO spark.SparkContext: Starting job: collect at /home/hduser/pyspark_excersice/excersize3_test5.py:29
19/12/18 20:06:54 INFO scheduler.DAGScheduler: Got job 1 (collect at /home/hduser/pyspark_excersice/excersize3_test5.py:29) with 2 output partitions
19/12/18 20:06:54 INFO scheduler.DAGScheduler: Final stage: ResultStage 1 (collect at /home/hduser/pyspark_excersice/excersize3_test5.py:29)
19/12/18 20:06:54 INFO scheduler.DAGScheduler: Parents of final stage: List()
19/12/18 20:06:54 INFO scheduler.DAGScheduler: Missing parents: List()
19/12/18 20:06:54 INFO scheduler.DAGScheduler: Submitting ResultStage 1 (PythonRDD[5] at collect at /home/hduser/pyspark_excersice/excersize3_test5.py:29), which has no missing parents
19/12/18 20:06:54 INFO memory.MemoryStore: Block broadcast_3 stored as values in memory (estimated size 6.1 KB, free 365.8 MB)
19/12/18 20:06:54 INFO memory.MemoryStore: Block broadcast_3_piece0 stored as bytes in memory (estimated size 3.7 KB, free 365.8 MB)
19/12/18 20:06:54 INFO storage.BlockManagerInfo: Added broadcast_3_piece0 in memory on 192.168.80.131:39883 (size: 3.7 KB, free: 366.2 MB)
19/12/18 20:06:54 INFO spark.SparkContext: Created broadcast 3 from broadcast at DAGScheduler.scala:1012
19/12/18 20:06:54 INFO scheduler.DAGScheduler: Submitting 2 missing tasks from ResultStage 1 (PythonRDD[5] at collect at /home/hduser/pyspark_excersice/excersize3_test5.py:29)
19/12/18 20:06:54 INFO scheduler.TaskSchedulerImpl: Adding task set 1.0 with 2 tasks
19/12/18 20:06:54 INFO scheduler.TaskSetManager: Starting task 0.0 in stage 1.0 (TID 2, localhost, partition 0, PROCESS_LOCAL, 5569 bytes)
19/12/18 20:06:54 INFO scheduler.TaskSetManager: Starting task 1.0 in stage 1.0 (TID 3, localhost, partition 1, PROCESS_LOCAL, 5569 bytes)
19/12/18 20:06:54 INFO executor.Executor: Running task 0.0 in stage 1.0 (TID 2)
19/12/18 20:06:54 INFO executor.Executor: Running task 1.0 in stage 1.0 (TID 3)
19/12/18 20:06:54 INFO rdd.HadoopRDD: Input split: file:/home/hduser/sparkdata/teamdata.txt:0+117
19/12/18 20:06:54 INFO rdd.HadoopRDD: Input split: file:/home/hduser/sparkdata/teamdata.txt:117+118
19/12/18 20:06:54 ERROR executor.Executor: Exception in task 0.0 in stage 1.0 (TID 2)
org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 172, in main
    process()
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 167, in process
    serializer.dump_stream(func(split_index, iterator), outfile)
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/serializers.py", line 263, in dump_stream
    vs = list(itertools.islice(iterator, batch))
  File "/home/hduser/pyspark_excersice/excersize3_test5.py", line 28, in <lambda>
    chennaiLines = lines.map(lambda x: x.split(",")).filter(lambda l: l[l].upper() == "CHENNAI")
TypeError: list indices must be integers, not list

        at org.apache.spark.api.python.PythonRunner$$anon$1.read(PythonRDD.scala:193)
        at org.apache.spark.api.python.PythonRunner$$anon$1.<init>(PythonRDD.scala:234)
        at org.apache.spark.api.python.PythonRunner.compute(PythonRDD.scala:152)
        at org.apache.spark.api.python.PythonRDD.compute(PythonRDD.scala:63)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:319)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:283)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:70)
        at org.apache.spark.scheduler.Task.run(Task.scala:86)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:274)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
        at java.lang.Thread.run(Thread.java:745)
19/12/18 20:06:54 ERROR executor.Executor: Exception in task 1.0 in stage 1.0 (TID 3)
org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 172, in main
    process()
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 167, in process
    serializer.dump_stream(func(split_index, iterator), outfile)
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/serializers.py", line 263, in dump_stream
    vs = list(itertools.islice(iterator, batch))
  File "/home/hduser/pyspark_excersice/excersize3_test5.py", line 28, in <lambda>
    chennaiLines = lines.map(lambda x: x.split(",")).filter(lambda l: l[l].upper() == "CHENNAI")
TypeError: list indices must be integers, not list

        at org.apache.spark.api.python.PythonRunner$$anon$1.read(PythonRDD.scala:193)
        at org.apache.spark.api.python.PythonRunner$$anon$1.<init>(PythonRDD.scala:234)
        at org.apache.spark.api.python.PythonRunner.compute(PythonRDD.scala:152)
        at org.apache.spark.api.python.PythonRDD.compute(PythonRDD.scala:63)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:319)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:283)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:70)
        at org.apache.spark.scheduler.Task.run(Task.scala:86)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:274)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
        at java.lang.Thread.run(Thread.java:745)
19/12/18 20:06:54 INFO storage.BlockManagerInfo: Removed broadcast_1_piece0 on 192.168.80.131:39883 in memory (size: 4.3 KB, free: 366.3 MB)
19/12/18 20:06:54 WARN scheduler.TaskSetManager: Lost task 0.0 in stage 1.0 (TID 2, localhost): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 172, in main
    process()
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 167, in process
    serializer.dump_stream(func(split_index, iterator), outfile)
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/serializers.py", line 263, in dump_stream
    vs = list(itertools.islice(iterator, batch))
  File "/home/hduser/pyspark_excersice/excersize3_test5.py", line 28, in <lambda>
    chennaiLines = lines.map(lambda x: x.split(",")).filter(lambda l: l[l].upper() == "CHENNAI")
TypeError: list indices must be integers, not list

        at org.apache.spark.api.python.PythonRunner$$anon$1.read(PythonRDD.scala:193)
        at org.apache.spark.api.python.PythonRunner$$anon$1.<init>(PythonRDD.scala:234)
        at org.apache.spark.api.python.PythonRunner.compute(PythonRDD.scala:152)
        at org.apache.spark.api.python.PythonRDD.compute(PythonRDD.scala:63)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:319)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:283)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:70)
        at org.apache.spark.scheduler.Task.run(Task.scala:86)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:274)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
        at java.lang.Thread.run(Thread.java:745)

19/12/18 20:06:54 ERROR scheduler.TaskSetManager: Task 0 in stage 1.0 failed 1 times; aborting job
19/12/18 20:06:54 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool
19/12/18 20:06:54 INFO scheduler.TaskSetManager: Lost task 1.0 in stage 1.0 (TID 3) on executor localhost: org.apache.spark.api.python.PythonException (Traceback (most recent call last):
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 172, in main
    process()
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 167, in process
    serializer.dump_stream(func(split_index, iterator), outfile)
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/serializers.py", line 263, in dump_stream
    vs = list(itertools.islice(iterator, batch))
  File "/home/hduser/pyspark_excersice/excersize3_test5.py", line 28, in <lambda>
    chennaiLines = lines.map(lambda x: x.split(",")).filter(lambda l: l[l].upper() == "CHENNAI")
TypeError: list indices must be integers, not list
) [duplicate 1]
19/12/18 20:06:54 INFO scheduler.TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool
19/12/18 20:06:54 INFO scheduler.TaskSchedulerImpl: Cancelling stage 1
19/12/18 20:06:54 INFO scheduler.DAGScheduler: ResultStage 1 (collect at /home/hduser/pyspark_excersice/excersize3_test5.py:29) failed in 0.432 s
19/12/18 20:06:54 INFO scheduler.DAGScheduler: Job 1 failed: collect at /home/hduser/pyspark_excersice/excersize3_test5.py:29, took 0.503494 s
Traceback (most recent call last):
  File "/home/hduser/pyspark_excersice/excersize3_test5.py", line 49, in <module>
    Create_RDD_from_RDD_Trans_Filter()
  File "/home/hduser/pyspark_excersice/excersize3_test5.py", line 29, in Create_RDD_from_RDD_Trans_Filter
    chennaiLines.collect()
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/rdd.py", line 776, in collect
  File "/usr/local/spark/python/lib/py4j-0.10.3-src.zip/py4j/java_gateway.py", line 1133, in __call__
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/sql/utils.py", line 63, in deco
  File "/usr/local/spark/python/lib/py4j-0.10.3-src.zip/py4j/protocol.py", line 319, in get_return_value
py4j.protocol.Py4JJavaError: An error occurred while calling z:org.apache.spark.api.python.PythonRDD.collectAndServe.
: org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 1.0 failed 1 times, most recent failure: Lost task 0.0 in stage 1.0 (TID 2, localhost): org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 172, in main
    process()
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 167, in process
    serializer.dump_stream(func(split_index, iterator), outfile)
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/serializers.py", line 263, in dump_stream
    vs = list(itertools.islice(iterator, batch))
  File "/home/hduser/pyspark_excersice/excersize3_test5.py", line 28, in <lambda>
    chennaiLines = lines.map(lambda x: x.split(",")).filter(lambda l: l[l].upper() == "CHENNAI")
TypeError: list indices must be integers, not list

        at org.apache.spark.api.python.PythonRunner$$anon$1.read(PythonRDD.scala:193)
        at org.apache.spark.api.python.PythonRunner$$anon$1.<init>(PythonRDD.scala:234)
        at org.apache.spark.api.python.PythonRunner.compute(PythonRDD.scala:152)
        at org.apache.spark.api.python.PythonRDD.compute(PythonRDD.scala:63)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:319)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:283)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:70)
        at org.apache.spark.scheduler.Task.run(Task.scala:86)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:274)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
        at java.lang.Thread.run(Thread.java:745)

Driver stacktrace:
        at org.apache.spark.scheduler.DAGScheduler.org$apache$spark$scheduler$DAGScheduler$$failJobAndIndependentStages(DAGScheduler.scala:1454)
        at org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1.apply(DAGScheduler.scala:1442)
        at org.apache.spark.scheduler.DAGScheduler$$anonfun$abortStage$1.apply(DAGScheduler.scala:1441)
        at scala.collection.mutable.ResizableArray$class.foreach(ResizableArray.scala:59)
        at scala.collection.mutable.ArrayBuffer.foreach(ArrayBuffer.scala:48)
        at org.apache.spark.scheduler.DAGScheduler.abortStage(DAGScheduler.scala:1441)
        at org.apache.spark.scheduler.DAGScheduler$$anonfun$handleTaskSetFailed$1.apply(DAGScheduler.scala:811)
        at org.apache.spark.scheduler.DAGScheduler$$anonfun$handleTaskSetFailed$1.apply(DAGScheduler.scala:811)
        at scala.Option.foreach(Option.scala:257)
        at org.apache.spark.scheduler.DAGScheduler.handleTaskSetFailed(DAGScheduler.scala:811)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.doOnReceive(DAGScheduler.scala:1667)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:1622)
        at org.apache.spark.scheduler.DAGSchedulerEventProcessLoop.onReceive(DAGScheduler.scala:1611)
        at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:48)
        at org.apache.spark.scheduler.DAGScheduler.runJob(DAGScheduler.scala:632)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:1890)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:1903)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:1916)
        at org.apache.spark.SparkContext.runJob(SparkContext.scala:1930)
        at org.apache.spark.rdd.RDD$$anonfun$collect$1.apply(RDD.scala:912)
        at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)
        at org.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:112)
        at org.apache.spark.rdd.RDD.withScope(RDD.scala:358)
        at org.apache.spark.rdd.RDD.collect(RDD.scala:911)
        at org.apache.spark.api.python.PythonRDD$.collectAndServe(PythonRDD.scala:453)
        at org.apache.spark.api.python.PythonRDD.collectAndServe(PythonRDD.scala)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:497)
        at py4j.reflection.MethodInvoker.invoke(MethodInvoker.java:237)
        at py4j.reflection.ReflectionEngine.invoke(ReflectionEngine.java:357)
        at py4j.Gateway.invoke(Gateway.java:280)
        at py4j.commands.AbstractCommand.invokeMethod(AbstractCommand.java:132)
        at py4j.commands.CallCommand.execute(CallCommand.java:79)
        at py4j.GatewayConnection.run(GatewayConnection.java:214)
        at java.lang.Thread.run(Thread.java:745)
Caused by: org.apache.spark.api.python.PythonException: Traceback (most recent call last):
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 172, in main
    process()
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/worker.py", line 167, in process
    serializer.dump_stream(func(split_index, iterator), outfile)
  File "/usr/local/spark/python/lib/pyspark.zip/pyspark/serializers.py", line 263, in dump_stream
    vs = list(itertools.islice(iterator, batch))
  File "/home/hduser/pyspark_excersice/excersize3_test5.py", line 28, in <lambda>
    chennaiLines = lines.map(lambda x: x.split(",")).filter(lambda l: l[l].upper() == "CHENNAI")
TypeError: list indices must be integers, not list

        at org.apache.spark.api.python.PythonRunner$$anon$1.read(PythonRDD.scala:193)
        at org.apache.spark.api.python.PythonRunner$$anon$1.<init>(PythonRDD.scala:234)
        at org.apache.spark.api.python.PythonRunner.compute(PythonRDD.scala:152)
        at org.apache.spark.api.python.PythonRDD.compute(PythonRDD.scala:63)
        at org.apache.spark.rdd.RDD.computeOrReadCheckpoint(RDD.scala:319)
        at org.apache.spark.rdd.RDD.iterator(RDD.scala:283)
        at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:70)
        at org.apache.spark.scheduler.Task.run(Task.scala:86)
        at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:274)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
        ... 1 more

19/12/18 20:06:55 INFO spark.SparkContext: Invoking stop() from shutdown hook
19/12/18 20:06:55 INFO server.ServerConnector: Stopped ServerConnector@48e4955d{HTTP/1.1}{0.0.0.0:4040}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@c520a9{/stages/stage/kill,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@42333054{/api,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@1465d2fc{/,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@559bd9a4{/static,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@66948407{/executors/threadDump/json,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@648650d4{/executors/threadDump,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@d8996d5{/executors/json,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@37509d8c{/executors,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@210100b{/environment/json,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@382a3981{/environment,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@165586dd{/storage/rdd/json,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@15f4103d{/storage/rdd,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@33313631{/storage/json,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@1b36bcfa{/storage,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@281566f8{/stages/pool/json,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@763a0b29{/stages/pool,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@57dffe0b{/stages/stage/json,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@46aeb7cf{/stages/stage,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@57e13204{/stages/json,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@589f3b72{/stages,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@78ed21b0{/jobs/job/json,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@1e51e1e6{/jobs/job,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@4169c316{/jobs/json,null,UNAVAILABLE}
19/12/18 20:06:55 INFO handler.ContextHandler: Stopped o.s.j.s.ServletContextHandler@746a9708{/jobs,null,UNAVAILABLE}
19/12/18 20:06:55 INFO ui.SparkUI: Stopped Spark web UI at http://192.168.80.131:4040
19/12/18 20:06:55 INFO spark.MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
19/12/18 20:06:55 INFO memory.MemoryStore: MemoryStore cleared
19/12/18 20:06:55 INFO storage.BlockManager: BlockManager stopped
19/12/18 20:06:55 INFO storage.BlockManagerMaster: BlockManagerMaster stopped
19/12/18 20:06:55 INFO scheduler.OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
19/12/18 20:06:55 INFO spark.SparkContext: Successfully stopped SparkContext
19/12/18 20:06:55 INFO util.ShutdownHookManager: Shutdown hook called
19/12/18 20:06:55 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-e8b45814-5ff3-4bb4-b217-0ba2dcb24c40
19/12/18 20:06:55 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-e8b45814-5ff3-4bb4-b217-0ba2dcb24c40/pyspark-b98613a8-f136-464f-9a68-0cd897961483
[hduser@Inceptez pyspark_excersice]$
