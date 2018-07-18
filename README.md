# MapReduceTutorial
Classic map-reduce examples 

## Word Count

## Word Count Histogram
### Data flow
Calling 2 mappers & reducers. Data flow is as follows:

> (word, 1) -> (word, count) -> (count, word) -> (count, # that this count occurs)

### Reference
I get suggestion from [chains of mappers @ StackOverflow](https://stackoverflow.com/questions/29741305/how-can-i-have-multiple-mappers-and-reducers).

### Current Issue
Currently the 2nd mapper doesn't work. One possible reason might be [LongWritable cannot be cast to Text](https://stackoverflow.com/questions/11784729/hadoop-java-lang-classcastexception-org-apache-hadoop-io-longwritable-cannot). Basically, it's that the 2nd mapper reads input from some intermediate files on disk (output for the first reducer), and 

> When you read a file with a M/R program, the input key of your mapper should be the index of the line in the file, while the input value will be the full line.

I asked the question [Why there is no output for hadoop word count histogram program
](https://stackoverflow.com/questions/51373965/why-there-is-no-output-for-hadoop-word-count-histogram-program) on StackOverflow.

## ChainMR
To implement paradiams like ``maps -> reducer -> other maps``, Hadoop provides with an interface called ``ChainMapper`` and ``ChainReducer``. Workflow is as follows:

* ``ChainMapper`` adds maps
* only ``1 reducer`` is allowed, added by ``ChainReducer``
* we could also add a few mappers after that reducer via ``ChainReducer``

Using multiple mappers together could **avoid lots of dist IO**.

This *ChainMR.java* only adds mappers before the reducer.

*I got reference from [Hadoop ChainMapper](https://stackoverflow.com/questions/12926474/hadoop-chainmapper-chainreducer).*

## ChainMR2
How could we add multiple mappers and reducers? As far as I searched on the Internet, there is no such utility achieving this. The only solution is to store the intermediate files on local disks and read by the next job. I.e.

> ChainMR (map1 ... mapK reducer1 mapK+1 ... ) -> local disk -> ChainMR'

## Possible Exceptions
### IOException wrong value class: IntWritable is not Text
Mappers' outputs are supposed to be the same of reducer by default, if you only set ``setOutputKeyClass`` and ``setOutputValueClass``. In that scenario, we should also set ``setMapOutputKeyClass`` and ``setMapOutputValueClass``.

### Connection refused, cannot start namenode
It's possibly because of the inconsistency of namenode and datanode IDs when you format HDFS for multiple times. Those IDs on datanode might be out-of-date compared to those on namenode. While it might be a solution to copy those IDs to datanodes (``/tmp/hadoop-username/dfs/name/current/VERSION``), another UGLY solution is [DELETE ALL](https://stackoverflow.com/questions/15630460/connection-refused-error-for-namenode-hdfs-hadoop-issue):

> ./bin/stop-all.sh
>
> rm -rf /tmp/hadoop*
>
> ./bin/hadoop namenode -format
