# MapReduceTutorial
Classic map-reduce examples 

## Word Count

## Word Count Histogram
### Data flow
Calling 2 mappers & reducers. Data flow is as follows:

(word, 1) -> (word, count) -> (count, word) -> (count, # that this count occurs)

### Reference
I get suggestion from [chains of mappers @ StackOverflow](https://stackoverflow.com/questions/29741305/how-can-i-have-multiple-mappers-and-reducers).

### Current Issue
Currently the 2nd mapper doesn't work. One possible reason might be [LongWritable cannot be cast to Text](https://stackoverflow.com/questions/11784729/hadoop-java-lang-classcastexception-org-apache-hadoop-io-longwritable-cannot). Basically, it's that the 2nd mapper reads input from some intermediate files on disk (output for the first reducer), and *When you read a file with a M/R program, the input key of your mapper should be the index of the line in the file, while the input value will be the full line.*
