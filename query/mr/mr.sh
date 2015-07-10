DIR_PREFIX=/user/ubuntu/

hadoop fs -rm -r $DIR_PREFIX/out

hadoop jar $HADOOP_PREFIX/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar \
    -files mapper.py,reducer.py \
    -input hdfs://localhost:9000/data/sample.csv \
    -output out \
    -mapper mapper.py \
    -reducer reducer.py

if [ -d ./out ]; then
    rm -rf out
fi     

hadoop fs -get $DIR_PREFIX/out .

