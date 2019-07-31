# args: <output_hdfs_path> <bytes_per_line_to_generate> <load_gen_period_in_minutes> <number_of_mappers>

hadoop jar ./dist/loadgen-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.dell.korea.psd.bigdata.mr.loadgen.LoadGen hdfs://<HDFS_HOSTNAME_OR_IP>/tmp/<PATH> 83886080 10 180