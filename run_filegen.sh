# args: <output_hdfs_path> <bytes_per_file_to_generate> <number_of_files_to_generate_per_mapper> <number_of_mappers> <immediate_close|delayed_close> <mapper_mb>

hadoop jar ./dist/loadgen-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.dell.korea.psd.bigdata.mr.filegen.FileGen hdfs://<HDFS_HOSTNAME_OR_IP>/tmp/<PATH> 1048576 100 5 immediate_close 2048