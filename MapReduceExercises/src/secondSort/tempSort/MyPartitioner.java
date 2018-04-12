package secondSort.tempSort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class MyPartitioner extends org.apache.hadoop.mapreduce.Partitioner<TempData, NullWritable>{

	@Override
	public int getPartition(TempData key, NullWritable value, int NumReduceTasks) {		 
		return key.getYear() % NumReduceTasks ;
	}
	
}