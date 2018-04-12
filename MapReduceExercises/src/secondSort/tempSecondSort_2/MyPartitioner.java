package secondSort.tempSecondSort_2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<TempInfo, NullWritable>{

	@Override
	public int getPartition(TempInfo key, NullWritable value, int numReduceTasks) {
		int result=key.getYear() % numReduceTasks;
		return result;
	}

}
