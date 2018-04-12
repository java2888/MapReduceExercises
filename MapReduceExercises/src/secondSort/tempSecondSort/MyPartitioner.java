package secondSort.tempSecondSort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<SelfDefinedTempInfo, NullWritable> {

	@Override
	public int getPartition(SelfDefinedTempInfo key, NullWritable value, int numReduceTasks) {
		int result=key.getYear() % numReduceTasks;
		return result;
	}

}
