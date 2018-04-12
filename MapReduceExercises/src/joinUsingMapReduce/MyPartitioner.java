package joinUsingMapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, Text>{

	@Override
	public int getPartition(Text key, Text Text, int numReduceTasks) {
		int result=(key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
		return result;
	}

 
	
}
