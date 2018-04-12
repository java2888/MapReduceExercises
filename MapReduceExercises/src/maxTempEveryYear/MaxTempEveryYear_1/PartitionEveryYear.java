package maxTempEveryYear.MaxTempEveryYear_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionEveryYear extends Partitioner<KeyPair	, Text> {

	@Override
	public int getPartition(KeyPair key, Text value, int numPartitions) {
		System.out.println("Begin to execute getPartition()...");
		return key.getYear()*127 % numPartitions;
	}
 
}
