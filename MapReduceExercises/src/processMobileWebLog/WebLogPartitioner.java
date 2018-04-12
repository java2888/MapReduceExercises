package processMobileWebLog;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WebLogPartitioner extends Partitioner<Text, UserWebPayLoad> {

	@Override
	public int getPartition(Text key, UserWebPayLoad value, int numReduceTasks) {
		int result = 0;
		if (key.toString().length() < 11) {
			result = 0;
		} else if (11 == key.toString().length()) {
			result = 1;
		}

		return result % numReduceTasks;
	}

}
