package secondSort.tempSecondSort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator {

	public MyGroupComparator() {
		super(SelfDefinedTempInfo.class,true);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		SelfDefinedTempInfo p1=(SelfDefinedTempInfo) a;
		SelfDefinedTempInfo p2=(SelfDefinedTempInfo) b;
		return p1.getYear()-p2.getYear();
	}

		
	

}
