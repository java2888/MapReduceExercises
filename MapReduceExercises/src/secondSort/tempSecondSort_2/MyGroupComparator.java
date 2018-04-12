package secondSort.tempSecondSort_2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator {

	public MyGroupComparator() {
		super(TempInfo.class, true);

	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TempInfo p1 = (TempInfo) a;
		TempInfo p2 = (TempInfo) b;
		return Integer.compare(p1.getYear(), p2.getYear());
	}
}
