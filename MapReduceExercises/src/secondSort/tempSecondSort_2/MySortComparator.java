package secondSort.tempSecondSort_2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySortComparator extends WritableComparator {

	public MySortComparator() {
		super(TempInfo.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TempInfo p1 = (TempInfo) a;
		TempInfo p2 = (TempInfo) b;
		if (p1.getYear() == p1.getYear()) {
			return -(Integer.compare(p1.getTemp(), p2.getTemp()));
		} else {
			return Integer.compare(p1.getYear(), p2.getYear());
		}

	}
}
