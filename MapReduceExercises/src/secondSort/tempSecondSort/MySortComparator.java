package secondSort.tempSecondSort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySortComparator extends WritableComparator {

	public MySortComparator() {
		super(SelfDefinedTempInfo.class, true);

	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		SelfDefinedTempInfo p1 = (SelfDefinedTempInfo) a;
		SelfDefinedTempInfo p2 = (SelfDefinedTempInfo) b;
		if (p1.getYear() == p2.getYear()) {
			return -(p1.getTemp() - p2.getTemp());
		} else {
			return p1.getYear() - p2.getYear();
		}
	}

}
