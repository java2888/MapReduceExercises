package groupExample.userConsumptionGroupExample;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator {

	public MyGroupComparator() {
		super(ConsumptionData.class, true);

	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		ConsumptionData p1 = (ConsumptionData) a;
		ConsumptionData p2 = (ConsumptionData) b;
		
		return p1.getName().compareTo(p2.getName());

	}

}
