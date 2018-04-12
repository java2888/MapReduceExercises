package secondSort.tempSort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator{

	
	public MyGroupComparator() {
		super(TempData.class,true);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TempData p1=(TempData) a;
		TempData p2=(TempData) b;
		return Integer.compare(p1.getYear(), p2.getYear());
	}
	
}
