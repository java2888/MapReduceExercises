package secondSort.tempSort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MySortComparator extends WritableComparator {
	
	public MySortComparator() {
		super(TempData.class,true);		 
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TempData p1=(TempData) a;
		TempData p2=(TempData) b;
		if(p1.getYear()==p2.getYear()){
			int result=Integer.compare(p1.getTemp(), p2.getTemp());
			return -result;
		}else{
			return Integer.compare(p1.getYear(), p2.getYear());
		}
		 
	}

	
}
