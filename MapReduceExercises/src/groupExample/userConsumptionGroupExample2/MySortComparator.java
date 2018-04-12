package groupExample.userConsumptionGroupExample2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/*public class MySortComparator implements RawComparator<ConsumptionData>{

	

	public MySortComparator() {
		super();
 
	}

	@Override
	public int compare(ConsumptionData o1, ConsumptionData o2) {
		if(o1.getName().equals(o2.getClass())){
			return  (int) -( o1.getCost()-o2.getCost() );
		}else{
			return o1.getName().compareTo(o2.getName());
		}
		 
	}

	@Override
	public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3, int arg4, int arg5) {
		// TODO Auto-generated method stub
		return 0;
	}
	
}*/

public class MySortComparator extends WritableComparator {

	public MySortComparator() {
		super(ConsumptionData.class, true);

	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		ConsumptionData p1 = (ConsumptionData) a;
		ConsumptionData p2 = (ConsumptionData) b;
		if (p1.getName().equals(p2.getName())) {
			long result = p1.getCost() - p2.getCost();
			return  (int) -result;
		} else {
			return p1.getName().compareTo(p2.getName());
		}		 
	}

}
