package maxTempEveryYear.MaxTempEveryYear_1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SortTemp extends WritableComparator {

	public SortTemp( ) {
		super(KeyPair.class, true);
		System.out.println("Begin to execute SortTemp()...");
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		KeyPair kp1=(KeyPair) a;
		KeyPair kp2=(KeyPair) b;
		int result=Integer.compare(kp1.getYear(), kp2.getYear());
		if(result==0){
			result= -Integer.compare(kp1.getTemp(), kp2.getTemp());
		}
		return result;
	}
 
	
}
