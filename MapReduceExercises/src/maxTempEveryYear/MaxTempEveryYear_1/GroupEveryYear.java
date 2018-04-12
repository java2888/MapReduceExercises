package maxTempEveryYear.MaxTempEveryYear_1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupEveryYear extends WritableComparator {

	public GroupEveryYear() {		
		super(KeyPair.class, true);		
		System.out.println("Begin to execute GroupEveryYear()...");
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		System.out.println("Begin to execute compare() in GroupEveryYear()...");
		KeyPair kp1= (KeyPair)a;
		KeyPair kp2=(KeyPair)b;
		return Integer.compare(kp1.getYear(), kp2.getYear());
	}

}
