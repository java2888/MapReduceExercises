package groupExample.userConsumptionGroupExample2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ConsumptionData implements WritableComparable<ConsumptionData> {

	private String name;
	private Long cost;
	
	public ConsumptionData() {
		super();
	}
	
	public ConsumptionData(String name, Long cost) {
		super();
		this.name = name;
		this.cost = cost;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Long getCost() {
		return cost;
	}

	public void setCost(Long cost) {
		this.cost = cost;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		 this.name=in.readUTF();
		 this.cost=in.readLong();		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		 out.writeUTF(name);
		 out.writeLong(cost);
	}

	@Override
	public int compareTo(ConsumptionData arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String toString() {
		return name + "," + cost;
	}
		
}
