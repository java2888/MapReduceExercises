package groupExample.userConsumptionGroupExample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class ConsumptionData implements WritableComparable<ConsumptionData> {
	private String name;
	private Integer cost;

	public ConsumptionData() {
		super();
	}

	public ConsumptionData(String name, Integer cost) {
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

	public Integer getCost() {
		return cost;
	}

	public void setCost(Integer cost) {
		this.cost = cost;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		name = in.readUTF();
		cost = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(name);
		out.writeInt(cost);
	}

	@Override
	public int compareTo(ConsumptionData o) {
		if(this.name.equals(o.getName())){
			int result = this.cost - o.getCost();
			return -result;			
		}else{
			return this.name.compareTo(o.getName());
		}

	}

	@Override
	public String toString() {
		return name + "," + cost;
	}

}
