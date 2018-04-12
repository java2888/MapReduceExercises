package sortExample;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserAllPayLoad implements WritableComparable<UserAllPayLoad> {
	private String mobileNumber;
	private int upPackNum;
	private int downPackNum;
	private int upPayLoad;
	private int downPayLoad;
	private int allPayLoad;

	public UserAllPayLoad() {
		super();
	}

	public UserAllPayLoad(String mobileNumber, int upPackNum, int downPackNum, int upPayLoad, int downPayLoad) {
		super();
		this.mobileNumber = mobileNumber;
		this.upPackNum = upPackNum;
		this.downPackNum = downPackNum;
		this.upPayLoad = upPayLoad;
		this.downPayLoad = downPayLoad;
		this.allPayLoad = upPayLoad + downPayLoad;
	}

	public UserAllPayLoad(String mobileNumber, int upPackNum, int downPackNum, int upPayLoad, int downPayLoad,
			int allPayLoad) {
		super();
		this.mobileNumber = mobileNumber;
		this.upPackNum = upPackNum;
		this.downPackNum = downPackNum;
		this.upPayLoad = upPayLoad;
		this.downPayLoad = downPayLoad;
		this.allPayLoad = allPayLoad;
	}

	public String getMobileNumber() {
		return mobileNumber;
	}

	public void setMobileNumber(String mobileNumber) {
		this.mobileNumber = mobileNumber;
	}

	public int getUpPackNum() {
		return upPackNum;
	}

	public void setUpPackNum(int upPackNum) {
		this.upPackNum = upPackNum;
	}

	public int getDownPackNum() {
		return downPackNum;
	}

	public void setDownPackNum(int downPackNum) {
		this.downPackNum = downPackNum;
	}

	public int getUpPayLoad() {
		return upPayLoad;
	}

	public void setUpPayLoad(int upPayLoad) {
		this.upPayLoad = upPayLoad;
	}

	public int getDownPayLoad() {
		return downPayLoad;
	}

	public void setDownPayLoad(int downPayLoad) {
		this.downPayLoad = downPayLoad;
	}

	public int getAllPayLoad() {
		return allPayLoad;
	}

	public void setAllPayLoad(int allPayLoad) {
		this.allPayLoad = allPayLoad;
	}

	@Override
	public String toString() {
		return mobileNumber + ", " + upPackNum + ", " + downPackNum + ", " + upPayLoad + ", " + downPayLoad + ", "
				+ allPayLoad;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		mobileNumber = in.readUTF();
		upPackNum = in.readInt();
		downPackNum = in.readInt();
		upPayLoad = in.readInt();
		downPayLoad = in.readInt();
		allPayLoad = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(mobileNumber);
		out.writeInt(upPackNum);
		out.writeInt(downPackNum);
		out.writeInt(upPayLoad);
		out.writeInt(downPayLoad);
		out.writeInt(allPayLoad);
	}

	@Override
	public int compareTo(UserAllPayLoad userAllPayLoad) {
		int result = this.allPayLoad - userAllPayLoad.getAllPayLoad();
		return -result;
	}

}
