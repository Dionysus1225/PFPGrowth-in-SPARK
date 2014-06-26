package PFPGrowth_in_SPARK;

import java.util.ArrayList;
import java.util.List;

public class NumericLine {
	List<Integer> numericRecord;
	
	public NumericLine(String line){
		numericRecord = new ArrayList<Integer>();
		if (line == null)
			numericRecord= null;
		line = line.trim();
		String[] items = line.split(" ");
		for (String strItem : items) {
			numericRecord.add(Integer.parseInt(strItem));
		}	
	}
	
	public List<Integer> getValues(){
		return numericRecord;
	}
}
