package com.xg.hadoop.training.secondarysorting;



import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InputGenerator {

	public static void main(String[] args) throws IOException {
		String[] namesArray = { "Aavneet", "Bavneet", "Cavneet", "Davneet", "Eavneet",
				"Favneet", "Gavneet", "Havneet", "Iavneet", "Javneet", "Kavneet", "Lavneet",
				"Mavneet", "Navneet", "Oavneet", "Pavneet", "Qavneet", "Ravneet",
				"Savneet", "Tavneet", "Uavneet", "Vavneet", "Wavneet", "Xavneet",
				"Yavneet", "Zavneet"};
		PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("E:\\sortdata", true)));

		List<String> list = new ArrayList<String> ();
		
		for(String f: namesArray) {
			for(String l: namesArray) {
				list.add(f + "\t" + l);
			}
		}

		Collections.shuffle(list);
		
		System.out.println(list.size());
		
		int recordCount = 0;
		for(String s: list) {
			recordCount ++;
			out.print(s);
			if(recordCount <= list.size()) {
				out.print(System.getProperty("line.separator"));
			}
			System.out.println(recordCount);
		}
		
		out.close();
	}
}