package nl.uva.cpp;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

public class HashtagCount {

	public static void main(String[] args) {
		int retval = 0;
		try {
			retval = ToolRunner.run(new Configuration(), new HashtagCountTool(), args);
		} catch (Exception e) {
			System.out.println(e.getMessage());
			retval = 2;			
		}
		System.exit(retval);
	}
}
