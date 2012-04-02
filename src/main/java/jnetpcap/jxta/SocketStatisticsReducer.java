package jnetpcap.jxta;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.SortedMapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

public class SocketStatisticsReducer extends Reducer<Text, SortedMapWritable, Text, Text> {
	
	@SuppressWarnings("rawtypes")
	protected void reduce(Text key, Iterable<SortedMapWritable> values, Context context) throws IOException, InterruptedException {
		StringBuilder strOutput = new StringBuilder();
		double sum = 0, i = 0;
		DoubleWritable val = null;
		
		SortedMapWritable sortedMap = values.iterator().next();
		strOutput.append("[");
		Set<WritableComparable> keys = sortedMap.keySet();
		for (WritableComparable mapKey : keys) {					
			val = (DoubleWritable)sortedMap.get(mapKey);
			sum += val.get();
			if(i > 0)
				strOutput.append(",");
			strOutput.append(val.get());
			i++;
		}
		strOutput.append("]");

		context.write(new Text(key.toString()), new Text(strOutput.toString()));
		context.write(new Text(key.toString() + "Med"), new Text(Double.toString(sum/i)));
	}
	
}