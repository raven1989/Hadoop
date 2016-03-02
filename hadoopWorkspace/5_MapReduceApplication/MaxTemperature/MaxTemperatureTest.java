import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class MaxTemperatureTest {
  
  @Test
  public void processesValidRecord() throws IOException {
    MaxTemperatureMapper mapper = new MaxTemperatureMapper();
    Text value = new Text("2016 00029");
    OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);
    mapper.map(null, value, output, null);
    mapper.close();
    verify(output).collect(new Text("2016"), new IntWritable(29));
  }
  
  @Test
  public void ignoresMissingTemperatureRecord() throws IOException {
    MaxTemperatureMapper mapper = new MaxTemperatureMapper();
    Text value = new Text("2016 +9999");
    OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);
    mapper.map(null, value, output, null);
    mapper.close();
    verify(output, never()).collect(any(Text.class), any(IntWritable.class));
  }
  
  @Test
  public void returnsMaxiumIntegerInValues() throws IOException {
    MaxTemperatureReducer reducer = new MaxTemperatureReducer();
    Text key = new Text("2006");
    Iterator<IntWritable> values = Arrays.asList(new IntWritable(10), new IntWritable(5)).iterator();
    OutputCollector<Text, IntWritable> output = mock(OutputCollector.class);
    reducer.reduce(key, values, output, null);
    reducer.close();
    verify(output).collect(key, new IntWritable(10));
  }

}
