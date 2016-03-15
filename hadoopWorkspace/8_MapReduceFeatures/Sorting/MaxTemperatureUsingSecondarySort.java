import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.examples.SecondarySort.IntPair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperatureUsingSecondarySort extends Configured implements Tool {
  //基本思路：按（年，气温）组合键排序，按照年分区，区内按照年分组(一组记录是key相同的数据集调用一次reduce)。
  //每组的第一条记录即是当年的最高气温
  
  static class MyIntPair extends IntPair {
    @Override
    public String toString() {
      return String.format("%s\t%s", this.getFirst(), this.getSecond());
    }
  }
  
  static class MaxTemperatureMapper 
    extends MapReduceBase 
    implements Mapper<LongWritable, Text, MyIntPair, NullWritable> {
    private ClimateRecordParser parser = new ClimateRecordParser();
    public void map(LongWritable key, Text value, 
        OutputCollector<MyIntPair, NullWritable> output, Reporter reporter) 
            throws IOException {
      parser.parse(value);
      if(parser.isValidTemperature()) {
        MyIntPair pair = new MyIntPair();
        pair.set(parser.getYearInt(), parser.getAirTemperature());
        output.collect(pair, NullWritable.get());
      }
    }
  }
  
  static class MaxTemperatureReducer 
    extends MapReduceBase 
    implements Reducer<MyIntPair, NullWritable, MyIntPair, NullWritable> {
    public void reduce(MyIntPair key, Iterator<NullWritable> values, 
        OutputCollector<MyIntPair, NullWritable> output, Reporter reporter) 
            throws IOException {
      output.collect(key, NullWritable.get());
    }
  }
  
  public static class FirstPartitioner implements Partitioner<MyIntPair, NullWritable> {
    public void configure(JobConf job) {
      // TODO Auto-generated method stub      
    }
    public int getPartition(MyIntPair key, NullWritable value, int numPartitions) {
      // TODO Auto-generated method stub
      return Math.abs(key.getFirst()*127) % numPartitions;
    }
  }
  
  public static class KeyComparator extends WritableComparator {
    protected KeyComparator() {
      super(MyIntPair.class, true);
    }
    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable w1, WritableComparable w2) {
      MyIntPair ip1 = (MyIntPair)w1;
      MyIntPair ip2 = (MyIntPair)w2;
      //年份从小到大
      int cmp = Integer.compare(ip1.getFirst(), ip2.getFirst());
      if(cmp != 0) return cmp;
      //气温从大到小
      return -Integer.compare(ip1.getSecond(), ip2.getSecond());
    }
  }
  
  public static class GroupComparator extends WritableComparator {
    protected GroupComparator() {
      super(MyIntPair.class, true);
    }
    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable w1, WritableComparable w2) {
      MyIntPair ip1 = (MyIntPair)w1;
      MyIntPair ip2 = (MyIntPair)w2;
      return Integer.compare(ip1.getFirst(), ip2.getFirst());
    }
  }

  public int run(String[] args) throws Exception {
    // TODO Auto-generated method stub
    if(args.length != 2){
      System.out.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Secondary Sort");
    conf.setJar("mss.jar");
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setMapperClass(MaxTemperatureMapper.class);
    //首先，setOutputxxx设置的是最终的输出，也就是reducer的输出，
    //通常mapper的输出与最终输出默认是一致的，同时mapper的输出又是reducer的输入；
    //所以这个设置的是mapper的输出，reducer的输入和输出。如果需要不一样，可以设置setMapxxx。
    conf.setOutputKeyClass(MyIntPair.class);
    conf.setOutputValueClass(NullWritable.class);
    //那么，setOutputKeyComparatorClass设置的KeyComparator会在map输出时，reduce输入时用到。
    //根据第六章map-reduce机制中降到的，map在输出时会用KeyComparator去排序（按照设定的年降温度升排序），
    conf.setOutputKeyComparatorClass(KeyComparator.class);
    //接下来其作用的是Partitioner，根据FirstPartitioner的设定，仅按照年来分区，所有年相同的记录被分到同一个分区；
    //然后进入reduce阶段，reduce对收到各个map的结果同样使用KeyComparator进行merge，
    //merge后的结果仍然是年降温度升排序，这时候每一个reduce获得的同一个key的数据就是完整的了。
    conf.setPartitionerClass(FirstPartitioner.class);
    //仅用分区还不够，因为接下来的分组默认还是使用设定的（年，温度）key，即每次输入到reduce方法的数据组是（年，温度）都相同的，
    //所以最后设定GroupComparator，使得分组仅按照年，由于记录已经是排好序的，所以每次传入reduce的key是记录的第一条，
    //也就是同一年温度最大的一条
    conf.setOutputValueGroupingComparator(GroupComparator.class);
    conf.setReducerClass(MaxTemperatureReducer.class);
    
    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    int exitCode = ToolRunner.run(new MaxTemperatureUsingSecondarySort(), args);
    System.exit(exitCode);

  }

}
