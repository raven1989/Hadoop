import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SortByTemperatureUsingTotalOrderPartitioner extends Configured implements Tool {
//全局排序
  public int run(String[] args) throws Exception {
    // TODO Auto-generated method stub
    if(args.length != 2){
      System.out.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }
    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Total Order Partition");
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setInputFormat(SequenceFileInputFormat.class);
    //conf.setMapOutputKeyClass(IntWritable.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setCompressOutput(conf, true);
    SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.BLOCK);
    
    conf.setPartitionerClass(TotalOrderPartitioner.class);
    //采样器的第一个参数0.5表示采样率，即10个取5个，测试用的样本数据只有14条，所以预期是采7条;
    //第二个参数表示最大的样本数量；
    //第三个参数是最大的分区数，这个参数一般是比可用reducer的资源数少，在第六章中讲了分区数一般就是reducer任务数
    InputSampler.Sampler<IntWritable, Text> sampler = 
        new InputSampler.RandomSampler<IntWritable, Text>(0.2, 20, 5);
    
    Path input = FileInputFormat.getInputPaths(conf)[0];
    FileSystem fs = input.getFileSystem(conf);
    input = input.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    
    Job job = Job.getInstance(conf);
    Path partitionFile = new Path(input, "_partitions");
    TotalOrderPartitioner.setPartitionFile((Configuration)conf, partitionFile);
    //InputFormat inf = ReflectionUtils.newInstance(job.getInputFormatClass(), job.getConfiguration());
    //IntWritable[] samples = (IntWritable[])sampler.getSample(inf, job);
    //for(int i=0; i<samples.length; i++){
      //System.out.println(samples[0].toString());
    //}
    InputSampler.writePartitionFile(conf, sampler);
    
    URI partitionUri = new URI(partitionFile.toString()+"#_partitions");
    job.addCacheFile(partitionUri);
    
    JobClient.runJob(conf);
    
    return 0;
  }

  public static void main(String[] args) throws Exception {
    // TODO Auto-generated method stub
    int exitCode = ToolRunner.run(new SortByTemperatureUsingTotalOrderPartitioner() , args);
    System.exit(exitCode);

  }

}
