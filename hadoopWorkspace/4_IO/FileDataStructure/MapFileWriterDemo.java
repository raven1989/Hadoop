import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;

public class MapFileWriterDemo {
  private static final String[] DATA = {
      "Ten little soldier boys went out to dine, one chocked his little self and threre were nine.",
      "Nine little soldier boys sat up very late, one overslep himself and there were eight.",
      "Eight little soldier boys travelling in Devon, one said he'd stay there and there were seven.",
      "Seven little soldier boys chopping up sticks, one chopped himself and there were six.",
      "Six little soldier boys playing with a hive, a bumble-bee stung one and there were five.",
      "Five little soldier boys going in for law, one got in chancery and there were four.",
      "Four little soldier boys going to sea, a red herring swallowed one and there three.",
      "Three little soldier boys walking into the zoo, a big bear hugged one and there were two.",
      "Two little soldier boys sitting in the sun, one got frizzled up and there was one.",
      "One little soldier boy left all alone, he went and hanged himself and there were None."
  };

  public static void main(String[] args) throws IOException {
    // TODO Auto-generated method stub
    String uri = args[0];
    Configuration conf = new Configuration();
    IntWritable key = new IntWritable();
    Text value = new Text();
    MapFile.Writer writer = null;
    try{
      writer = new MapFile.Writer(conf, new Path(uri), 
          MapFile.Writer.keyClass(key.getClass()), 
          MapFile.Writer.valueClass(value.getClass()));
      for(int i=0; i<512; i++){
        key.set(i+1);
        value.set(DATA[i%DATA.length]);
        writer.append(key, value);
      }
    } finally {
      IOUtils.closeStream(writer);
    }

  }

}
