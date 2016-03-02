import org.apache.hadoop.conf.Configuration;

public class ConfMergeAndOverride {

  public static void main(String[] args) {
    // TODO Auto-generated method stub
    Configuration conf = new Configuration();
    
    String conf1 = "conf-part1.xml";
    String conf2 = "conf-part2.xml";
    
    conf.addResource(conf1);
    System.out.printf("conf1 --> %s: %s\n", "color", conf.get("color"));
    System.out.printf("conf2 --> %s: %s\n", "weight", conf.get("weight"));
    System.out.printf("conf1 --> %s: %s\n", "size", conf.getInt("size", 0));
    System.out.printf("conf1 --> %s: %s\n", "breadth", conf.get("breadth", "wide"));
    
    conf.addResource(conf2);
    System.out.printf("conf2 --> %s: %s\n", "size", conf.getInt("size", 0));
    //设置final的属性不能覆盖
    System.out.printf("conf2 --> %s: %s\n", "weight", conf.get("weight"));

  }

}
