import org.apache.hadoop.io.Text;

public class ClimateRecordParser {
  private static final int MISSING = 9999;
  private String year;
  private int airTemperature;
  private String stationId;
  
  public void parse(String record) {
    year = record.substring(0, 4);
    if(record.charAt(5) == '+'){
      airTemperature = Integer.parseInt(record.substring(6, 10));
    }else{
      airTemperature = Integer.parseInt(record.substring(5, 10));
    }
    stationId = record.substring(11, 15);
  }
  
  public void parse(Text record) {
    parse(record.toString());
  }
  
  public boolean isValidTemperature() {
    return airTemperature!=MISSING;
  }
  
  public String getYear() {
    return year;
  }
  public int getAirTemperature() {
    return airTemperature;
  }
  public String getStationId() {
    return stationId;
  }
  
}
