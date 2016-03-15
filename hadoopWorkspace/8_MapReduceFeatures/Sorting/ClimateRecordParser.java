import org.apache.hadoop.io.Text;

public class ClimateRecordParser {
  enum Reason {
    OK,
    MISSING,
    MALFORMED
  }
  private static final int MISSING = 9999;
  private String year;
  private int airTemperature;
  private String stationId;
  private Reason reason = Reason.OK;
  
  public void parse(String record) {
    reason = Reason.OK;
    if(record.isEmpty() || record.length()<15) {
      reason = Reason.MALFORMED;
      return;
    }
    year = record.substring(0, 4);
    if(record.charAt(5) == '+'){
      airTemperature = Integer.parseInt(record.substring(6, 10));
    }else{
      airTemperature = Integer.parseInt(record.substring(5, 10));
    }
    stationId = record.substring(11, 15);
    if(airTemperature == MISSING){
      reason = Reason.MISSING;
    }
  }
  
  public void parse(Text record) {
    parse(record.toString());
  }
  
  public boolean isMalformedTemperature() {
    return reason == Reason.MALFORMED;
  }
  public boolean isMissingTemperature() {
    return reason == Reason.MISSING;
  }
  public boolean isValidTemperature() {
    return reason == Reason.OK;
  }
  
  public String getYear() {
    return year;
  }
  public int getYearInt(){
    return Integer.parseInt(year);
  }
  public int getAirTemperature() {
    return airTemperature;
  }
  public String getStationId() {
    return stationId;
  }
  
}
