package kairosdb.export.csv.conf;

public class Config {

  public String START_TIME = "2018-8-30T00:00:00+08:00";
  public String ENDED_TIME = "2018-8-31T00:00:00+08:00";
  public boolean DELETE_CSV = true;
  public boolean INSERTINTOIOTDB = false;
  public int THREAD_NUM = 128;
  public String dirAbsolutePath = "/data/fy/res";
  public String tmpPath = "/data/fy/tmp";
  public int HASH_NUM = 0;
  public int TOTAL_HASH = 10;
  public boolean IS_MERGE = true;
  public String IOTDB_IP = "127.0.0.1";

  Config() {
  }

}
