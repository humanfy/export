package kairosdb.export.csv.utils;

import kairosdb.export.csv.conf.Constants;
import org.joda.time.DateTime;

public class TimeUtils {


  public static long convertDateStrToTimestamp(String dateStr) {
    DateTime dateTime = new DateTime(dateStr);
    return dateTime.getMillis();
  }

  /**
   * 按天拆分线程
   */
  public static int timeRange(long startTime, long endTime) {
    return (int) Math.ceil((float) (endTime - startTime) / Constants.TIME_DAY);
  }


}
