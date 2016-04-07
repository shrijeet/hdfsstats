package com.hdfsstats;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class Utils {

  public static final long ONE_GB = 1073741824L;

  public static DateTime utcDateTime(long instant) {
    return new DateTime(instant, DateTimeZone.UTC);
  }

  public static float bytesToGB(long bytes) {
    return (float)bytes / ONE_GB;
  }

  public static DateTime currentUTCDateTime() {
    return new DateTime(DateTimeZone.UTC);
  }

}
