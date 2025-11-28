package de.uniwue.dw.imports.manager;

import java.io.File;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import de.uniwue.dw.imports.FileCompare;

import de.uniwue.misc.sql.SQLTypes;

public class DataUtil {
  static Date minTime = null;

  static Date maxTime = null;

  public static int MONTH_IMPORT_MAP_PRECISION = 3;

  public static long IMPORT_MAP_PRECISION_MS = 30L * 24L * 60L * 60L * 1000L
          * Long.valueOf(MONTH_IMPORT_MAP_PRECISION);

  public static Collection<File> getFilesToProcess(File dir, String project) {
    Collection<File> result = new ArrayList<File>();
    long time1 = System.nanoTime();

    Collection<File> fileCol = FileUtils.listFiles(dir, FileFilterUtils.fileFileFilter(),
            FileFilterUtils.directoryFileFilter());
    if (fileCol == null) {
      // this can happen, although the directory exists, e.g. when acess
      // permission is denied
      return result;
    }
    File[] allFiles = fileCol.toArray(new File[0]);
    // sort files by creation time
    FileCompare[] pairs = new FileCompare[allFiles.length];
    for (int i = 0; i < allFiles.length; i++) {
      pairs[i] = new FileCompare(allFiles[i]);
    }

    Arrays.sort(pairs);

    for (int i = 0; i < allFiles.length; i++) {
      allFiles[i] = pairs[i].f;
    }

    long time2 = System.nanoTime();
    ImportLogManager.info("read files for " + project + " took "
            + TimeUnit.NANOSECONDS.toMillis(time2 - time1) + " ms");

    result.addAll(Arrays.asList(allFiles));
    return result;
  }

  public static Timestamp setMaxTimePerDay(Timestamp time) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(time);
    cal.set(Calendar.MINUTE, 59);
    cal.set(Calendar.HOUR_OF_DAY, 23);
    cal.set(Calendar.SECOND, 59);
    cal.set(Calendar.MILLISECOND, 999);
    Timestamp ret = new Timestamp(cal.getTime().getTime());
    return ret;
  }

  public static Timestamp setMinTimePerDay(Timestamp time) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(time);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    Timestamp ret = new Timestamp(cal.getTime().getTime());
    return ret;
  }

  public static Long getLongForCovariable(String value, int len) {

    byte[] code = DigestUtils.sha512(String.valueOf(value));

    return reduceBytesToInt(code, len);
  }
  
  private static long reduceBytesToInt(byte[] code, int prec) {
    int nPerDigit = code.length / prec;
    long nanos = 0;
    for (int i = 0; i < prec; i++) {
      int from = i * nPerDigit;
      int until;
      if (i < prec - 1)
        until = i * nPerDigit + nPerDigit;
      else
        until = code.length;
      int red = bytesReduce(code, from, until);
      
      nanos = 10 * nanos; 
      if(red == 0) {
        red = 1;
      }
      nanos += red;
    }
    nanos *= (int)Math.pow(10, 9-prec);
    return nanos;
  }
  
  public static Timestamp getSpecificTimeForCovariable(Timestamp applicationDateTime, String medExtIDConcat) {
    return getSpecificTimeForCovariable(applicationDateTime, medExtIDConcat, false);
  }
  public static Timestamp getSpecificTimeForCovariable(Timestamp applicationDateTime, String medExtIDConcat, boolean useSecMin) {

    // int first = (int) medExtIDConcat.charAt(0);
    // int middle = (int) medExtIDConcat.charAt((int) ((medExtIDConcat.length() - 1) / 2));
    // int last = (int) medExtIDConcat.charAt(medExtIDConcat.length() - 1);
    // long nr = first * 1000000 + middle * Math.pow(10, fr) + last;

    byte[] code = DigestUtils.sha512(medExtIDConcat);

    return getSpecificDateForByteArr(applicationDateTime, code, useSecMin);
  }

  public static boolean isTimeInValidRange(Date checkthis) {
    if (minTime == null) {
      Calendar cal = Calendar.getInstance();
      cal.set(1970, 1, 1);
      minTime = cal.getTime();
      cal.setTime(new Date());
      cal.add(Calendar.MONTH, +MONTH_IMPORT_MAP_PRECISION);
      maxTime = cal.getTime();
    }
    if (checkthis == null) {
      return false;
    }
    int compareToMax = checkthis.compareTo(maxTime);
    int compareToMin = checkthis.compareTo(minTime);
    if (compareToMax < 0 && compareToMin > 0) {
      return true;
    }
    return false;
  }

  public static Timestamp getSpecificTimeForCovariable(Timestamp applicationDateTime, Long nr) {
    int fr = SQLTypes.fractionalSecondsPrecision;
    return getSpecificTimeForCovariable(applicationDateTime, nr, fr);
  }

  public static Timestamp getSpecificTimeForCovariable(Timestamp applicationDateTime, Long nr, int prec) {

    /*
     * nr = nr ^ Long.MAX_VALUE;
     * 
     * Long x = 0l; int lb = Long.BYTES * 8; if (fr < lb) { for (int i = 0; i < lb; i += 20) { long
     * change = (nr >>> i); x ^= change; System.out.println(i + " " + Long.toBinaryString(change) +
     * " " + x + " " + Long.toBinaryString(x)); } x = (x>>>10) ^ (x>>>50); }
     */
    byte[] code = DigestUtils.sha512(String.valueOf(nr));

    return getSpecificDateForByteArr(applicationDateTime, code, prec);
  }

  private static Timestamp getSpecificDateForByteArr(Timestamp applicationDateTime, byte[] code, boolean useSecMin) {
    int fr = SQLTypes.fractionalSecondsPrecision;
    return getSpecificDateForByteArr(applicationDateTime, code, fr, useSecMin);
  }
  private static Timestamp getSpecificDateForByteArr(Timestamp applicationDateTime, byte[] code) {
    int fr = SQLTypes.fractionalSecondsPrecision;
    return getSpecificDateForByteArr(applicationDateTime, code, fr);
  }

  private static Timestamp getSpecificDateForByteArr(Timestamp applicationDateTime, byte[] code, int prec) {
    return getSpecificDateForByteArr(applicationDateTime, code, prec, false);
  }
    private static Timestamp getSpecificDateForByteArr(Timestamp applicationDateTime, byte[] code, int prec, boolean useSecMin) {
    // System.out.println(" ");
    Timestamp toReturn = (Timestamp) applicationDateTime.clone();

    /*
     * int ll = Long.BYTES; long x = bytesToLong(code, 10, ll); // BEST CURRENT
     * 
     * Integer nanos = (int) Math.abs(x % (int) Math.pow(10, prec)) * (int) Math.pow(10, 9 - prec);
     */

    /*
     * String xn = Long.toString(x); int nPerDigit = xn.length() / prec; nanos = 0; for (int i = 0;
     * i < prec; i++) { String part; if(i<prec-1) part = xn.substring(i * nPerDigit, i * nPerDigit +
     * nPerDigit); else part = xn.substring(i * nPerDigit); int partres = 0; for (int si = 0; si <
     * part.length(); si++) { char partc = part.charAt(si); partres += (int)partc; } String
     * partresstr = Integer.toString(partres); int partreslast =
     * Integer.parseInt(partresstr.substring(partresstr.length()-1)); if(partreslast == 0)
     * partreslast = 1; int tp = (int) Math.pow(10, i); int partreslastpow = (int) (tp *
     * partreslast); nanos += partreslastpow;
     * 
     * } nanos *= (int)Math.pow(10, 9-prec);
     */
    if(!useSecMin) {
      int nanos = (int) reduceBytesToInt(code, prec);
      toReturn.setNanos(nanos);
    } else {
      int addedparts = 2;
      int nPerDigit = code.length / (prec + addedparts);
      //int min = bytesMerge(code, 0, 1 * nPerDigit + nPerDigit -1 )  % 100;
      //int sec = bytesMerge(code, 2 * nPerDigit, 3 * nPerDigit + nPerDigit -1) % 100;
      int sec = bytesMerge(code, 0, 1 * nPerDigit + nPerDigit -1) % 100;
      int nanos = 0;
      for (int i = 2; i < prec + addedparts; i++) {
        int from = i * nPerDigit;
        int until;
        if (i < (prec + addedparts) - 1)
          until = i * nPerDigit + nPerDigit -1;
        else
          until = code.length;
        int red = bytesReduce(code, from, until);
        
        nanos = 10 * nanos; 
        if(red == 0) {
          red = 1;
        }
        nanos += red;
      }
      nanos *= (int)Math.pow(10, 9-prec);
      toReturn.setNanos(nanos);
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(toReturn.getTime());
      cal.add(Calendar.SECOND, sec - (sec/2) );
      //cal.add(Calendar.MINUTE, min - (min/2) );
      toReturn = new Timestamp(cal.getTime().getTime());
      
    }
     
    return toReturn;
  }
  
    private static int bytesMerge(byte[] code, int from, int until) {

      int red = 0;
      for (int i = from; i < until; i++) {
        red += Math.abs(code[i]);
      }
      return red;
    }

  private static int bytesReduce(byte[] code, int from, int until) {

    int red = bytesMerge(code, from,until);
    red = ( red % 13);
    if(red > 9)
      red = red % 10;
    return red;
  }

  private static long bytesToLong(byte[] code, int index, int length) {
    ByteBuffer buffer = ByteBuffer.allocate(length);
    buffer.put(code, index, length);
    buffer.flip();
    long t = buffer.getLong();
    return t;
  }

  public static long bytesToLong(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.put(bytes);
    buffer.flip();// need flip
    return buffer.getLong();
  }


}
