package de.uniwue.misc.util;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;

public class FileUtilsUniWue {

  public static void ensureDir(File aFile) {
    if (!aFile.exists()) {
      aFile.mkdir();
    }
  }

  /**
   * the extensions have to be given without the "."
   */
  public static Collection<File> listFiles(File dir, String[] extensions, boolean recursive) {
    return org.apache.commons.io.FileUtils.listFiles(dir, extensions, recursive);
  }

  public static void cleanDirectory(File aDir) throws IOException {
    org.apache.commons.io.FileUtils.cleanDirectory(aDir);
  }

  public static Collection<File> listFiles(File aDir) {
    return listFiles(aDir, false);
  }

  public static Collection<File> listFiles(File aDir, boolean recursive) {
    return listFiles(aDir, null, recursive);
  }

  public static String file2String(File aFile) throws IOException {
    return file2String(aFile, "UTF-8");
  }

  public static String file2String(File aFile, String encoding) throws IOException {
    return FileUtils.readFileToString(aFile, encoding);
  }

  public static void copyToDir(File aFile, File targetDir) throws IOException {
    File newFile = new File(targetDir, aFile.getName());
    org.apache.commons.io.FileUtils.copyFile(aFile, newFile);
  }

  public static void copyToTargetFile(File aFile, File targetFile) throws IOException {
    org.apache.commons.io.FileUtils.copyFile(aFile, targetFile);
  }

  public static void copyDir(File aDir, File target) throws IOException {
    org.apache.commons.io.FileUtils.copyDirectory(aDir, target);
  }

  public static void deleteDir(File aDir) throws IOException {
    org.apache.commons.io.FileUtils.deleteDirectory(aDir);
  }

  public static void moveFile(File aFile, File target) throws IOException {
    org.apache.commons.io.FileUtils.moveFile(aFile, target);
  }

  public static void saveString2File(String aText, File aFile, String encoding) throws IOException {
    FileUtils.write(aFile, aText, encoding);
  }

  public static void saveString2File(String aText, File aFile) throws IOException {
    FileUtils.write(aFile, aText, "UTF-8");
  }

}
