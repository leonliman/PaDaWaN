package de.uniwue.dw.imports.configured.data;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import de.uniwue.dw.imports.DataElem;
import de.uniwue.dw.imports.DataElemCompare;
import de.uniwue.dw.imports.DataElemFile;
import de.uniwue.dw.imports.FileElemIterator;
import de.uniwue.dw.imports.IDataElemIterator;
import de.uniwue.dw.imports.ImportException;
import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.manager.ImportLogManager;

public class ConfigDataSourceFilesystem extends ConfigDataSource {

  public File dir;

  public ConfigDataSourceFilesystem(ConfigStructureElem aParent, File importDir,
          String anEncoding) {
    super(aParent, anEncoding);
    dir = importDir;
  }

  public ConfigDataSourceFilesystem(File importDir, String anEncoding) {
    this(null, importDir, anEncoding);
  }

  public ConfigDataSourceFilesystem(File importDir) {
    this(importDir, "UTF-8");
    dir = importDir;
  }

  public ConfigDataSourceFilesystem(ConfigStructureElem aParent) {
    super(aParent);
  }

  private void forceCacheFileTimestamps(ArrayList<DataElem> allFileNames) {
    final ExecutorService executor = Executors.newFixedThreadPool(10);
    final List<Future<?>> futures = new ArrayList<Future<?>>();
    for (DataElem anElem : allFileNames) {
      Future<?> future = executor.submit(() -> {
        anElem.getTimestamp();
      });
      futures.add(future);
    }
    try {
      int fetchedDocs = 0;
      for (Future<?> future : futures) {
        fetchedDocs++;
        if (fetchedDocs % 1000 == 0) {
          System.out.print(":");
        }
        future.get(); // do anything you need, e.g. isDone(), ...
      }
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
    executor.shutdown();
  }

  @Override
  public IDataElemIterator getDataElemsToProcess(String project, boolean doSort)
          throws ImportException {
    Collection<DataElem> result = new ArrayList<DataElem>();
    long time1 = System.nanoTime();
    if (dir == null) {
      throw new ImportException(ImportExceptionType.IMPORT_DIR_NONE_EXISTANT,
              "Import directory parameter not set", project);
    }
    if (!dir.exists()) {
      throw new ImportException(ImportExceptionType.IMPORT_DIR_NONE_EXISTANT,
              "Import directory '" + dir.getAbsolutePath() + "' does not exist", project);
    }
    ArrayList<DataElem> allFileNames = new ArrayList<DataElem>();
    getFilenamesRecursive(dir, allFileNames, project);
    if (allFileNames.isEmpty()) {
      // this can happen, although the directory exists, e.g. when access
      // permission is denied
      return new FileElemIterator(result);
    }
    // this was a trial to quicken reading the file timestamps of the huge amount of discharge
    // letters
    // forceCacheFileTimestamps(allFileNames);
    DataElem[] allFiles = allFileNames.toArray(new DataElem[0]);
    // Arrays.sort(allFiles,
    // LastModifiedFileComparator.LASTMODIFIED_COMPARATOR); // this is slow !
    if (doSort) {
      // sort files by creation time
      DataElemCompare[] pairs = new DataElemCompare[allFiles.length];
      for (int i = 0; i < allFiles.length; i++) {
        pairs[i] = new DataElemCompare(allFiles[i]);
      }
      Arrays.sort(pairs);
      for (int i = 0; i < allFiles.length; i++) {
        allFiles[i] = pairs[i].f;
      }
    }
    long time2 = System.nanoTime();
    ImportLogManager.info("selecting " + allFiles.length
            + " data elements to import from import source for project " + project + " took "
            + TimeUnit.NANOSECONDS.toMillis(time2 - time1) + " ms");
    result.addAll(Arrays.asList(allFiles));
    allFiles = new DataElem[0];
    return new FileElemIterator(result);
  }

  protected void getFilenamesRecursive(File dir, ArrayList<DataElem> result, String project)
          throws ImportException {
    String[] list = dir.list();
    if (list == null) { // this can happen when access on the directory is denied
      ImportLogManager.info("Directory " + dir.getAbsolutePath() + " in project " + project
              + " could not be accessed ");
      return;
    }
    for (String aFilename : list) {
      File aFile = new File(dir, aFilename);
      if (!aFilename.contains(".")) {
        if (aFile.isDirectory()) {
          getFilenamesRecursive(aFile, result, project);
        }
      } else {
        result.add(new DataElemFile(aFile, this));
      }
    }
  }

  @Override
  public void addDataElemsToProcess(String project, File afile) throws ImportException {
    // TODO Auto-generated method stub

  }
}
