package de.uniwue.dw.imports.data;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import de.uniwue.dw.core.model.data.CatalogEntryType;

public class PdfValue {
  private boolean inRow = false;

  public interface Transformable {
    public String transform(String param);
  }

  public enum Types {
    PDF_COL_ROW_VALUE, PDF_NEXT_BY_NUMBER, PDF_OTHER
  };

  public PdfValue(String name, boolean importThisIntoDB) {
    super();
    this.originalName = name;
    this.importThisIntoDB = importThisIntoDB;
  }

  public PdfValue(String name, String val) {
    super();
    this.originalName = name;
    this.val = val;
  }

  public PdfValue(String parameterParentname, String origDocRowname, int positionFromRowname, String origDocColumnname,
          CatalogEntryType cattype) {
    super();
    this.parentname = parameterParentname;
    this.originalName = origDocRowname;
    this.takeElementAtPos = positionFromRowname;
    this.columnname = origDocColumnname;
    this.cattype = cattype;
    setType(Types.PDF_NEXT_BY_NUMBER);
  }

  public PdfValue(String parameterParentname, boolean checkParent, String origDocRowname, int positionFromRowname,
          String parameterName, CatalogEntryType cattype, Transformable trans) {
    super();
    this.parentname = parameterParentname;
    this.originalName = origDocRowname;
    this.takeElementAtPos = positionFromRowname;
    this.parameterName = parameterName;
    this.cattype = cattype;
    this.trans = trans;
    this.checkParent = checkParent;
    setType(Types.PDF_NEXT_BY_NUMBER);
  }

  public PdfValue(String parameterParentname, boolean checkParent, String origDocRowname, int positionFromRowname,
          String parameterName, CatalogEntryType cattype, 
          Pattern patternFirst) {
    super();
    this.parentname = parameterParentname;
    this.originalName = origDocRowname;
    this.takeElementAtPos = positionFromRowname;
    this.parameterName = parameterName;
    this.cattype = cattype;
    this.patternFirst = patternFirst;
    this.checkParent = checkParent;
    setType(Types.PDF_NEXT_BY_NUMBER);
  }

  public PdfValue(String parameterParentname, boolean checkParent, String origDocRowname, int positionFromRowname,
          String parameterName, CatalogEntryType cattype) {
    super();
    this.parentname = parameterParentname;
    this.originalName = origDocRowname;
    this.takeElementAtPos = positionFromRowname;
    this.parameterName = parameterName;
    this.cattype = cattype;
    this.checkParent = checkParent;
    setType(Types.PDF_NEXT_BY_NUMBER);
  }

  public PdfValue(String parameterParentname, String origDocRowname, String origDocColumnname, String parameterName,
          CatalogEntryType parameterCattype) {
    super();
    this.parentname = parameterParentname;
    this.originalName = origDocRowname;
    this.columnname = origDocColumnname;
    this.parameterName = parameterName;
    this.cattype = parameterCattype;
    setType(Types.PDF_COL_ROW_VALUE);
  }

  public PdfValue(String parentname, String startAtElementName, int takeStartElementPos, String stopAtElementName,
          int takeStopElementPos, String prefix, String postfix, List<String> contains, CatalogEntryType cattype) {
    super();
    this.parentname = parentname;
    this.originalName = startAtElementName;
    this.takeElementAtPos = takeStartElementPos;
    this.stopAtElementName = stopAtElementName;
    this.takeStopElementPos = takeStopElementPos;
    this.prefix = prefix;
    this.postfix = postfix;
    this.listcontains = contains;
    this.cattype = cattype;
  }

  public PdfValue(String startAtElementName, int takeElementAtPos, List<String> contains) {
    super();
    this.originalName = startAtElementName;
    this.takeElementAtPos = takeElementAtPos;
    this.listcontains = contains;
  }

  public PdfValue(String startAtElementName, int takeElementAtPos, List<String> contains, boolean inRow) {
    super();
    this.originalName = startAtElementName;
    this.takeElementAtPos = takeElementAtPos;
    this.listcontains = contains;
    this.inRow = inRow;
  }

  public boolean getValuesPerRow() {
    return inRow;
  }

  @Override
  public String toString() {

    return getFullName() + " = " + val;
  }

  private Types valtype = Types.PDF_OTHER;

  public CatalogEntryType cattype = CatalogEntryType.Text;

  public boolean importThisIntoDB = true;

  public String parentname = null;

  public String originalName = null;

  private String parameterName = null;

  public String columnname = null;

  public Integer takeElementAtPos = 0;

  public String stopAtElementName = null;

  public Integer takeStopElementPos = 0;

  public String prefix = null;

  public String postfix = null;

  private String val = null;

  public boolean checkParent = false;

  public boolean hasVal() {
    return val != null;
  }

  public String getVal() {
    if (trans != null) {
      return trans.transform(val);
    } else if (patternFirst != null) {
      Matcher m = patternFirst.matcher(val);
      if (m.find()) {
        return m.group(1);
      }
    }

    return val;
  }

  public void setVal(String val) {
    this.val = val;
  }

  public List<String> listcontains = null;

  Transformable trans = null;

  Pattern patternFirst = null;

  public String getFullName() {
    if (parameterName != null) {
      return parameterName;
    } else {
      return (prefix == null ? "" : prefix + " ") + originalName + (postfix == null ? "" : " " + postfix);
    }
  }

  public Types getType() {
    return valtype;
  }

  public void setType(Types valtype) {
    this.valtype = valtype;
  }
};