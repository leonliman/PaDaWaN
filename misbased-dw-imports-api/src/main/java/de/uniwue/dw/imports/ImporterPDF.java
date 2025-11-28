package de.uniwue.dw.imports;

import de.uniwue.dw.imports.ImportException.ImportExceptionType;
import de.uniwue.dw.imports.configured.data.ConfigCatalogEntry;
import de.uniwue.dw.imports.configured.data.ConfigDataSourceCSVDir;
import de.uniwue.dw.imports.data.DocInfo;
import de.uniwue.dw.imports.data.PdfValue;
import de.uniwue.dw.imports.data.PdfValue.Types;
import de.uniwue.dw.imports.manager.DataUtil;
import de.uniwue.dw.imports.manager.ImportLogManager;
import de.uniwue.dw.imports.manager.ImporterManager;
import org.apache.pdfbox.Loader;
import org.apache.pdfbox.cos.COSFloat;
import org.apache.pdfbox.cos.COSInteger;
import org.apache.pdfbox.cos.COSString;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.util.Matrix;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Abstract class that provides architecture for all csv-based importers. Provides further
 * functionality neede for handling csv files.
 */
public abstract class ImporterPDF extends ImporterTable {
    Collection<File> pdffilesToProcess = null;
    String encoding;
    CsvPdfData data = new CsvPdfData();
    HashMap<String, PdfColumn> header = new HashMap<String, PdfColumn>();
    HashMap<Integer, PdfColumn> headerByInt = new HashMap<Integer, PdfColumn>();
    HashMap<String, PdfRow> rows = new HashMap<String, PdfRow>();
    HashMap<Integer, PdfRow> rowByInt = new HashMap<Integer, PdfRow>();
    DateFormat df = new SimpleDateFormat("yyyyMMdd_HHmmss");
    File mainImportDir;
    private final int maxRowDiff = 5;
    private final int maxHeaderDiff = 20;
    private PrintWriter writer = null;
    private File writerFile = null;
    private String writerName = null;
    private File pdfSourceDir;

    public ImporterPDF() {
    }

    public ImporterPDF(ImporterManager mgr, ConfigCatalogEntry aParentEntry, File aMainImportDir,
                       File aPDFSourceDir) throws ImportException {
        super(mgr, aParentEntry, aParentEntry.getProject(), new ConfigDataSourceCSVDir(aMainImportDir));

        mainImportDir = aMainImportDir;

        encoding = "UTF-8";

        setPDFSourceDir(aPDFSourceDir);
        addPdfValue(new PdfValue("docid", false));
        pdffilesToProcess = getSourceFilesToProcess();
    }

    public CsvPdfData getData() {
        return data;
    }

    public List<String> getDataNamesList() {
        List<String> res = new ArrayList<String>();
        for (PdfValue val : data.getValues()) {
            res.add(val.getFullName());
        }
        return res;
    }

    public void addColumn(int startElementPos, String name) {
        header.put(name, new PdfColumn(startElementPos, name));
    }

    public void addPdfValue(PdfValue val) {
        data.add(val);
        if (!rows.containsKey(val.originalName)) {
            rows.put(val.originalName, new PdfRow(val.originalName));
        }
        if (!header.containsKey(val.columnname)) {
            header.put(val.columnname, new PdfColumn(val.columnname));
        }
    }

    protected void addPdfValue(List<PdfValue> asList) {
        for (PdfValue val : asList) {
            addPdfValue(val);
        }
    }

    public File getPDFSourceDir() {
        return pdfSourceDir;
    }

    public void setPDFSourceDir(File nmainSourceDir) {
        pdfSourceDir = nmainSourceDir;
    }

    protected Collection<File> getSourceFilesToProcess() {

        if (getPDFSourceDir() == null) {
            Collection<File> result = new ArrayList<File>();
            return result;
        }

        return DataUtil.getFilesToProcess(getPDFSourceDir(), getProject());

    }

    public boolean containsCaseInsensitive(String s, ArrayList<PdfField> row) {
        for (PdfField val : row) {
            if (val.value.equalsIgnoreCase(s)) {
                return true;
            }
        }
        return false;
    }

    protected void setParameterValueFromRawdata(ArrayList<ArrayList<PdfField>> rawdata,
                                                boolean searchAllCsvData) throws ImportException {

        resetPdfValues();

        for (PdfValue val : data.getValues()) {

            // System.out.println("val" + val.name);
            boolean foundParent = false;
            for (int i = 0; i < rawdata.size(); i++) {
                ArrayList<PdfField> row = rawdata.get(i);

                if (containsCaseInsensitive(val.parentname, row))
                    foundParent = true;

                if (!val.checkParent || (val.checkParent && foundParent)) {
                    if (val.getType() == Types.PDF_COL_ROW_VALUE) {

                        for (PdfField pdfField : row) {

                            // System.out.println(pdfField.value + " " + pdfField.header + "=" + val.columnname +
                            // " " + pdfField.row
                            // + "=" + val.name);

                            if (pdfField.header != null && pdfField.row != null) {
                                if (pdfField.header.name.equals(val.columnname)
                                        && pdfField.row.name.equals(val.originalName)) {
                                    val.setVal(pdfField.value);
                                }
                            }
                        }

                    } else if (val.listcontains == null) {

                        if (containsCaseInsensitive(val.originalName, row)) {
                            setParameterValue(rawdata, val, i);
                            if (!searchAllCsvData)
                                break;
                        }

                    } else {
                        if (row.containsAll(val.listcontains)) {
                            setParameterValue(rawdata, val, i);
                            if (!searchAllCsvData)
                                break;
                        }

                    }
                }
            }
        }

    }

    private void resetPdfValues() {
        for (PdfValue val : data.getValues()) {
            val.setVal(null);
        }

    }

    String concat(ArrayList<PdfField> arrayList) {
        String res = "";
        for (int i = 0; i < arrayList.size(); i++) {
            if (i > 0)
                res += " ";
            res += arrayList.get(i).value;
        }
        return res;
    }

    private void setParameterValue(ArrayList<ArrayList<PdfField>> rawdata, PdfValue parameter, int i)
            throws ImportException {
        boolean multilineAgg = parameter.stopAtElementName != null && parameter.takeStopElementPos != null;

        if (!parameter.getValuesPerRow()) {
            // take elements from the whole stream

            if (!multilineAgg) {
                // standard, take single element
                if (rawdata.size() <= (i + parameter.takeElementAtPos))
                    throw new ImportException(ImportExceptionType.DATA_MALFORMED,
                            "importerpdf-setParameterValue: value element not available");
                parameter.setVal(concat(rawdata.get(i + parameter.takeElementAtPos)));

            } else {
                // take multiple elements
                String fullvalue = "";
                int xi = i + parameter.takeElementAtPos;
                while (rawdata.size() > xi - parameter.takeStopElementPos
                        && !containsCaseInsensitive(parameter.stopAtElementName,
                        rawdata.get(xi - parameter.takeStopElementPos))) {

                    if (fullvalue.length() > 0)
                        fullvalue += "\n";
                    fullvalue += concat(rawdata.get(xi));
                    xi++;
                }
                parameter.setVal(fullvalue);
            }

        } else {
            // take elements from a single row

            if (!multilineAgg) {
                // standard, take single element
                parameter.setVal(rawdata.get(i).get(parameter.takeElementAtPos).value);

            } else {
                // take multiple elements
                String fullvalue = "";
                int xi = parameter.takeElementAtPos;
                while (rawdata.get(i).size() > xi - parameter.takeStopElementPos
                        && !rawdata.get(i).get(xi - parameter.takeStopElementPos).value
                        .equalsIgnoreCase(parameter.stopAtElementName)) {
                    if (fullvalue.length() > 0)
                        fullvalue += " ";
                    fullvalue += rawdata.get(i).get(xi);
                    xi++;
                }
                parameter.setVal(fullvalue);
            }

        }
    }

    protected void setParameterValueFromRawdata(ArrayList<ArrayList<PdfField>> rawdata)
            throws ImportException {
        setParameterValueFromRawdata(rawdata, false);

    }

    /**
     * This method is intended to be called before the import of this domain is started
     *
     * @throws ImportException
     */

    protected void runBeforeImport() throws ImportException {
        preprocessSourceFiles();
        // System.out.println();
    }

    private void finishWriter() throws ImportException {
        if (writer != null)
            writer.close();
        if (writerName != null)
            writerName = null;
        if (writerFile != null)
            dataSource.addDataElemsToProcess(getProject(), writerFile);
        // filesToImport.add(writerFile);

        writer = null;
        writerFile = null;
    }

    private PrintWriter getWriter(DocInfo doc, File afile) throws ImportException {

        try {
            if (writer != null) {
                finishWriter();
            }
            if (writerName == null) {

                writerName = afile.getName() + ".txt";

            }
            writerFile = new File(mainImportDir + "\\" + writerName);
            writer = new PrintWriter(writerFile, encoding);

        } catch (FileNotFoundException | UnsupportedEncodingException e) {
            throw new ImportException(ImportExceptionType.FILE_IMPORT_ERROR,
                    "error while creating new file", e);

        }
        return writer;

    }

    public void preprocessSourceFiles() throws ImportException {

        int counter = 0;
        for (File aFile : pdffilesToProcess) {

            // ImportLogManager.info(new ImportException(ImportExceptionType.IMPORT_PROCESS, project
            // + ":  starting file '" + aFile.getName() + "'", project));
            Boolean fileImportSuccessfull = true;

            DataElemFile dfile = new DataElemFile(aFile, dataSource);

            try {

                if (ImportLogManager.isDataElemAlreadyImported(dfile, getProject())) {
                    // ImportLogManager.fileImportSkip(aFile, getProject());
                    continue;
                }
                /*
                 * csvFile = new CsvFile(aFile, encoding);
                 * csvFile.setRequiredCSVColumnHeaders(requiredHeaders); runBeforeImportInfoFileLines();
                 */

                if (fileImportSuccessfull) {
                    Pattern p = Pattern.compile("(\\d+)\\.pdf");
                    Matcher m = p.matcher(aFile.getName());
                    if (!m.find()) {

                        ImportLogManager.info(new ImportException(ImportExceptionType.IMPORT_PROCESS,
                                getProject() + ":  file '" + aFile.getName()
                                        + "' can not be used with this importer", getProject()));

                        continue;
                    }

                    String docIDStr = m.group(1);

                    DocInfo doc = getDocInfo(Long.parseLong(docIDStr));
                    CsvPdfData data = getCsvParameterFromSourceFile(aFile);

                    if (data != null) {
                        // first data element is docid, defined within constructor of ImportPDF

                        data.values.get(0).setVal(docIDStr);

                        commit();
                        PrintWriter pwriter = getWriter(doc, aFile);
                        pwriter.write(data.getHeaderLine());
                        pwriter.write(data.getValueLine());
                        finishWriter();
                    }
                    counter++;
                }

                // csvFile.close();

                ImportLogManager.fileImportSuccess(dfile, getProject());

                /*
                 * if (importerManager.moveImporterFiles(this) && DWImportsConfig.getMoveFilesAfterUpdate())
                 * { backupFile(aFile); }
                 */
            } catch (ImportException e) {
                ImportLogManager.error(e);
                fileImportSuccessfull = false;
                ImportLogManager.fileImportError(dfile, getProject());
                continue;
            }
        }

        finishWriter();
        commit();
    }

    public abstract CsvPdfData getCsvParameterFromSourceFile(File aFile) throws ImportException;

    protected ArrayList<ArrayList<PdfField>> extractRawData(File aFile, int pagenr,
                                                            boolean writeLines, boolean writeRowArrays, boolean writeObjects, PDFlineAggType aggtype)
            throws ImportException {

        headerByInt = new HashMap<Integer, PdfColumn>();

        ArrayList<ArrayList<PdfField>> rawdata = new ArrayList<ArrayList<PdfField>>();

        try {

            PDDocument pdDoc;
            pdDoc = Loader.loadPDF(aFile);
            PDPage page = pdDoc.getPage(pagenr);

            CustomPDFStreamEngine pse = new CustomPDFStreamEngine();
            pse.processPage(page);
            List<Object> tokens = pse.getTokens();

            pdDoc.close();

            // https://stackoverflow.com/questions/39485920/how-to-add-unicode-in-truetype0font-on-pdfbox-2-0-0
            // PDResources res = page.getResources();
            // for (COSName fontName : res.getFontNames())
            // {
            // PDFont font = res.getFont(fontName);
            // }

            // PDFTextStripper ts = new PDFTextStripper();
            // String x = ts.getText(pdDoc);

            if (writeLines) {
                InputStream cont = page.getContents();
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(cont));
                String line = bufferedReader.readLine();
                while (line != null) {
                    // System.out.println(line);
                    line = bufferedReader.readLine();
                }
            }

            // PDFStreamParser parser = new PDFStreamParser(page);
            // parser.parse();
            // List<Object> tokens = parser.getTokens();
            /*
             * List<Object> tokens = new ArrayList<Object>(); Object token; while ((token =
             * parser.parseNextToken()) != null) { tokens.add(token); }
             */

            // boolean parsingTextObject = true;
            // PDFTextObject textobj = new PDFTextObject();
            // for (int i = 0; i < tokens.size(); i++) {
            // Object next = tokens.get(i);
            // System.out.println( next);
            // if (next instanceof Operator) {
            // Operator op = (Operator) next;
            // switch (op.getName()) {
            // case "BT": // BT: Begin Text.
            // parsingTextObject = true;
            // textobj = new PDFTextObject();
            // break;
            // case "ET":
            // parsingTextObject = false;
            // System.out.println("Text: " + textobj.getText() + "@" + textobj.getX() + ","

            // + textobj.getY());
            // break;
            // case "Tj":
            // textobj.setText();
            // break;
            // case "Tm":
            // textobj.setMatrix();
            // break;
            // default:
            // //System.out.println("unsupported operation " + op.getName());
            // }
            // textobj.clearAllAttributes();
            // } else if (parsingTextObject) {
            // textobj.addAttribute(next);
            // }
            // }

            int lastVal = 0;
            ArrayList<PdfField> vals = new ArrayList<PdfField>();

            for (Object object : tokens) {
                if (writeObjects)
                    System.out.println(object);

                if (object instanceof PDFString cstr) {
                  Matrix matrix = ((PDFString) object).matrix;

                    int val = 0;
                    if (aggtype == PDFlineAggType.COL || aggtype == PDFlineAggType.ROW) {
                        if (aggtype == PDFlineAggType.COL) {
                            val = (int) cstr.matrix.createAffineTransform().getTranslateX();
                        } else if (aggtype == PDFlineAggType.ROW) {
                            val = (int) cstr.matrix.createAffineTransform().getTranslateY();
                        }
                        if (val != lastVal) { // System.out.println("work");
                            if (vals.size() > 0) {
                                if (writeRowArrays)
                                    System.out.println(vals);
                                rawdata.add(vals);
                                vals = new ArrayList<PdfField>();
                            }
                            lastVal = val;
                        }
                        PdfField vtmp = new PdfField(cstr.unicode, matrix.getTranslateX(),
                                matrix.getTranslateY());
                        setParameterHeader(vtmp);

                        vals.add(vtmp);

                        // System.out.println(vtmp.value + " " + vtmp.header);
                        // System.out.println();
                    } else {
                        PdfField vtmp = new PdfField(cstr.unicode, matrix.getTranslateX(),
                                matrix.getTranslateY());
                        setParameterHeader(vtmp);

                        vals.add(vtmp);
                        rawdata.add(vals);
                        if (writeRowArrays)
                            System.out.println(vals);
                        vals = new ArrayList<PdfField>();
                    }

                    if (rows.containsKey(cstr.unicode)) {
                        PdfRow tmp = rows.get(cstr.unicode);
                        tmp.x = matrix.getTranslateX();
                        tmp.y = matrix.getTranslateY();
                        rowByInt.put(Math.round(tmp.y), tmp);

                    }
                    if (header.containsKey(cstr.unicode)) {
                        PdfColumn tmp = header.get(cstr.unicode);
                        tmp.x = matrix.getTranslateX();
                        tmp.y = matrix.getTranslateY();
                        headerByInt.put(Math.round(tmp.x), tmp);

                    }
                }
            }
            rawdata.add(vals);
        } catch (IOException e) {
            throw new ImportException(ImportExceptionType.DATA_PARSING_ERROR,
                    "error while parsing pdf file " + aFile.getName(), e);
        }

        for (ArrayList<PdfField> parameterList : rawdata) {
            for (PdfField parameter : parameterList) {
                // System.out.println(parameter.value);
                setParameterRowName(parameter);
            }
        }

        return rawdata;
    }

    private void setParameterRowName(PdfField parameter) {

        ArrayList<Integer> hlist = new ArrayList<Integer>(rowByInt.keySet());
        Collections.sort(hlist);
        /*
         * Integer maxRowDiff = 0; for (int i = 1; i < hlist.size(); i++) { Integer tmpmi = hlist.get(i)
         * - hlist.get(i - 1); if (tmpmi > maxRowDiff) maxRowDiff = tmpmi; }
         */

        Integer vtmpint = 999;
        Integer vtmpsel = null;
        for (Integer thy : rowByInt.keySet()) {
            Integer vtd = 999;
            if (thy > parameter.y)
                vtd = Math.round(thy - parameter.y);
            else
                vtd = Math.round(parameter.y - thy);

            if (vtd < vtmpint) {
                vtmpint = vtd;
                vtmpsel = thy;
            }
        }
        if (vtmpint + 1 < maxRowDiff)
            parameter.row = rowByInt.get(vtmpsel);

    }

    private void setParameterHeader(PdfField parameter) {

        ArrayList<Integer> hlist = new ArrayList<Integer>(headerByInt.keySet());
        Collections.sort(hlist);
        /*
         * Integer maxHeaderDiff = 0; for (int i = 1; i < hlist.size(); i++) { Integer tmpmi =
         * hlist.get(i) - hlist.get(i - 1); if (tmpmi > maxHeaderDiff) maxHeaderDiff = tmpmi; }
         */

        Integer vtmpint = 999;
        Integer vtmpsel = null;
        for (Integer thx : headerByInt.keySet()) {
            Integer vtd = 999;
            if (thx > parameter.x)
                vtd = Math.round(thx - parameter.x);
            else
                vtd = Math.round(parameter.x - thx);

            if (vtd < vtmpint) {
                vtmpint = vtd;
                vtmpsel = thx;
            }
        }
        if (vtmpint + 1 < maxHeaderDiff)
            parameter.header = headerByInt.get(vtmpsel);

    }

    public enum PDFlineAggType {
        ROW, COL, COMBINED
    }

    public class PdfField {

        public String value;
        public float x;
        public float y;
        public PdfColumn header;
        public PdfRow row;

        public PdfField(String unicode, float translateX, float translateY) {
            value = unicode;
            x = translateX;
            y = translateY;
        }
    }

    public class PdfColumn {
        public String name;
        public boolean hasNumber = false;
        public int number;
        public int xstart;
        public int xend;
        public int width;
        public float x;
        public float y;

        public PdfColumn(int startElementPos, String name) {
            this.name = name;
            number = startElementPos;
            hasNumber = true;
        }

        public PdfColumn(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

    }

    public class PdfRow {
        public String name;
        public int xstart;
        public int xend;
        public int width;
        public float x;
        public float y;

        public PdfRow(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

    }

    public class CsvPdfData {

        String separator = "\t";

        String separatorReplacement = "_";

        String EOL = "\r\n";

        ArrayList<PdfValue> values = new ArrayList<PdfValue>();

        Map<String, PdfValue> valMap = new HashMap<String, PdfValue>();

        public ArrayList<PdfValue> getValues() {
            return values;
        }

        public String getHeaderLine() {
            String line = "";
            for (int i = 0; i < values.size(); i++) {
                if (i > 0)
                    line += separator;
                if (values.get(i) != null)
                    line += values.get(i).getFullName();
            }
            line += EOL;
            return line;
        }

        public String getValueLine() {
            String line = "";
            for (int i = 0; i < values.size(); i++) {
                if (i > 0)
                    line += separator;
                if (values.get(i) != null) {
                    PdfValue tmp = values.get(i);
                    if (tmp.hasVal())
                        line += tmp.getVal().replace(separator, separatorReplacement);
                }
            }
            line = line.replace("\r", "");
            line = line.replace("\n", "<br>");
            line += EOL;
            return line;
        }

        public void add(PdfValue value, int colIndex) {
            values.set(colIndex, value);
            valMap.put(value.getFullName().toUpperCase(), value);
        }

        public void add(PdfValue pdfValue) {
            values.add(pdfValue);
            valMap.put(pdfValue.getFullName().toUpperCase(), pdfValue);

        }

        public PdfValue get(String name) {
            return valMap.get(name.toUpperCase());
        }

    }

    class PDFTextObject {
        private List<Object> attributes = new ArrayList<Object>();

        private String text = "";

        private float x = -1;

        private float y = -1;

        public void clearAllAttributes() {
            attributes = new ArrayList<Object>();
        }

        public void addAttribute(Object anAttribute) {
            attributes.add(anAttribute);
        }

        public void setText() {
            // Move the contents of the attributes to the text attribute.
            for (int i = 0; i < attributes.size(); i++) {
                if (attributes.get(i) instanceof COSString aString) {
                  text = text + aString.getString();
                } else {
                    System.out.println("Whoops! Wrong type of property...");
                }
            }
        }

        public String getText() {
            return text;
        }

        public void setMatrix() {
            // Move the contents of the attributes to the x and y attributes.
            // A Matrix has 6 attributes, the last two of which are x and y
            for (int i = 4; i < attributes.size(); i++) {
                float curval = -1;
                if (attributes.get(i) instanceof COSInteger aCOSInteger) {
                  curval = aCOSInteger.floatValue();

                }
                if (attributes.get(i) instanceof COSFloat aCOSFloat) {
                  curval = aCOSFloat.floatValue();
                }
                switch (i) {
                    case 4:
                        x = curval;
                        break;
                    case 5:
                        y = curval;
                        break;
                }
            }
        }

        public float getX() {
            return x;
        }

        public float getY() {
            return y;
        }
    }

}
