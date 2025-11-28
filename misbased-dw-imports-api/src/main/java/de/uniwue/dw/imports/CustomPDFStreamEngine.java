package de.uniwue.dw.imports;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.pdfbox.contentstream.PDFStreamEngine;
import org.apache.pdfbox.contentstream.operator.DrawObject;
import org.apache.pdfbox.contentstream.operator.Operator;
import org.apache.pdfbox.contentstream.operator.OperatorProcessor;
import org.apache.pdfbox.contentstream.operator.state.*;
import org.apache.pdfbox.contentstream.operator.text.*;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSString;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.text.TextPosition;
import org.apache.pdfbox.util.Matrix;
import org.apache.pdfbox.util.Vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * PDFString is the result parameter returned vio the CustomPDFStreamEngine class.
 *
 * @author kaspar_m
 */
class PDFString {
    public COSString cosstr = null;

    public String unicode = "";

    public Matrix matrix = null;
}

/**
 * CustomShowText is the processor used for COSString text elements, that need to be specially
 * handled (transformed to unicode)
 *
 * @author kaspar_m
 */
class CustomShowText extends OperatorProcessor {

    PDFStreamEngine context;

    protected CustomShowText(PDFStreamEngine context) {
        super(context);
        this.context = context;
    }

    @Override
    public void process(Operator operator, List<COSBase> arguments) throws IOException {
        if (arguments.size() < 1) {
            // ignore ( )Tj
            return;
        }
        COSBase base = arguments.get(0);
        if (!(base instanceof COSString)) {
            // ignore
            return;
        }
        if (context.getTextMatrix() == null) {
            // ignore: outside of BT...ET
            return;
        }
        if (context instanceof CustomPDFStreamEngine cont) {
            cont.cstr = new PDFString();
            cont.cstr.cosstr = (COSString) base;
            context.showTextString(cont.cstr.cosstr.getBytes());

            cont.addToken();
        }

    }

    @Override
    public String getName() {
        return "Tj";
    }
}


/**
 * CustomPDFStreamEngine
 *
 * @author kaspar_m
 */
class CustomPDFStreamEngine extends PDFStreamEngine {

    protected ArrayList<List<TextPosition>> charactersByArticle = new ArrayList<List<TextPosition>>();
    protected PDFString cstr = new PDFString();
    List<Object> tokens = new ArrayList<Object>();

    /**
     * Constructor.
     */
    CustomPDFStreamEngine() throws IOException {
        addOperator(new BeginText(this));
        addOperator(new Concatenate(this));
        addOperator(new DrawObject(this)); // special text version
        addOperator(new EndText(this));
        addOperator(new SetGraphicsStateParameters(this));
        addOperator(new Save(this));
        addOperator(new Restore(this));
        addOperator(new NextLine(this));
        addOperator(new SetCharSpacing(this));
        addOperator(new MoveText(this));
        addOperator(new MoveTextSetLeading(this));
        addOperator(new SetFontAndSize(this));
        addOperator(new CustomShowText(this));
        addOperator(new ShowTextAdjusted(this));
        addOperator(new SetTextLeading(this));
        addOperator(new SetMatrix(this));
        addOperator(new SetTextRenderingMode(this));
        addOperator(new SetTextRise(this));
        addOperator(new SetWordSpacing(this));
        addOperator(new SetTextHorizontalScaling(this));
        addOperator(new ShowTextLine(this));
        addOperator(new ShowTextLineAndSpace(this));
    }

    public void addToken() {
        cstr.matrix = getTextMatrix();
        cstr.unicode = cstr.unicode.trim();
        tokens.add(cstr);
    }

    /**
     * A method provided as an event interface to allow a subclass to perform some specific
     * functionality when text needs to be processed.
     *
     * @param text The text to be processed.
     */
    protected void processTextPosition(TextPosition text) {

        // charactersByArticle.add(text);
        cstr.unicode += text.getUnicode();

    }

    @Override
    protected void showGlyph(Matrix textRenderingMatrix, PDFont font, int code, Vector displacement) throws IOException {
        cstr.unicode += font.toUnicode(code);
    }

    @Override
    protected void showFontGlyph(Matrix textRenderingMatrix, PDFont font, int code, Vector displacement) throws IOException {
        cstr.unicode += font.toUnicode(code);
    }

    public List<Object> getTokens() {
        return tokens;
    }


}
