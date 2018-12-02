/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.tools;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.Cast;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Flatten;
import org.apache.kafka.connect.transforms.HoistField;
import org.apache.kafka.connect.transforms.InsertField;
import org.apache.kafka.connect.transforms.MaskField;
import org.apache.kafka.connect.transforms.RegexRouter;
import org.apache.kafka.connect.transforms.ReplaceField;
import org.apache.kafka.connect.transforms.SetSchemaMetadata;
import org.apache.kafka.connect.transforms.TimestampConverter;
import org.apache.kafka.connect.transforms.TimestampRouter;
import org.apache.kafka.connect.transforms.ValueToKey;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

public class TransformationDoc {

    private static final class DocInfo {
        final String transformationName;
        final String overview;
        final ConfigDef configDef;

        private DocInfo(String transformationName, String overview, ConfigDef configDef) {
            this.transformationName = transformationName;
            this.overview = overview;
            this.configDef = configDef;
        }
    }

    private static final List<DocInfo> TRANSFORMATIONS = Arrays.asList(
            new DocInfo(InsertField.class.getName(), InsertField.OVERVIEW_DOC, InsertField.CONFIG_DEF),
            new DocInfo(ReplaceField.class.getName(), ReplaceField.OVERVIEW_DOC, ReplaceField.CONFIG_DEF),
            new DocInfo(MaskField.class.getName(), MaskField.OVERVIEW_DOC, MaskField.CONFIG_DEF),
            new DocInfo(ValueToKey.class.getName(), ValueToKey.OVERVIEW_DOC, ValueToKey.CONFIG_DEF),
            new DocInfo(HoistField.class.getName(), HoistField.OVERVIEW_DOC, HoistField.CONFIG_DEF),
            new DocInfo(ExtractField.class.getName(), ExtractField.OVERVIEW_DOC, ExtractField.CONFIG_DEF),
            new DocInfo(SetSchemaMetadata.class.getName(), SetSchemaMetadata.OVERVIEW_DOC, SetSchemaMetadata.CONFIG_DEF),
            new DocInfo(TimestampRouter.class.getName(), TimestampRouter.OVERVIEW_DOC, TimestampRouter.CONFIG_DEF),
            new DocInfo(RegexRouter.class.getName(), RegexRouter.OVERVIEW_DOC, RegexRouter.CONFIG_DEF),
            new DocInfo(Flatten.class.getName(), Flatten.OVERVIEW_DOC, Flatten.CONFIG_DEF),
            new DocInfo(Cast.class.getName(), Cast.OVERVIEW_DOC, Cast.CONFIG_DEF),
            new DocInfo(TimestampConverter.class.getName(), TimestampConverter.OVERVIEW_DOC, TimestampConverter.CONFIG_DEF)
    );

    private static void printTransformationHtml(PrintStream out, DocInfo docInfo) {
        out.println("<div id=\"" + docInfo.transformationName + "\">");

        out.print("<h5>");
        out.print(docInfo.transformationName);
        out.println("</h5>");

        out.println(docInfo.overview);

        out.println("<p/>");

        out.println(docInfo.configDef.toHtmlTable());

        out.println("</div>");
    }

    private static void printHtml(PrintStream out) throws NoSuchFieldException, IllegalAccessException, InstantiationException {
        for (final DocInfo docInfo : TRANSFORMATIONS) {
            printTransformationHtml(out, docInfo);
        }
    }

    public static void main(String... args) throws Exception {
        printHtml(System.out);
    }

}
