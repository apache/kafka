<xsl:transform xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="2.0">

<!--***********************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 ***********************************************************-->

<!-- This style sheet converts any rat-report.xml file.  -->

<xsl:template match="/">

  <html>
    <head>
     <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
     <style type="text/css">
    &lt;!--
body {margin-top: 0px;font-size: 0.8em;background-color: #F9F7ED;}

h1 {color:red;}
h2 {color:blue;}
h3 {color:green;}
h4 {color:orange;}

/* Table Design */

table,tr,td {text-align:center;font-weight:bold;border:1px solid #000;}
caption {color:blue;text-align:left;}
.notes, .binaries, .archives, .standards {width:25%;}
.notes {background:#D7EDEE;}
.binaries {background:#D0F2F4;}
.archives {background:#ABE7E9;}
.standards {background:#A0F0F4;}
.licenced, .generated {width:50%;}
.licenced {background:#C6EBDD;}
.generated {background:#ABE9D2;}
.java_note {background:#D6EBC6;}
.generated_note {background:#C9E7A9;}
.unknown {width:100%;background:#E92020;}
.unknown-zero {color:#00CC00;}
.center{text-align:center;margin:0 auto;}
--&gt;
     </style>
    </head>
    <body>
      <xsl:apply-templates/>
      <xsl:call-template name="generated"/>
    </body>
  </html>
</xsl:template>

<xsl:template match="rat-report">

  <h1>Rat Report</h1>
  <p>This HTML version (yes, it is!) is generated from the RAT xml reports using Saxon9B. All the outputs required are displayed below, similar to the .txt version.
           This is obviously a work in progress; and a prettier, easier to read and manage version will be available soon</p>
<div class="center">
<table id="rat-reports summary" cellspacing="0" summary="A snapshot summary of this rat report">
<caption>
Table 1: A snapshot summary of this rat report.
</caption>
  <tr>
    <td colspan="1" class="notes">Notes: <xsl:value-of select="count(descendant::type[attribute::name=&quot;notice&quot;])"/></td>
    <td colspan="1" class="binaries">Binaries: <xsl:value-of select="count(descendant::type[attribute::name=&quot;binary&quot;])"/></td>
    <td colspan="1" class="archives">Archives: <xsl:value-of select="count(descendant::type[attribute::name=&quot;archive&quot;])"/></td>
    <td colspan="1" class="standards">Standards: <xsl:value-of select="count(descendant::type[attribute::name=&quot;standard&quot;])"/></td>
  </tr>
  <tr>
    <td colspan="2" class="licenced">Apache Licensed: <xsl:value-of select="count(descendant::header-type[attribute::name=&quot;AL   &quot;])"/></td>
    <td colspan="2" class="generated">Generated Documents: <xsl:value-of select="count(descendant::header-type[attribute::name=&quot;GEN  &quot;])"/></td>
  </tr>
  <tr>
    <td colspan="2" class="java_note">Note: JavaDocs are generated and so license header is optional</td>
    <td colspan="2" class="generated_note">Note: Generated files do not require license headers</td>
  </tr>
  <tr>
<xsl:choose>
  <xsl:when test="count(descendant::header-type[attribute::name=&quot;?????&quot;]) &gt; 0">
    <td colspan="4" class="unknown"><xsl:value-of select="count(descendant::header-type[attribute::name=&quot;?????&quot;])"/> Unknown Licenses - or files without a license.</td>
  </xsl:when>
  <xsl:otherwise>
    <td colspan="4" class="unknown-zero"><xsl:value-of select="count(descendant::header-type[attribute::name=&quot;?????&quot;])"/> Unknown Licenses - or files without a license.</td>
  </xsl:otherwise>
</xsl:choose>
  </tr>
</table>
</div>
<hr/>
  <h3>Unapproved Licenses:</h3>

  <xsl:for-each select="descendant::resource[license-approval/@name=&quot;false&quot;]">
  <xsl:text>  </xsl:text>
  <xsl:value-of select="@name"/><br/>
  <xsl:text>
</xsl:text>
</xsl:for-each>
<hr/>

<h3>Archives:</h3>

<xsl:for-each select="descendant::resource[type/@name=&quot;archive&quot;]">
 + <xsl:value-of select="@name"/>
 <br/>
 </xsl:for-each>
 <hr/>

 <p>
   Files with Apache License headers will be marked AL<br/>
   Binary files (which do not require AL headers) will be marked B<br/>
  Compressed archives will be marked A<br/>
  Notices, licenses etc will be marked N<br/>
  </p>

 <xsl:for-each select="descendant::resource">
  <xsl:choose>
   <xsl:when test="license-approval/@name=&quot;false&quot;">!</xsl:when>
   <xsl:otherwise><xsl:text> </xsl:text></xsl:otherwise>
 </xsl:choose>
 <xsl:choose>
   <xsl:when test="type/@name=&quot;notice&quot;">N   </xsl:when>
   <xsl:when test="type/@name=&quot;archive&quot;">A   </xsl:when>
   <xsl:when test="type/@name=&quot;binary&quot;">B   </xsl:when>
   <xsl:when test="type/@name=&quot;standard&quot;"><xsl:value-of select="header-type/@name"/></xsl:when>
   <xsl:otherwise>!!!!!</xsl:otherwise>
 </xsl:choose>
 <xsl:text>      </xsl:text>
 <xsl:value-of select="@name"/><br/>
 <xsl:text>
 </xsl:text>
 </xsl:for-each>
 <hr/>

 <h3>Printing headers for files without AL header...</h3>

 <xsl:for-each select="descendant::resource[header-type/@name=&quot;?????&quot;]">

   <h4><xsl:value-of select="@name"/></h4>
  <xsl:value-of select="header-sample"/>
  <hr/>
</xsl:for-each>
<br/>

 <!-- <xsl:apply-templates select="resource"/>
    <xsl:apply-templates select="header-sample"/>
    <xsl:apply-templates select="header-type"/>
    <xsl:apply-templates select="license-family"/>
    <xsl:apply-templates select="license-approval"/>
    <xsl:apply-templates select="type"/> -->

</xsl:template>

<xsl:template match="resource">
  <div>
    <h3>Resource: <xsl:value-of select="@name"/></h3>
      <xsl:apply-templates/>
    </div>
</xsl:template>

<xsl:template match="header-sample">
  <xsl:if test="normalize-space(.) != ''">
  <h4>First few lines of non-compliant file</h4>
    <p>
      <xsl:value-of select="."/>
    </p>
    </xsl:if>
    <h4>Other Info:</h4>
</xsl:template>

<xsl:template match="header-type">
  Header Type: <xsl:value-of select="@name"/>
  <br/>
</xsl:template>

<xsl:template match="license-family">
  License Family: <xsl:value-of select="@name"/>
  <br/>
</xsl:template>

<xsl:template match="license-approval">
  License Approval: <xsl:value-of select="@name"/>
  <br/>
</xsl:template>

<xsl:template match="type">
  Type: <xsl:value-of select="@name"/>
  <br/>
</xsl:template>

<xsl:template name="generated">
</xsl:template>
</xsl:transform>
