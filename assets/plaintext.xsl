<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:math="http://www.w3.org/2005/xpath-functions/math"
     xpath-default-namespace="http://www.tei-c.org/ns/1.0"
    exclude-result-prefixes="xs math"
    version="3.0" >
    <xsl:output method="text"></xsl:output>
    <xsl:strip-space elements="ab"/>
    <xsl:template match="TEI">
        <xsl:apply-templates select=".//body"/>
    </xsl:template>
    <xsl:template match="lb">
        <xsl:text>
{</xsl:text>
        <xsl:value-of select="@cert"/><xsl:text>}</xsl:text>
    </xsl:template>
    <xsl:template match="lb[1]">
        <xsl:text>{</xsl:text>
        <xsl:value-of select="@cert"/><xsl:text>}</xsl:text>
    </xsl:template>
    <xsl:template match="note|fw" />
    <xsl:template match="body|ab">
        <xsl:apply-templates/>
    </xsl:template>
    <xsl:template match="text()"><xsl:value-of select="normalize-space(.)"/></xsl:template>
</xsl:stylesheet>