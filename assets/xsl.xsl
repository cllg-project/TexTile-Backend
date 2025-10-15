<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:math="http://www.w3.org/2005/xpath-functions/math"
     xpath-default-namespace="http://www.tei-c.org/ns/1.0"
    exclude-result-prefixes="xs math"
    version="3.0">
    <xsl:output media-type="html" encoding="UTF-8"/>
    <xsl:template match="TEI">
        <div id="rendered-tei">
            <xsl:apply-templates select=".//text"/>
        </div>
    </xsl:template>
    <xsl:template match="body">
        <xsl:apply-templates />
    </xsl:template>
    <xsl:template match="lb"><br /></xsl:template>
    <xsl:template match="pb"><span class="pb">[<xsl:value-of select="@n" />]</span></xsl:template>
    <xsl:template match="note|fw"><span class="marginal"><xsl:apply-templates/></span></xsl:template>
    <xsl:template match="span[@rend='rubricated']"><span class="rubric"><xsl:apply-templates/></span></xsl:template>
</xsl:stylesheet>