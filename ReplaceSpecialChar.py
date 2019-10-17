
from pyspark.sql.functions import regexp_replace

def replaceTabChar(inputDf, replacementChar = '', forAllCoumns=True, *forColumns):
    return replaceChar(inputDf, "\\t", replacementChar, forAllCoumns, *forColumns)

def replaceNewLineChar(inputDf, replacementChar = '', forAllCoumns=True, *forColumns):
    return replaceChar(inputDf, "\\n", replacementChar, forAllCoumns, *forColumns)

def replacelineBreakChar(inputDf, replacementChar = '', forAllCoumns=True, *forColumns):
    return replaceChar(inputDf, "\\^M", replacementChar, forAllCoumns, *forColumns)

def getColumns(inputDf):
    return inputDf.columns

def replaceChar(inputDf, replaceChar, replacementChar, forAllCoumns, *forColumns):
    if forAllCoumns:
        for column in getColumns(inputDf):
            inputDf = inputDf.withColumn(column, regexp_replace(column, replaceChar, replacementChar))
        return inputDf
    else:
        for column in forColumns:
            inputDf = inputDf.withColumn(column, regexp_replace(column, replaceChar, replacementChar))
        return inputDf


