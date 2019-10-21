from pyspark.sql.functions import udf


def formatUTF8String(inputDf, forAllColumns, *forColumns, replacement):
    for column in getColumns(inputDf, forAllColumns, forColumns):
        inputDf = inputDf.withColumn(column, validateUTF8AndModify(column, replacement))
        return inputDf


@udf("string")
def validateUTF8AndModify(data, replacement):
    try:
        data.decode('utf-8')
        if replacement is None:
            return None
        else:
            return ""
    except UnicodeError:
        return data


def getColumns(inputDf, forAllColumns=True, *forColumns):
    if forAllColumns:
        return inputDf.columns
    else:
        return forColumns
