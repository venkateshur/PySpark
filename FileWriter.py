#File writer - write files of format csv,json,xml,parquet,text

def csvWriter(processedDf, outputPath, mode ='overwrite', codec = None, *paritionColumns):
    if requiredPartitions(paritionColumns):
        processedDf.write.mode(mode).partitionBy(*paritionColumns).csv(outputPath, compression = codec)
    else:
        processedDf.write.mode(mode).csv(outputPath)


def jsonWriter(processedDf, outputPath, mode ='overwrite', codec = None,  *paritionColumns):
    if requiredPartitions(paritionColumns):
        processedDf.write.mode(mode).partitionBy(*paritionColumns).json(outputPath, compression = codec)
    else:
        processedDf.write.mode(mode).json(outputPath)


def parquetWriter(processedDf, outputPath, mode ='overwrite', codec = None, *paritionColumns):
    if requiredPartitions(paritionColumns):
        processedDf.write.mode(mode).partitionBy(*paritionColumns).parquet(outputPath, compression = codec)
    else:
        processedDf.write.mode(mode).parquet(outputPath)


def text(processedDf, outputPath, mode ='overwrite', codec = None, *paritionColumns):
    if requiredPartitions(paritionColumns):
        processedDf.write.mode(mode).partitionBy(*paritionColumns).text(outputPath, compression = codec)
    else:
        processedDf.write.mode(mode).text(outputPath)


#data bricks library required to read xml files
def xmlWriter(processedDf, outputPath, mode ='overwrite', codec = None, *paritionColumns):
    if requiredPartitions(paritionColumns):
        processedDf.write.mode(mode).partitionBy(*paritionColumns).format('xml').save(outputPath, compression = codec)
    else:
        processedDf.write.mode(mode).format('xml').save(outputPath)


def requiredPartitions(partitionsColumns):
    if len(partitionsColumns) > 0:
        return True
    else:
        return False
