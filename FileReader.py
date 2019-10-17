
#File Reader - to read files of format csv,json,xml,parquet,text

def csvReader(sparkSession, inputPath, schema = None, header = True, separator=";", inferSchema = False):
    return sparkSession.read.csv(path = inputPath, schema = schema, header = header, sep = separator, inferSchema = inferSchema )

def jsonReader(sparkSession, inputPath, schema = None, multiLine = False, inferSchema = False):
    return sparkSession.read.json(path = inputPath, schema = schema, multiLine = multiLine, inferSchema = inferSchema)

def textReader(sparkSession, inputPath):
    return sparkSession.read.text(inputPath)

def parquetReader(sparkSession, inputPath, schema = None, inferSchema = False):
    return sparkSession.read.parquet(path = inputPath, schema = schema, inferSchema = inferSchema)

#data bricks library required to read xml files
def xmlReader(sparkSession, inputPath, schema, *tags):
    return sparkSession.read.format('xml').options(",".join(tags)).load(inputPath, schema = schema)

