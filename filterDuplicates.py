

def filterDuplcatest(inputDf, forAllCoumns=True, *duplicateCheckColumns):
    if forAllCoumns:
        return inputDf.dropDuplicates
    else:
        return inputDf.dropDuplicates(duplicateCheckColumns)