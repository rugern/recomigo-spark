import os
import pyspark
from pyspark.mllib.recommendation import ALS

from lib.IO import printRed, printYellow, printGreen

sc = pyspark.SparkContext()

def sanitize(row):
    # if len(row) != 7:
        # printRed(row)
    return len(row) == 7

def readCSV(filename):
    assert os.path.isfile(filename)
    rdd = sc.textFile(filename)
    header = None
    # header = rdd.take(1)[0]
    rdd = rdd \
        .map(lambda line: line.split(",")) \
        .filter(sanitize) \
        .cache()
        # .filter(lambda line: line != header) \
    return header, rdd

def train(data):
    rank = 8
    seed = 5
    iterations = 10
    regularizationParameter = 0.1
    model = ALS.trainImplicit(data, rank, seed=seed, iterations=iterations,
                              lambda_=regularizationParameter)
    return model

def splitData(data):
    training, test = data \
        .randomSplit([7, 3], seed=0)
    test = test.map(lambda row: (row[0], row[1]))
    return training, test

def getUserId(data, userKey):
    return data.filter(lambda row: row[0] == userKey).first()[5]

def createRecommendations(model, rdd, ratings, userId):
    printRed(ratings.first())
    userRatings = ratings \
        .filter(lambda row: row[0] == userId) \
        .map(lambda row: row[1]).distinct().collect()

    userNotRated = ratings \
        .map(lambda row: row[1]) \
        .filter(lambda contentId: contentId not in userRatings) \
        .distinct() \
        .map(lambda contentId: (userId, contentId))
    predictions = model.predictAll(userNotRated)

    metadata = rdd.map(lambda row: (int(row[6]), (row[2], row[4]))).distinct()
    recommendations = predictions \
        .map(lambda pred: (pred.product, pred.rating)) \
        .join(metadata) \
        .map(lambda row: (row[0], row[1][0], row[1][1][0], row[1][1][1])) \
        .takeOrdered(25, key=lambda row: -row[1])

    return recommendations

def run(userKey, filename):
    printYellow('Reading data from drive')
    _, rdd = readCSV(filename)
    ratings = rdd.map(lambda row: (int(row[5]), int(row[6]), 1.0))
    userId = getUserId(rdd, userKey)

    if userId is None:
        printRed('Could not find user key in data. Aborting...')
        return None, None, None

    printYellow('Training model...')
    model = train(ratings)
    printGreen('Finished training the model')

    printYellow('Creating recommendations...')
    recommendations = createRecommendations(model, rdd, ratings, userId)
    printGreen('Finished creating recommendations')

    return recommendations
