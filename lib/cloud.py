import csv
from google.cloud import bigquery
import numpy

from lib.IO import printRed, printYellow, printGreen

client = bigquery.Client(project="amedia-analytics-eu")

def performQuery(query):
    printYellow('Executing BQ request')
    job = client.run_sync_query(query)
    job.use_legacy_sql = False
    job.timeout_ms = 5000
    job.run()

    pageToken = None
    data = []

    iterator = job.fetch_data()
    for row in iterator:
        data.append(row)

    printGreen('BQ request finished!')
    return data

def categoricalLookup(column):
    lookup, categories = numpy.unique(column, return_inverse=True)
    return lookup, categories

def categorical(inData):
    data = numpy.array(inData)
    if len(data.shape) < 2:
        printRed('BQ data had the wrong shape!')
        printRed(data.shape)
        return None

    _, userIds = categoricalLookup(data[:, 0])
    _, contentIds = categoricalLookup(data[:, 3])
    data = numpy.column_stack((data, userIds))
    data = numpy.column_stack((data, contentIds))
    return data.tolist()

def fetchReadingData(paperList):
    papers = ','.join(["'{}'".format(paper) for paper in paperList])
    start = '2017-08-01'
    end = '2017-08-17'

    query = '''
SELECT
    a.a_user_key,
    a.a_virtual,
    b.a_title,
    b.a_acpid,
    b.url
FROM
    `amedia-analytics-eu.traffic.events` a
JOIN (
    SELECT
        a_acpid,
        ANY_VALUE(a_title) a_title,
        ANY_VALUE(url) url
    FROM 
        `amedia-analytics-eu.traffic.events`
    WHERE
        _PARTITIONTIME BETWEEN TIMESTAMP("{0}") AND TIMESTAMP("{1}")
        AND a_title IS NOT NULL
        AND a_acpid IS NOT NULL
        AND url IS NOT NULL
    GROUP BY 1
) b ON a.a_acpid=b.a_acpid
WHERE
    _PARTITIONTIME BETWEEN TIMESTAMP("{0}") AND TIMESTAMP("{1}")
    AND a_user_key IS NOT NULL
    AND a_virtual in ({2})
GROUP BY 1, 2, 3, 4, 5
    '''.format(start, end, papers)

    result = performQuery(query)
    return categorical(result)
