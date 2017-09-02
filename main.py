import os
from flask import Flask, abort, request, jsonify
from flask_cors import CORS

from lib.IO import printRed, printYellow, printGreen
from lib import IO, cloud
from sample import brain

path = '/Users/nils/Documents/jobb/amedia/recomigo/data/'
app = Flask(__name__)
CORS(app)

def createFilename(publications):
    publicationList = publications.split(',') if ',' in publications else [publications]
    return path + '-'.join(publicationList) + '.csv', publicationList

def saveData(publicationList, filename):
    printYellow('Fetching data for {}'.format(', '.join(publicationList)))
    data = cloud.fetchReadingData(publicationList)
    if data is None:
        return False
    IO.writeCSV(data, filename)
    return True

def assertOrSaveData(publications):
    filename, publicationList = createFilename(publications)
    dataExists = os.path.isfile(filename)
    if not dataExists:
        dataExists = saveData(publicationList, filename)
    return filename, publicationList, dataExists

@app.route('/recommendations/<userKey>')
def run(userKey=None):
    publications = request.args.get('publications', None)
    if userKey is None or publications is None:
        return abort(400)
    filename, _, dataExists = assertOrSaveData(publications)

    if not dataExists:
        return abort(400)

    recommendations = brain.run(userKey, filename)
    if recommendations is None:
        return abort(400)
    return jsonify(recommendations)

@app.route('/')
def main():
    return jsonify('OK')


if __name__ == '__main__':
    app.run(port=8080)

# ad3ec6fd-935c-4975-b6a3-2e984d3a0ff9
