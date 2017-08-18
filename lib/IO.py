import csv
from termcolor import cprint

printRed = lambda x: cprint(x, 'red')
printGreen = lambda x: cprint(x, 'green')
printYellow = lambda x: cprint(x, 'yellow')

def sanitize(row):
    for i in range(len(row)):
        row[i] = ' '.join(row[i].splitlines())
    return row

def writeCSV(data, filename):
    data = map(sanitize, data)
    with open(filename, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(data)
    printGreen('Finished writing {}'.format(filename.split('/')[-1]))
