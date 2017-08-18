import numpy
import implicit
from termcolor import cprint
from scipy.sparse import coo_matrix

printRed = lambda x: cprint(x, 'red')
printGreen = lambda x: cprint(x, 'green')
printYellow = lambda x: cprint(x, 'yellow')
filename = '/Users/nils/Documents/jobb/amedia/recomigo/data/glomdalen.npy'

def categorical(data):
    lookup, categories = numpy.unique(data, return_inverse=True)
    return lookup, categories

def transform(data):
    data = data[:, [0, 3]]
    for i in range(data.shape[1]):
        _, data[:, i] = categorical(data[:, i])
    data = data.astype(numpy.uint32)
    return coo_matrix((numpy.full(data.shape[0], 1.0), (data[:, 0], data[:, 1])))

def run():
    printYellow('Loading data')
    data = numpy.load(filename)
    data = transform(data)
    model = implicit.als.AlternatingLeastSquares(
        factors=50,
        regularization=0.01,
        use_native=True,
        use_cg=False,
        dtype=numpy.float64,
        iterations=15
    )

    printYellow('Calculating similar artists')
    model.fit(data)
    printGreen('Finished :D:D')
