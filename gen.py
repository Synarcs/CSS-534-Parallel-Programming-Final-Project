import os
import sys
from argparse import ArgumentParser
import random

parser = ArgumentParser()

def generateFacilitiesData(facilitySize, gridSize):
    data = list()

    prefix_list = ['F' + str(i) for i in range(1, facilitySize + 1)]
    for prefix in prefix_list:
        for i in range(1, gridSize + 1):
            value = format(random.randint(0, 1 << int(gridSize/2)), '0{0}b'.format(gridSize))
            featureMap = "{0} {1} {2}\n".format(prefix, i, value)
            data.append(featureMap)

    return data

if __name__ == "__main__":
    # lazy to switch to argparser
    args = list(sys.argv)
    if len(args) == 1:
        print('require objects, grid size') # also add the feature types later
        exit(1)

    random_data = generateFacilitiesData(int(sys.argv[1]), int(sys.argv[2]))
    if os.path.exists(os.path.join(os.getcwd(), 'input.txt')):
        os.remove(os.path.join(os.getcwd(), 'input.txt'))
    with open(os.path.join(os.getcwd(), 'input.txt'), 'w+') as inputGrid:
        inputGrid.writelines(random_data)
