import os, sys 
from concurrent.futures import ThreadPoolExecutor 
from argparse import ArgumentParser

parser = ArgumentParser()


def generateFacilitiesData(args):
    dataGenerat: list = []
    with open(os.path.join(os.getcwd(), 'input.txt'), 'r', encoding='utf-8') as openFile:
        openFile.writelines(dataGenerat)
    

if __name__ == "__main__":
    generateFacilitiesData(sys)
    