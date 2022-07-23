import argparse


parser = argparse.ArgumentParser()
parser.add_argument("-N", action='store', required=True, type=int, dest='N')
parser.add_argument("-M", action='store', required=True, type=int, dest='M')
parser.add_argument('INPUT_DIRECTORY', type=str)
