from mapreduce import driver
from mapreduce.driver.args import parser


if __name__ == '__main__':
    args = parser.parse_args()
    driver.serve(args.N, args.M, args.INPUT_DIRECTORY)
