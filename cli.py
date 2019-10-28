import argparse
from compute import ColumnsRemover
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument('input', metavar='input.csv',
                    type=str, help='Path to input.csv')
parser.add_argument('output', metavar='output.csv',
                    type=str, help='Path to output.csv')
parser.add_argument('rules', metavar='rules.json',
                    type=str, help='Path to rules.json')
args = parser.parse_args()


def main(args):
    columns_remover = ColumnsRemover(args.input, args.output, args.rules)

    startTime = datetime.now()
    columns_remover.read_csv()
    print('1. Read csv.\t\t\tExecution time: ' +
          str(datetime.now() - startTime))

    startTime = datetime.now()
    columns_remover.process()
    print('2. Remove columns.\t\tExecution time: ' +
          str(datetime.now() - startTime))

    startTime = datetime.now()
    columns_remover.write_to_file()
    print('3. Write to file\t\tExecution time: ' +
          str(datetime.now() - startTime))


if (__name__ == '__main__'):
    main(args)
