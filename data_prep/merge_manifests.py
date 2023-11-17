import pandas as pd
import argparse


if __name__ == "__main__": 
    parser = argparse.ArgumentParser(description='Merge two manifest files')
    parser.add_argument(
        'manifest_1', 
        type=str,
        help='The LOCAL path to the first manifest file')
    parser.add_argument(
        'manifest_2', 
        type=str,
        help='The LOCAL path to the second manifest file')
    parser.add_argument(
        'output', 
        type=str, 
        help='The LOCAL path to the output manifest file')
    args = parser.parse_args()
    
    df1 = pd.read_csv(args.manifest_1)
    df2 = pd.read_csv(args.manifest_2)
    concatenated_df = pd.concat([df1, df2])
    concatenated_df.to_csv(args.output, index=False)