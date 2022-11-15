import csv

def write_csv(o_path, data, header=None):
    with open(o_path,'a') as o_fp:
        writer = csv.writer(o_fp)
        if header is not None:
            writer.writerow(header)
        for row in data:
            writer.writerow(row)