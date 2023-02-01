import os
import json
from pathlib import Path

def sniff_json_columns(fname):
    with open(fname) as f:
        line = f.readline()
        obj = json.loads(line)

        return ', '.join(["json->>'{}' AS \"{}\"".format(x, x) for x in obj.keys()])

def view_for(view_name, fname, glob):
    view_name = view_name.replace('.', '_')
    if fname.endswith('.csv') or fname.endswith('.tsv'):
        return "CREATE VIEW \"{}\" AS SELECT * FROM read_csv_auto('{}', header=true)".format(view_name, glob)
    elif fname.endswith('.parquet'):
        return "CREATE VIEW \"{}\" AS SELECT * FROM '{}'".format(view_name, glob)
    elif fname.endswith('.ndjson') or fname.endswith('.jsonl'):
        # We need to sniff the first row of the file in order to build a good view
        columns = sniff_json_columns(fname)
        return "CREATE VIEW \"{}\" AS SELECT {} FROM read_json_objects('{}')".format(view_name, columns, glob)

def create_views(dirname):
    rv = []

    # Add in sorted order so the user sees alphabetically stable sort
    for f in sorted(os.scandir(dirname), key=lambda x: x.path):
        fname = f.path
        if f.is_dir():
            files = list(os.scandir(f.path))

            if not files:
                continue

            # We only sniff the first file, we assume all files in the directory
            # will have the same extension and shape. YOLO.
            file = files[0]
            rv.append(view_for(Path(fname).stem, file.path, os.path.join(fname, '*' + Path(file).suffix)))
        else:
            rv.append(view_for(Path(fname).stem, fname, fname))

    return [x for x in rv if x]
