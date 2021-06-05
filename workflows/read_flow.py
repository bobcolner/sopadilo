import pandas as pd
import ray


def run(file_paths: list, reader=pd.read_feather, return_df: bool=False, on_ray: bool=False):

    if on_ray:
        reader_ray = ray.remote(reader)

    results = []
    for path in file_paths:
        if on_ray:
            output = reader_ray.remote(path)
        else:
            output = reader(path)

        results.append(output)
    
    if on_ray:
        results = ray.get(output)

    if return_df:
        results = pd.concat(results)

    return results
