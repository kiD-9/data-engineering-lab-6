import pandas as pd
import json
import os.path

from pandas import DataFrame


def load_data(path):
    return pd.read_csv(path, sep=';')


def load_optimized_data_chunk(path, columns, optimized_dtypes, chunksize):
    return pd.read_csv(path, sep=';', usecols=lambda x: x in columns, dtype=optimized_dtypes, chunksize=chunksize)


def write_to_json(path, data):
    with open(path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)


def get_file_on_disk_size(path):
    size = round(os.path.getsize(path) / 1024 / 1024, 3)
    return f'File on disk size = {size} MiB'


def mem_usage(data: DataFrame):
    mem_usage_sum = data.memory_usage(deep=True).sum()
    size = round(mem_usage_sum / 1024 / 1024, 3)
    return f'{size} MiB'


def get_file_in_mem_size(data: DataFrame):
    return f'File load in memory size = {mem_usage(data)}'


def get_columns_stat(data: DataFrame):
    columns_info = data.memory_usage(deep=True)
    total_size = columns_info.sum()
    columns_stat = []
    for key in data.dtypes.keys():
        columns_stat.append({
            'name': key,
            'memory_usage': round(columns_info[key] / 1024, 1),
            'memory_part': round(columns_info[key] / total_size * 100, 3),
            'type': data.dtypes[key]
        })
    columns_stat.sort(key=lambda c: c['memory_usage'], reverse=True)
    for column in columns_stat:
        yield {
            'column': column['name'],
            'memory_usage': f'{column['memory_usage']} KiB',
            'memory_part': f'{column['memory_part']}%',
            'type': str(column['type'])

        }
        # yield f'{column['name']:13} - memory usage: {column['memory_usage']:7} KiB - {column['memory_part']:7}%. Type: {column['type']}'


def optimize_object(data: DataFrame):
    optimized_object = pd.DataFrame()
    dataset_obj = data.select_dtypes(include=['object']).copy()

    for column in dataset_obj.columns:
        count_unique = len(dataset_obj[column].unique())
        count = len(dataset_obj[column])
        if count_unique / count < 0.5:
            optimized_object[column] = dataset_obj[column].astype('category')
        else:
            optimized_object[column] = dataset_obj[column]

    print(f'Pre-optimized object columns size: {mem_usage(dataset_obj)}')
    print(f'Optimized object columns size: {mem_usage(optimized_object)}\n')

    return optimized_object


def optimize_int(data: DataFrame):
    dataset_int = data.select_dtypes(include=['int']).copy()
    optimized_int = dataset_int.apply(pd.to_numeric, downcast='unsigned')

    print(f'Pre-optimized int columns size: {mem_usage(dataset_int)}')
    print(f'Optimized int columns size: {mem_usage(optimized_int)}\n')

    return optimized_int


def optimize_float(data: DataFrame):
    dataset_float = data.select_dtypes(include=['float']).copy()
    optimized_float = dataset_float.apply(pd.to_numeric, downcast='float')

    print(f'Pre-optimized float columns size: {mem_usage(dataset_float)}')
    print(f'Optimized float columns size: {mem_usage(optimized_float)}\n')

    return optimized_float

# https://www.kaggle.com/datasets/mrdaniilak/russia-real-estate-2021
# Steps 1 - 3
path = './data/input_data.csv'
data = load_data(path)
file_on_disk_size = get_file_on_disk_size(path)
file_in_mem_size = get_file_in_mem_size(data)
print(file_on_disk_size)
print(file_in_mem_size)
columns_stat = list(get_columns_stat(data))
write_to_json("unoptimized.json", columns_stat)
# for c in columns_stat:
#     print(c)

# Steps 4 - 6
optimized_data = data.copy()

optimized_object = optimize_object(data)
optimized_int = optimize_int(data)
optimized_float = optimize_float(data)

optimized_data[optimized_object.columns] = optimized_object
optimized_data[optimized_int.columns] = optimized_int
optimized_data[optimized_float.columns] = optimized_float

# Step 7
file_in_mem_size = get_file_in_mem_size(optimized_data)
print(file_in_mem_size)
columns_stat = list(get_columns_stat(optimized_data))
write_to_json("optimized.json", columns_stat)

# Step 8
optimized_dtypes = dict()
for key in optimized_data.columns:
    optimized_dtypes[key] = optimized_data.dtypes[key]

json_dtypes = dict(optimized_dtypes)
for k, v in json_dtypes.items():
    json_dtypes[k] = str(v)
write_to_json('dtypes.json', json_dtypes)

columns = ['price', 'rooms', 'area', 'kitchen_area', 'date', 'level', 'levels', 'building_type', 'object_type',
                'id_region']

new_dataset_path = './data/plotting_data.csv'
has_header = True
for chunk in load_optimized_data_chunk(path, columns, optimized_dtypes, 100000):
    chunk.to_csv(new_dataset_path, mode='a', header=has_header, index=False)
    has_header = False