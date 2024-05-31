import json
import os
from multiprocessing import Process
from cffi.backend_ctypes import xrange

'''This code is only tested on Linux'''


def get_input_file(input_dir=None):
    if not (input_dir is None):
        return input_dir + "/data.csv"
    return "./data.csv"


def get_output_join_file(output_dir=None):
    if not (output_dir is None):
        return output_dir + "/output.csv"
    return "./output.csv"


def get_temp_map_file(index, reducer, output_dir=None):
    if not (output_dir is None):
        return output_dir + "/map_data_" + str(index) + "-" + str(reducer) + ".tmp"
    return "./map_data_" + str(index) + "-" + str(reducer) + ".tmp"


def get_output_file(index, output_dir=None):
    if not (output_dir is None):
        return output_dir + "/reduce_data_" + str(index) + ".out"
    return "./reduce_data_" + str(index) + ".out"


# import utils
def get_input_split_file(index, input_dir=None):
    if not (input_dir is None):
        return input_dir + "/data_" + str(index) + ".csv"
    return "./data_" + str(index) + ".csv"


def split_file(split_idx, idx):
    split_file = open(get_input_split_file(split_idx - 1), "w+")
    split_file.write(str(idx) + "\n")
    return split_file


def is_char_split_pos(char, idx, split_sz, curr_split):
    return idx > split_sz * curr_split + 1 and char.isspace()


def is_line_split_pos(idx, split_sz, curr_split):
    return idx > split_sz * curr_split + 1


class FileHandler(object):
    def __init__(self, input_file, output_folder):
        self.input_file = input_file
        self.output_folder = output_folder

    def char_split_file(self, num_splits):
        file_sz = os.path.getsize(self.input_file)
        unit_sz = file_sz / num_splits + 1
        orig_file = open(self.input_file, "r")
        file_cnt = orig_file.read()
        orig_file.close()
        (idx, curr_split_idx) = (1, 1)
        curr_split_unit = split_file(curr_split_idx, idx)
        for char in file_cnt:
            curr_split_unit.write(char)
            if is_char_split_pos(char, idx, unit_sz, curr_split_idx):
                curr_split_unit.close()
                curr_split_idx += 1
                curr_split_unit = split_file(curr_split_idx, idx)
            idx += 1
        curr_split_unit.close()

    def line_split_file(self, num_splits):
        with open(self.input_file, "r") as input_file:
            line_cnt = sum(1 for _ in input_file)
        unit_sz = line_cnt // num_splits + 1
        (idx, curr_split_idx) = (1, 1)
        curr_split_unit = split_file(curr_split_idx, idx)
        with open(self.input_file, "r") as input_file:
            for line in input_file:
                curr_split_unit.write(line)
                if is_line_split_pos(idx, unit_sz, curr_split_idx):
                    curr_split_unit.close()
                    curr_split_idx += 1
                    curr_split_unit = split_file(curr_split_idx, idx)
                idx += 1
            curr_split_unit.close()

    def merge_files(self, num_files, clean=False, sort=True, decreasing=True):
        output_merge_list = []
        for reducer_idx in xrange(0, num_files):
            f = open(get_output_file(reducer_idx), "r")
            output_merge_list += json.load(f)
            f.close()
            if clean:
                os.unlink(get_output_file(reducer_idx))
        if sort:
            from operator import itemgetter as operator_ig
            output_merge_list.sort(key=operator_ig(1), reverse=decreasing)
        output_merge_file = open(get_output_join_file(self.output_folder), "w+")

        for item in output_merge_list:
            output_merge_file.write('{},{}\n'.format(item[0], item[1]))
        output_merge_file.close()
        return output_merge_list


def map_func(key, value):
    results = []
    count = 1
    value = [s for s in value.split('\n') if s]
    for v in value:
        results.append((v.split(',')[0], count))
    return results


def reduce_func(key, values):
    wordcount = sum(value for value in values)
    return key, wordcount


class MapReduceProcess(object):
    def __init__(self, input_folder, output_folder,
                 num_mappers, num_reducers,
                 clean=True):
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.clean = clean
        self.file_handler = FileHandler(get_input_file(self.input_folder), self.output_folder)
        self.file_handler.line_split_file(self.num_mappers)

    def run_map(self, idx):
        input_split_file = open(get_input_split_file(idx), "r")
        key = input_split_file.readline()
        value = input_split_file.read()
        input_split_file.close()
        if self.clean:
            os.unlink(get_input_split_file(idx))
        print(f"Mapper{idx + 1}: Start mapping...")
        map_result = map_func(key, value)
        print(f"Mapper{idx + 1}: Finish mapping")
        print(f"Mapper{idx + 1}: Start suffling...")
        for reducer_idx in range(self.num_reducers):
            temp_map_file = open(get_temp_map_file(idx, reducer_idx), "w+")
            json.dump([(key, value) for (key, value) in map_result
                       if self.check_pos(key, reducer_idx)]
                      , temp_map_file)
            temp_map_file.close()
            print(f"Mapper{idx + 1}: suffle for reducer{reducer_idx}")
        print(f"Mapper{idx + 1}: Finish suffling")

    def run_reduce(self, idx):
        key_values_map = {}
        print(f"Reducer{idx + 1}: Start reducing...")
        for mapper_idx in range(self.num_mappers):
            temp_map_file = open(get_temp_map_file(mapper_idx, idx), "r")
            map_results = json.load(temp_map_file)
            for (key, value) in map_results:
                if not (key in key_values_map):
                    key_values_map[key] = []
                key_values_map[key].append(value)

            temp_map_file.close()
            if self.clean:
                os.unlink(get_temp_map_file(mapper_idx, idx))
        key_value_list = []
        for key in key_values_map:
            key_value_list.append(reduce_func(key, key_values_map[key]))
        print(f"Reducer{idx + 1}: Finish reducing...")
        output_file = open(get_output_file(idx), "w+")
        json.dump(key_value_list, output_file)
        output_file.close()

    def run(self, join=True):
        map_workers = []
        rdc_workers = []

        for thread_id in range(self.num_mappers):
            print(f"Main: Mapper{thread_id + 1} start:")
            p = Process(target=self.run_map, args=(thread_id,))
            p.start()
            map_workers.append(p)
        [t.join() for t in map_workers]
        for thread_id in range(self.num_reducers):
            print(f"Main: Reducer{thread_id + 1} start:")
            p = Process(target=self.run_reduce, args=(thread_id,))
            p.start()
            rdc_workers.append(p)
        [t.join() for t in rdc_workers]
        if join:
            print("Main: Start Joining")
            self.merge_outputs()

    def merge_outputs(self, clean=True, sort=True, decreasing=True):
        return self.file_handler.merge_files(self.num_reducers, clean, sort, decreasing)

    def check_pos(self, key, position):
        return position == (hash(key) % self.num_reducers)
