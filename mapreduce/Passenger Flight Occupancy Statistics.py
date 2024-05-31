from Mapreduce import MapReduceProcess
default_n_mappers = 5
default_n_reducers = 5
if __name__ == '__main__':
    print("MapReduce start...")
    input_dir, output_dir, n_mappers, n_reducers = None, None, default_n_mappers, default_n_reducers
    word_count = MapReduceProcess(input_folder=input_dir, output_folder=output_dir, num_mappers=n_mappers,
                                  num_reducers=n_reducers)
    word_count.run()
    print("Results stored")
