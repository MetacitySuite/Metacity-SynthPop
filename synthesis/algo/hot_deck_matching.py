import itertools, functools
import multiprocessing as mp
from tqdm import tqdm
from re import search
import numpy as np

'''
Random hot-deck imputation

A statistical matching method according to the Sao Paulo synthetic population.
https://github.com/eqasim-org/sao_paulo

'''

class HotDeckMatcher:
    def __init__(self, df_source, source_id_column, mandatory_attributes, preferred_attributes, default_id, matching_minimum_samples):
        self.mandatory_attributes = mandatory_attributes
        self.preferred_attributes = preferred_attributes
        self.all_attributes = self.mandatory_attributes + self.preferred_attributes
        self.minimum_samples = matching_minimum_samples
        self.source_ids = df_source[source_id_column]
        self.default_id = default_id

        #get list of unique values for each attribute
        self.unique_values = {
            attrib : list(df_source[attrib].unique()) for attrib in self.all_attributes
        }

        self.attribute_values_count = [len(self.unique_values[col]) for col in self.all_attributes]
        self.attribute_indices = np.cumsum(self.attribute_values_count)

        for attrib in self.all_attributes:
            print("Found categories for %s:" % attrib, ", ".join([str(c) for c in self.unique_values[attrib]]))

        #define search order for each column with unique value
        search_order = [
            [list(np.arange(size) == k) for k in range(size)] +
            ([] if attrib in self.mandatory_attributes else [[False] * size])
            for attrib, size in zip(self.all_attributes, self.attribute_values_count)
        ]

        #cartesian product of column search order
        self.attribute_masks = list(itertools.product(*search_order)) 

        #source filter mask
        self.source_matrix = self.make_matrix(df_source, source = True)

    def __call__(self, df_target, chunk_index = 0):
        target_matrix = self.make_matrix(df_target)
        matched_mask = np.zeros(len(df_target), dtype=bool)
        matched_indices = np.ones(len(df_target), dtype=int) * (-1)

        # Note: This speeds things up quite a bit. We generate a random number
        # for each person which is later on used for the sampling.
        random = np.array([
            np.random.random() for _ in tqdm(range(len(df_target)), desc = "Generating random numbers", position = chunk_index)
        ])

        with tqdm(total = len(self.attribute_masks), position = chunk_index, desc = "Hot Deck Matching") as progress:
            for attrib_mask in self.attribute_masks:
                attrib_mask = np.array(functools.reduce(lambda x, y: x + y, attrib_mask), dtype = np.bool)
                source_mask = np.all(self.source_matrix[:, attrib_mask], axis = 1) #the whole row has "True" for columns specified in attrib_mask

                if np.any(source_mask) and np.count_nonzero(source_mask) >= self.minimum_samples: # match these with target
                    target_mask = np.all(target_matrix[:, attrib_mask], axis = 1) 

                    if np.any(target_mask):
                        source_indices = np.where(source_mask)[0] #array of indices where source_mask is True
                        random_indices = np.floor(random[target_mask] * len(source_indices)).astype(np.int) #scaled random indices to the length of source_indices
                        matched_indices[np.where(~matched_mask)[0][target_mask]] = source_indices[random_indices] #assign a random index from the matched source samples

                        # We continuously shrink these matrix to make the matching
                        # easier and easier as the HDM proceeds
                        random = random[~target_mask]
                        target_matrix = target_matrix[~target_mask] # keep not yet matched
                        matched_mask[~matched_mask] |= target_mask

                progress.update()

        matched_ids = np.zeros(len(df_target), dtype = self.source_ids.dtype)
        matched_ids[matched_mask] = self.source_ids.iloc[matched_indices[matched_mask]]
        matched_ids[~matched_mask] = self.default_id

        return matched_ids

    '''
    Expands dataframe into a matrix with type of columns with their unique value, mark T/F for if the value matches.
    '''
    def make_matrix(self, df, chunk_index = None, source = False):
        columns = sum(self.attribute_values_count)

        matrix = np.zeros((len(df), columns), dtype = np.bool)
        column_index = 0

        with tqdm(total = columns, desc = "Reading categories (%s) ..." % ("source" if source else "target"), position = chunk_index) as progress:
            for attribute in self.all_attributes:
                for value in self.unique_values[attribute]:
                    matrix[:, column_index] = df[attribute] == value
                    column_index += 1
                    progress.update()
        return matrix

def run(df_target, df_source, source_id_column, mandatory_fields, preferred_fields, default_id = -1, minimum_source_samples = 1, process_num = 1):
    matcher = HotDeckMatcher(df_source, source_id_column, mandatory_fields, preferred_fields, default_id, minimum_source_samples)
    if process_num > 1:
        
        with mp.Pool(processes = process_num, initializer = initializer, initargs = (matcher,)) as pool:
            chunks = np.array_split(df_target, process_num)
            df_target.loc[:, "hdm_source_id"] = np.hstack(pool.map(running_process, enumerate(chunks)))
    else:
        df_target.loc[:, "hdm_source_id"] = matcher(df_target, 0)

matcher = None

def initializer(_matcher):
    global matcher
    matcher = _matcher

def running_process(args):
    index, df_chunk = args
    return matcher(df_chunk, index)