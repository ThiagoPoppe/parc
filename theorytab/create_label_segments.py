import sys
sys.path.append('..')

import json
import h5py
import logging

from tqdm import tqdm
from skimage.util import view_as_windows

from source.utils import has_valid_tags, encode_labels
from source.constants import (
    ALL_TASKS,
    STEP_SIZE,
    WINDOW_SIZE,
    TASK_DOMAINS,
    THEORYTAB_DATASET_FILEPATH
)

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def main():
    with open(THEORYTAB_DATASET_FILEPATH, 'r') as fp:
        theorytab_dataset = json.load(fp)

    theorytab_ids_to_keep = set()
    complete_rns = set(TASK_DOMAINS['complete_rn'])

    for theorytab_id, theorytab in theorytab_dataset.items():
        if not has_valid_tags(theorytab):
            continue

        keep_theorytab = True
        for chord in theorytab['chords']:
            if chord['complete_rn'] not in complete_rns:
                keep_theorytab = False
                break
            
        if keep_theorytab:
            theorytab_ids_to_keep.add(theorytab_id)

    theorytab_dataset = {
        theorytab_id: theorytab
        for theorytab_id, theorytab in theorytab_dataset.items() if theorytab_id in theorytab_ids_to_keep
    }

    with h5py.File('/storage/datasets/thiago.poppe/TheoryTabDB/segments/labels.h5', 'w') as h5f:
        for theorytab_id, theorytab in tqdm(theorytab_dataset.items()):
            labels = encode_labels(theorytab)
            labels_segments = view_as_windows(labels, (len(ALL_TASKS), WINDOW_SIZE), (len(ALL_TASKS), STEP_SIZE)).squeeze(0)

            for segment_idx, segment in enumerate(labels_segments):
                h5f.create_dataset(f'{theorytab_id}/{segment_idx}', data=segment, compression='gzip')


if __name__ == "__main__":
    main()