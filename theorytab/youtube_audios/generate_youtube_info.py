import sys
sys.path.append('../..')

import h5py
import json

from collections import defaultdict
from source.utils import has_valid_tags
from source.constants import AUDIOS_FILEPATH, THEORYTAB_DATASET_FILEPATH


def build_youtube_info(theorytab_dataset, finished_youtube_ids = None):
    youtube_info = defaultdict(lambda: defaultdict(list))
    if finished_youtube_ids is None:
        finished_youtube_ids = set()    

    for theorytab_id, theorytab in theorytab_dataset.items():
        if not has_valid_tags(theorytab):
            continue

        youtube_id = theorytab['youtube']['id']
        start_sync = theorytab['youtube']['start_sync']
        end_sync = theorytab['youtube']['end_sync']
        
        if youtube_id is None or start_sync is None or end_sync is None:
            print(f'Invalid YouTube ID or sync times for theorytab {theorytab_id}: {youtube_id}, {start_sync}, {end_sync}')
            continue
        
        youtube_info[youtube_id]['error_message'] = None 
        youtube_info[youtube_id]['finished'] = youtube_id in finished_youtube_ids
        youtube_info[youtube_id]['alignments'].append(
            {
                'theorytab_id': theorytab_id,
                'start_sync': start_sync,
                'end_sync': end_sync
            }
        )

    return youtube_info


if __name__ == '__main__':
    print('Loading YouTube info from:', THEORYTAB_DATASET_FILEPATH)
    with open(THEORYTAB_DATASET_FILEPATH, 'r') as fp:
        dataset = json.load(fp)

    print('Loading finished YouTube IDs from:', AUDIOS_FILEPATH, end='\n\n')
    with h5py.File(AUDIOS_FILEPATH, 'r') as h5f:
        finished_theorytab_ids = set(h5f.keys())
        finished_youtube_ids = {theorytab['youtube']['id'] for theorytab_id, theorytab in dataset.items() if theorytab_id in finished_theorytab_ids}
    
    youtube_info = build_youtube_info(dataset, finished_youtube_ids)
    print('\nTotal number of valid YouTube IDs:', len(youtube_info))
    
    with open('youtube_info.json', 'w') as fp:
        json.dump(youtube_info, fp, indent=2)
