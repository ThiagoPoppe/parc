import sys
sys.path.append('../..')

import h5py
import logging

from tqdm import tqdm
from source.constants import AUDIOS_FILEPATH

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


with h5py.File(AUDIOS_FILEPATH, 'a') as h5f:
    for theorytab_id in tqdm(h5f, desc='Cleaning empty audios'):
        if len(h5f[theorytab_id][:]) == 0:
            logging.warning(f"Deleting theorytab {theorytab_id} with empty audio")
            del h5f[theorytab_id]
