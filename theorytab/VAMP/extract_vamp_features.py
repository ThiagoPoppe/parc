import sys
sys.path.append('../..')

import os
import json
import vamp
import h5py
import librosa
import logging
import numpy as np

from typing import Tuple
from multiprocessing.pool import Pool
from scipy.interpolate import interp1d
from skimage.util import view_as_windows

from source.utils import has_valid_tags
from source.constants import (
    STEP_SIZE,
    WINDOW_SIZE,
    SAMPLING_RATE,
    VAMP_FEATURE_STEP,
    AUDIOS_FILEPATH,
    VAMP_FEATURES_FILEPATH,
    THEORYTAB_DATASET_FILEPATH
)

NUM_WORKERS = 8
BATCH_SIZE = 100

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def minmax(x, axis=0):
    return (x - x.min(axis=axis)) / (x.max(axis=axis) - x.min(axis=axis) + 1e-8)


def standardize(x, axis=0):
    return (x - x.mean(axis=axis)) / (x.std(axis=axis) + 1e-8)


def get_beats_to_frames(num_beats: int, num_frames: int):
    beat_to_time_fn = interp1d((0, num_beats), (0, VAMP_FEATURE_STEP * num_frames), kind='linear', fill_value='extrapolate')
    time_to_frame_fn = lambda time: librosa.time_to_frames(time, sr=SAMPLING_RATE, hop_length=2048)
    
    beats_to_frames = time_to_frame_fn(beat_to_time_fn(np.arange(0, num_beats + 1e-4)))
    return beats_to_frames


def resample_feature(feature, beats_to_frames, normalization = None):
    resampled_frames = []
    num_beats = len(beats_to_frames) - 1
    
    for i in range(num_beats):
        if beats_to_frames[i] == beats_to_frames[i+1]:
            frame_idx = min(beats_to_frames[i], feature.shape[1] - 1)
            frame = feature[:, frame_idx]
        else:
            frame = np.mean(feature[:, beats_to_frames[i]:beats_to_frames[i+1]], axis=1)
            
        resampled_frames.append(frame)
        
    resampled_feature = np.stack(resampled_frames, axis=1)
    assert resampled_feature.shape[1] == num_beats
    
    if normalization is not None:
        resampled_feature = normalization(resampled_feature)
    
    return resampled_feature


def compute_feature(audio: np.ndarray, num_beats: int, name: str, chromanormalize: int = 1, **kwargs) -> np.ndarray:
    _, feature = vamp.collect(
        audio,
        sample_rate=SAMPLING_RATE,
        plugin_key='nnls-chroma:nnls-chroma',
        output=name,
        parameters={'chromanormalize': chromanormalize}
    )['matrix']

    # For some reason, VAMP is returning the features 3 semitones higher
    feature = feature.T
    feature = np.roll(feature, shift=-3, axis=0)

    beats_to_frames = get_beats_to_frames(num_beats, feature.shape[1])
    resampled_feature = resample_feature(feature, beats_to_frames, **kwargs)

    return resampled_feature


def get_vamp_features(audio: np.ndarray, num_beats: int) -> Tuple:
    chroma = compute_feature(audio, num_beats, name='chroma', normalization=minmax)
    basschroma = compute_feature(audio, num_beats, name='basschroma', normalization=minmax)
    spectrum = compute_feature(audio, num_beats, name='semitonespectrum', chromanormalize=0, normalization=standardize)

    return chroma, basschroma, spectrum


def chunkify_feature(feature: np.ndarray, feature_size: int):
    if feature.shape[1] < WINDOW_SIZE:
        feature = np.pad(feature, ((0, 0), (0, WINDOW_SIZE - feature.shape[1])))

    windows = view_as_windows(feature, (feature_size, WINDOW_SIZE), (feature_size, STEP_SIZE))
    windows = windows.squeeze(0)

    return windows


def main():
    with open(THEORYTAB_DATASET_FILEPATH, 'r') as fp:
        theorytab_dataset = json.load(fp)
        valid_theorytab_ids = [theorytab_id for theorytab_id, theorytab in theorytab_dataset.items() if has_valid_tags(theorytab['tags'])]

    if not os.path.exists(VAMP_FEATURES_FILEPATH):
        with h5py.File(VAMP_FEATURES_FILEPATH, 'w'):
            pass
    
    with h5py.File(VAMP_FEATURES_FILEPATH, 'r') as vamp_h5f:
        processed_theorytab_ids = set(vamp_h5f)

    with h5py.File(AUDIOS_FILEPATH, 'r') as audios_h5f:
        pending_theorytab_ids = [theorytab_id for theorytab_id in valid_theorytab_ids if theorytab_id not in processed_theorytab_ids]
        logging.info(f'Pending theorytab ids: {len(pending_theorytab_ids)}/{len(valid_theorytab_ids)}')

        counter = 0
        with h5py.File(VAMP_FEATURES_FILEPATH, 'a') as vamp_h5f:
            for i in range(0, len(pending_theorytab_ids), BATCH_SIZE):
                try:
                    batch_theorytab_ids = pending_theorytab_ids[i:i+BATCH_SIZE]
                    batch_audios = [audios_h5f[theorytab_id][:] for theorytab_id in batch_theorytab_ids]
                    batch_num_beats = [theorytab_dataset[theorytab_id]['num_beats'] for theorytab_id in batch_theorytab_ids]

                    valid_batch_audios = []
                    valid_batch_theorytab_ids = []
                    valid_batch_num_beats = []

                    for theorytab_id, audio, num_beats in zip(batch_theorytab_ids, batch_audios, batch_num_beats):
                        if librosa.get_duration(y=audio, sr=SAMPLING_RATE) == 0:
                            logging.warning(f'Skipping {theorytab_id = } due to empty audio')
                            continue

                        valid_batch_audios.append(audio)
                        valid_batch_theorytab_ids.append(theorytab_id)
                        valid_batch_num_beats.append(num_beats)

                    with Pool(NUM_WORKERS) as pool:
                        results = pool.starmap(get_vamp_features, zip(valid_batch_audios, valid_batch_num_beats))

                    for theorytab_id, (chroma, basschroma, spectrum) in zip(valid_batch_theorytab_ids, results):
                        chroma_chunks = chunkify_feature(chroma, feature_size=12)
                        basschroma_chunks = chunkify_feature(basschroma, feature_size=12)
                        spectrum_chunks = chunkify_feature(spectrum, feature_size=84)

                        assert len(chroma_chunks) == len(basschroma_chunks)
                        assert len(chroma_chunks) == len(spectrum_chunks)

                        for idx in range(len(chroma_chunks)):
                            vamp_h5f.create_dataset(f'{theorytab_id}/{idx}/chroma', data=chroma_chunks[idx], compression='gzip')
                            vamp_h5f.create_dataset(f'{theorytab_id}/{idx}/basschroma', data=basschroma_chunks[idx], compression='gzip')
                            vamp_h5f.create_dataset(f'{theorytab_id}/{idx}/spectrum', data=spectrum_chunks[idx], compression='gzip')
                except Exception as err:
                    logging.error(f'Exception for batch theorytab ids: {valid_batch_theorytab_ids}: {err}')
                    sys.exit(1)

                counter += len(batch_theorytab_ids)
                logging.info(f'Processed {counter}/{len(pending_theorytab_ids)} theorytab ids')


if __name__ == '__main__':
    main()
