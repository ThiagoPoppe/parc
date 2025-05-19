import sys
sys.path.append('../..')

import os
import json
import time
import h5py
import random
import yt_dlp
import librosa
import logging
import requests

from stem import Signal
from stem.control import Controller

from source.constants import AUDIOS_FILEPATH, SAMPLING_RATE

SLEEP_RANGE = (2.5, 5.5)  # in seconds
BIG_SLEEP_TIME = 900  # in seconds
BIG_SLEEP_INTERVAL = 300
RENEW_TOR_INTERVAL = 75

logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def renew_tor_connection():
    with Controller.from_port(port=9151) as controller:
        controller.authenticate()
        controller.signal(Signal.NEWNYM)


def download_audio(youtube_id: str) -> bool:
    ydl_opts = {
        'format': 'bestaudio/best',
        'outtmpl': youtube_id,
        'postprocessors': [
            {
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'wav',
            }
        ],
        'quiet': True,
        'proxy': 'socks5://localhost:9150'
    }

    try:
        time.sleep(random.uniform(*SLEEP_RANGE))
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download(f'https://www.youtube.com/watch?v={youtube_id}')
    
    except requests.Timeout as err:
        logging.info('Renewing Tor connection')
        renew_tor_connection()
        
        logging.warning('Sleeping for {BIG_SLEEP_TIME} seconds and retrying later')
        time.sleep(BIG_SLEEP_TIME)
        download_audio(youtube_id)

    except Exception as err:
        if 'Sign in to confirm' in str(err) and 'bot' in str(err):
            logging.info('Renewing Tor connection')
            renew_tor_connection()
            time.sleep(random.uniform(*SLEEP_RANGE))
            return download_audio(youtube_id)
        
        elif 'Failed to extract any player response' in str(err):
            logging.error('Shutting down to connection problem with Tor')
            sys.exit(1)

        return False, err
    
    return True, None


def process_youtube_id(youtube_id, info):
    success_status, err = download_audio(youtube_id)
    if not success_status:
        return [], err
    
    filename = f'{youtube_id}.wav'
    audio, sr = librosa.load(filename, sr=SAMPLING_RATE)
    duration = librosa.get_duration(y=audio, sr=sr)

    audios = []
    for alignment in info['alignments']:
        start_time = alignment['start_sync'] * duration if 0 <= alignment['start_sync'] <= 1 else alignment['start_sync']
        end_time = alignment['end_sync'] * duration if 0 <= alignment['end_sync'] <= 1 else alignment['end_sync']

        start_sample = librosa.time_to_samples(start_time, sr=sr)
        end_sample = librosa.time_to_samples(end_time, sr=sr)
        
        audio_segment = audio[start_sample:end_sample]
        audios.append((alignment['theorytab_id'], audio_segment))

    os.remove(f'{youtube_id}.wav')
        
    return audios, None


def main():
    if not os.path.exists('youtube_info.json'):
        logging.error('Please run the script to create youtube_info.json first')
        sys.exit(1)

    with open('youtube_info.json', 'r') as fp:
        youtube_info = json.load(fp)
        youtube_info = dict(sorted(youtube_info.items(), key=lambda p: (p[1]['finished'], len(str(p[1]['error_message']))))[::-1])

    with h5py.File(AUDIOS_FILEPATH, 'r') as h5f:
        existent_theorytab_ids = set(h5f.keys())

    for idx, (youtube_id, info) in enumerate(youtube_info.items()):
        if info['finished'] and all([alignment['theorytab_id'] in existent_theorytab_ids for alignment in info['alignments']]):
            logging.info(f'[{idx+1}/{len(youtube_info)}] {youtube_id} already finished')
            continue

        elif info['error_message']:
            logging.info(f'[{idx+1}/{len(youtube_info)}] {youtube_id} failed previously with the following error\n{info["error_message"]}')
            continue

        audios, err = process_youtube_id(youtube_id, info)
        
        if err:
            logging.warning(f'[{idx+1}/{len(youtube_info)}] {youtube_id} failed with the following error\n{err}')
            youtube_info[youtube_id]['error_message'] = str(err)
        else:
            logging.info(f'[{idx+1}/{len(youtube_info)}] {youtube_id} successfully downloaded')

            with h5py.File(AUDIOS_FILEPATH, 'a') as h5f:
                for theorytab_id, audio in audios:
                    if theorytab_id not in existent_theorytab_ids:
                        h5f.create_dataset(theorytab_id, data=audio, compression='gzip')
                        
            youtube_info[youtube_id]['finished'] = True
            with open('youtube_info.json', 'w') as fp:
                json.dump(youtube_info, fp, indent=2)

        if (idx + 1) % BIG_SLEEP_INTERVAL == 0:
            logging.info('Renewing Tor connection')
            renew_tor_connection()

            logging.info(f'Big sleep after seen {idx + 1} YouTube IDs...')
            time.sleep(BIG_SLEEP_TIME)

        elif (idx + 1) % RENEW_TOR_INTERVAL == 0:
            logging.info('Renewing Tor connection')
            renew_tor_connection()
            
    logging.info('Job finished!')


if __name__ == '__main__':
    main()
