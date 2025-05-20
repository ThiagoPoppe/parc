"""
    Esse script irá converter os dados dumped do TheoryTab coletados via crawler para o seguinte formato json:

    -> num_beats = indicando quantas beats nós temos no trecho da música
    -> youtube = chave contendo informações do áudio do YouTube (id, que pode ser none, start e end para sincronizar [acho que é em segundos])
    -> notes = chave contendo informações de notas (sd = scale_degree, octave, onset, offset, is_rest)
    -> chords = chave contendo informações de acordes (root, onset, offset, type, inversion, applied, adds, omits, alterations, suspensions, substitutions, borrowed, is_rest)
        * borrowed pode ser uma string (major, minor, etc) ou uma lista com 7 valores indicando um "template" da escala emprestada.
    -> keys = chave contendo informações dos tons (onset, offset, scale, tonic)
    -> tempos = chave contendo informações sobre tempo (onset, offset, bpm, swing_factor, swing_beat)
    -> meters = chave contendo informações sobre fórmula de compasso (onset, offset, beats_in_measure, beat_unit)

    Então, teremos a base coletada em formato json, indexado pelo ID das músicas, em um formato padronizado!

    Observação
    ----------
        O script também oferece parâmetros opcionais para realizar o processamento do arquivo dumped para um formato mais padronizado.
        São eles:
            - min_bpm: valor mínimo de BPM para filtrar músicas lentas (default = 40)
            - max_bpm: valor máximo de BPM para filtrar músicas rápidas (default = 300)
            - allow_swing: se True, permite swing, ou seja músicas com swing_factor != 0 (default = False)
            - keep_only_4x4: se True, mantém apenas músicas com compasso 4/4 (default = True)

        * A configuração escolhida de parâmetros será salva em um arquivo readme.txt junto do arquivo .json processado.
"""

import os
import re
import h5py
import json
import logging
import traceback

from tqdm import tqdm
from bs4 import BeautifulSoup
from os.path import join as ospj
from argparse import ArgumentParser

from source.constants import AUDIOS_FILEPATH

SHARP_ACCIDENTALS_ORDER = [3, 0, 4, 1, 5, 2, 6]
FLATS_ACCIDENTALS_ORDER = [6, 2, 5, 1, 4, 0, 3]
ACCIDENTAL_TO_SCALE_MODE = {'b': 'minor', '0': 'major', '-2': 'dorian', '-4': 'phrygian', 
                            '1': 'lydian', '-1': 'mixolydian', '-3': 'minor', '-5': 'locrian'}

DUMPED_DB_FILEPATH = '<insert-path>'
PROCESSED_DB_FILEPATH = '<insert-path>'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logging.info('Loading theorytab ids with audio, this may take a while...')
with h5py.File(AUDIOS_FILEPATH, 'r') as h5f:
    theorytab_ids_with_audio = list(h5f.keys())


def extract_youtube_id(url: str):
    '''
    Method to extract a YouTube's video id based on its URL.

    Arguments
    ---------
        - url (str): url for a YouTube's video.

    Return
    ------
        - The YouTube's video ID if found, otherwise None.

    Notes
    -----
        1. First, this method tries to match the URL to a YouTube URL regex in order to return its URL.
        2. If no match was found, then the method checks if the URL is already an ID, if so return it.
    '''
    if url is None:
        return None
    
    url_regex = r'(?:https?:\/\/)?(?:www\.)?(?:youtube\.com\/(?:[^\/\n\s]+\/\S*?v=|embed\/|v\/|watch\?v=)|youtu\.be\/)([a-zA-Z0-9_-]{11})'
    
    # First try to match a YouTube URL
    url_match = re.search(url_regex, url)
    if url_match:
        return url_match.group(1) 
        
    # If no URL match, check if it's a YouTube video ID directly
    id_regex = r'^[a-zA-Z0-9_-]{11}$'
    id_match = re.match(id_regex, url)
    if id_match:
        return url
    
    return None  # Neither a URL nor an ID


def get_borrowed_scale(borrowed: str):
    '''
    Method to get a borrowed scale name or intervals.

    Arguments
    ---------
        - borrowed (str): the borrowed information stored inside a borrowed tag.

    Return
    ------
        - A string representing the borrowed scale name, e.g. mixolydian, or the scale template (list with 7 elements).
    '''
    if borrowed is None:
        return None
    
    if borrowed in ACCIDENTAL_TO_SCALE_MODE:
        return ACCIDENTAL_TO_SCALE_MODE[borrowed]
    
    # If int(borrowed) < 0, then we are adding flats (factor = -1); otherwise, sharps (factor = 1)
    borrowed = int(borrowed)
    factor = -1 if borrowed < 0 else 1
    accidentals_order = FLATS_ACCIDENTALS_ORDER if borrowed < 0 else SHARP_ACCIDENTALS_ORDER

    # Altering a major scale template by adding flats or sharps depending on the borrowed argument
    borrowed_scale = [0, 2, 4, 5, 7, 9, 11]
    for i in range(abs(borrowed)):
        idx = accidentals_order[i % 7]
        borrowed_scale[idx] += factor

    return borrowed_scale


def process_json(soup):
    processed_data = {}
    payload = json.loads(soup.find('jsonData').string)

    # Retrieving number of beats information
    num_beats = payload['keyFrames'][-1]['beat'] - 1
    processed_data['num_beats'] = num_beats

    # Retrieving youtube id information
    youtube_id = extract_youtube_id(soup.find('youTubeID').string)

    if youtube_id is None:
        youtube_id = extract_youtube_id(payload['youtube']['id'])

    processed_data['youtube'] = {
        'id': youtube_id,
        'start_sync': payload['youtube']['syncStart'],
        'end_sync': payload['youtube']['syncEnd']
    }

    notes = []
    for melody_voice in payload['inactiveNotes']:
        for note in melody_voice:
            if note.get('beat') is None or note.get('duration') is None:
                continue

            onset = max(0, note['beat'] - 1)
            if onset >= num_beats:
                continue

            offset = min(num_beats, onset + note['duration'])

            if note.get('isRest') or note['sd'] == 'rest':
                continue

            notes.append({
                'sd': note['sd'],
                'octave': note['octave'],
                'onset': onset,
                'offset': offset,
                # 'is_rest': note.get('isRest', note['sd'] == 'rest')
            })

    if len(notes) == 0:  # just to be certain in case that inactiveNotes doesn't have anything
        for note in payload['notes']:
            if note.get('beat') is None or note.get('duration') is None:
                continue

            onset = max(0, note['beat'] - 1)
            if onset >= num_beats:
                continue

            offset = min(num_beats, onset + note['duration'])

            if note.get('isRest') or note['sd'] == 'rest':
                continue

            notes.append({
                'sd': note['sd'],
                'octave': note['octave'],
                'onset': onset,
                'offset': offset,
                # 'is_rest': note.get('isRest', note['sd'] == 'rest')
            })
            
    # Retrieving chords
    chords = []
    for chord in payload['chords']:
        onset = max(0, chord['beat'] - 1)
        if onset >= num_beats:
            continue

        offset = min(num_beats, onset + chord['duration'])

        borrowed = chord['borrowed']
        if borrowed == 'super:2':
            borrowed = [1, 2, 4, 6, 7, 9, 11]

        if chord['root'] == 'rest' or chord['root'] == 0 or chord.get('isRest'):
            continue

        chords.append({
            'root': chord['root'],
            'onset': onset,
            'offset': offset,
            'type': chord['type'],
            'inversion': chord['inversion'],
            'applied': chord['applied'],
            'adds': chord['adds'],
            'omits': chord['omits'],
            'alterations': chord['alterations'],
            'suspensions': chord['suspensions'],
            'substitutions': chord.get('substitutions', []),
            'borrowed': borrowed,
            # 'is_rest': chord.get('isRest', chord['root'] == 'rest')
        })

    # Retrieving keys
    keys = []
    for i in range(len(payload['keys']) - 1):
        curr_key = payload['keys'][i]
        next_key = payload['keys'][i+1]

        keys.append({
            'onset': curr_key['beat'] - 1,
            'offset': next_key['beat'] - 1,
            'scale': curr_key['scale'],
            'tonic': curr_key['tonic']
        })

    last_key = payload['keys'][-1]
    keys.append({
        'onset': last_key['beat'] - 1,
        'offset': num_beats,
        'scale': last_key['scale'],
        'tonic': last_key['tonic']
    })

    # Correcting chords and notes that are splitted by two keys
    def split_objects(objs):
        def get_key_split(onset, offset):
            for key in keys:
                if key['offset'] > onset and key['offset'] < offset:
                    return key
                
            return None
        
        final_objs = []
        has_split = False

        for obj in objs:
            key_split = get_key_split(obj['onset'], obj['offset'])

            if key_split:
                has_split = True
                first_part = obj.copy()
                first_part['offset'] = key_split['offset']

                second_part = obj.copy()
                second_part['onset'] = key_split['offset']    
            
                final_objs.append(first_part)
                final_objs.append(second_part)
            else:
                final_objs.append(obj)

        if has_split:
            return split_objects(final_objs)
        
        return final_objs

    final_notes = split_objects(notes)
    final_chords = split_objects(chords)

    # Retrieving tempos
    tempos = []
    for i in range(len(payload['tempos']) - 1):
        curr_tempo = payload['tempos'][i]
        next_tempo = payload['tempos'][i+1]

        tempos.append({
            'onset': curr_tempo['beat'] - 1,
            'offset': next_tempo['beat'] - 1,
            'bpm': curr_tempo['bpm'],
            'swing_factor': curr_tempo['swingFactor'],
            'swing_beat': curr_tempo['swingBeat']
        })

    last_tempo = payload['tempos'][-1]
    tempos.append({
        'onset': last_tempo['beat'] - 1,
        'offset': num_beats,
        'bpm': last_tempo['bpm'],
        'swing_factor': last_tempo['swingFactor'],
        'swing_beat': last_tempo['swingBeat']
    })

    # Retrieving meters
    meters = []
    for i in range(len(payload['meters']) - 1):
        curr_meter = payload['meters'][i]
        next_meter = payload['meters'][i+1]

        meters.append({
            'onset': curr_meter['beat'] - 1,
            'offset': next_meter['beat'] - 1,
            'beats_in_measure': curr_meter['numBeats'],
            'beat_unit': curr_meter['beatUnit']
        })

    last_meter = payload['meters'][-1]
    meters.append({
        'onset': last_meter['beat'] - 1,
        'offset': num_beats,
        'beats_in_measure': last_meter['numBeats'],
        'beat_unit': last_meter['beatUnit']
    })

    processed_data['notes'] = final_notes
    processed_data['chords'] = final_chords
    processed_data['keys'] = keys
    processed_data['tempos'] = tempos
    processed_data['meters'] = meters

    return processed_data


def process_xml(soup):
    processed_data = {}
    payload = soup.find('xmlData')

    # Retrieving youtube id information
    youtube_id = extract_youtube_id(soup.find('youTubeID').string)

    if youtube_id is None:
        youtube_id = extract_youtube_id(payload.find('YouTubeID').string)

    # Grabbing relevant overall meta information
    meta = payload.find('meta')
    key = meta.find('key').string
    mode_names = ['major', 'dorian', 'phrygian', 'lydian', 'mixolydian', 'minor', 'locrian']
    mode = 'major' if meta.mode is None else mode_names[int(meta.mode.string) - 1]

    bpm = None if meta.BPM is None else int(meta.BPM.string)
    beats_in_measure = int(meta.find('beats_in_measure').string)

    if payload.find('sections'):  # checking if filter by section is required
        num_sections = len(payload.find('sections').findChildren(recursive=False))

        if num_sections > 1:
            section_name = soup.find('section').string
            section_info = payload.find_all(section_name)

            if len(section_info) == 0:
                logging.warning(f'Section {section_name} not found for xmlData.')
                return None

            meta, payload = section_info

    # Retrieving number of beats information
    num_beats_per_segment = []

    for segment in payload.find_all('segment'):
        if segment.find('numBeats'):
            num_beats_per_segment.append(int(segment.find('numBeats').string))
        elif segment.find('numMeasures'):
            num_beats_per_segment.append(beats_in_measure * int(segment.find('numMeasures').string))
        else:
            logging.error("Couldn't find neither numBeats nor numMeasures!")
            return None

    num_beats = sum(num_beats_per_segment)
    processed_data['num_beats'] = num_beats

    # Retrieving youtube information
    global_start = float(meta.find('global_start').string)
    active_start = float(meta.find('active_start').string)
    active_stop = float(meta.find('active_stop').string)

    processed_data['youtube'] = {
        'id': youtube_id,
        'start_sync': global_start + active_start,
        'end_sync': global_start + active_stop
    }

    # Retrieving notes
    notes = []
    global_onset = 0.0
    for segment_idx, segment in enumerate(payload.find_all('segment')):
        for note in segment.find_all('note'):
            start_measure = int(note.start_measure.string)
            start_beat = float(note.start_beat.string)

            onset = global_onset + beats_in_measure * (start_measure - 1) + (start_beat - 1)
            offset = onset + float(note.note_length.string)

            is_rest = False
            if note.isRest is not None and note.isRest.string == '1':
                is_rest = True
            elif note.isRest is None and note.scale_degree.string == 'rest':
                is_rest = True

            if is_rest:
                continue

            notes.append({
                'sd': note.scale_degree.string,
                'octave': int(note.octave.string),
                'onset': onset,
                'offset': offset,
                # 'is_rest': is_rest
            })

        global_onset += num_beats_per_segment[segment_idx]

    # Retrieving chords
    chord_info_mapper = {
        'type': {
            '7': 7,
            '9': 9,
            '11': 11
        },
        'inversion': {
            '6': 1,
            '64': 2,
            '65': 1,
            '43': 2,
            '42': 3
        },
        'sus': {
            'sus2': [2],
            'sus4': [4],
            'sus42': [2, 4]
        },
        'adds': {
            'add9': [9]
        },
        'alterations': {
            '#5': ['#5'],
            'b5': ['b5']
        }
    }

    chords = []
    global_onset = 0.0
    for segment_idx, segment in enumerate(payload.find_all('segment')):
        for chord in segment.find_all('chord'):
            root = chord.sd.string
            applied = chord.sec.string

            if applied:  # to be consistent with the JSON data
                root, applied = applied, root

            start_measure = int(chord.start_measure.string)
            start_beat = float(chord.start_beat.string)

            onset = global_onset + beats_in_measure * (start_measure - 1) + (start_beat - 1)
            offset = onset + float(chord.chord_duration.string)

            adds = []
            if chord.emb is not None:
                adds = chord_info_mapper['adds'].get(chord.emb.string, [])

            alterations = []
            if chord.emb is not None:
                alterations = chord_info_mapper['alterations'].get(chord.emb.string, [])

            is_rest = False
            if chord.isRest is not None and chord.isRest.string == '1':
                is_rest = True
            elif chord.isRest is None and chord.sd.string == 'rest':
                is_rest = True
            elif root == 'rest' or (root.isdigit() and int(root) == 0):
                is_rest = True

            if is_rest:
                continue
            
            chords.append({
                'root': int(root),
                'onset': onset,
                'offset': offset,
                'type': chord_info_mapper['type'].get(chord.fb.string, 7 if chord.fb.string == '42' else 5),
                'inversion': chord_info_mapper['inversion'].get(chord.fb.string, 0),
                'applied': int(applied) if applied is not None else 0,
                'adds': adds,
                'omits': [],
                'alterations': alterations,
                'suspensions': chord_info_mapper['sus'].get(chord.sus.string, []),
                'substitutions': [],
                'borrowed': get_borrowed_scale(chord.borrowed.string),
                # 'is_rest': is_rest
            })

        global_onset += num_beats_per_segment[segment_idx]

    # Retrieving keys
    keys = [{
        'onset': 0,
        'offset': num_beats,
        'scale': mode,
        'tonic': key
    }]

    # Retrieving tempos
    tempos = [{
        'onset': 0,
        'offset': num_beats,
        'bpm': bpm,
        'swing_factor': 0,
        'swing_beat': 0.5
    }]
    
    # Retrieving meters
    meters = [{
        'onset': 0,
        'offset': num_beats,
        'beats_in_measure': beats_in_measure,
        'beat_unit': 1
    }]

    processed_data['notes'] = notes
    processed_data['chords'] = chords
    processed_data['keys'] = keys
    processed_data['tempos'] = tempos
    processed_data['meters'] = meters

    return processed_data


def retrieve_theorytab_tags(theorytab_id, theorytab):
    tags = []

    if theorytab_id in theorytab_ids_with_audio:
        tags.append('HAS_AUDIO')

    if len(theorytab['chords']) != 0:
        tags.append('HAS_HARMONY')

    if len(theorytab['notes']) != 0:
        tags.append('HAS_MELODY')

    if len(theorytab['keys']) > 1:
        tags.append('HAS_KEY_CHANGE')
    
    if len(theorytab['meters']) > 1:
        tags.append('HAS_METER_CHANGE')
    
    if len(theorytab['tempos']) > 1:
        tags.append('HAS_TEMPO_CHANGE')

    has_swing_tempo = any([tempo['swing_factor'] != 0 for tempo in theorytab['tempos']])
    if has_swing_tempo:
        tags.append('HAS_SWING_TEMPO')

    is_all_4x4 = all([meter['beats_in_measure'] == 4 and meter['beat_unit'] == 1 for meter in theorytab['meters']])
    if is_all_4x4:
        tags.append('ONLY_COMMON_TIME')

    is_majmin_theorytab = True
    for key in theorytab['keys']:
        if key['scale'] not in ('major', 'minor'):
            is_majmin_theorytab = False
            break
    
    if is_majmin_theorytab:
        tags.append('ONLY_MAJMIN_KEYS')

    return tags


def main(args):
    with open(DUMPED_DB_FILEPATH, 'r') as fp:
        dumped_database = json.load(fp)
    
    processed_dataset = {}
    for entry in tqdm(dumped_database):
        try:
            data = list(entry.values())[0]
            theorytab_id = list(entry.keys())[0]
            soup = BeautifulSoup(data['payload'], 'xml')

            processed_data = None
            if soup.find('xmlData') and len(soup.find('xmlData').contents) > 0:
                xml_soup = BeautifulSoup(data['payload'].replace('&lt;', '<').replace('&gt;', '>'), 'xml')
                processed_data = process_xml(xml_soup)

            elif soup.find('jsonData') and len(soup.find('jsonData').contents) > 0:
                processed_data = process_json(soup)

            if processed_data is None:
                logging.error(f'Skipping {theorytab_id} due to insufficient data!')
                continue

            has_malformed_root = False
            for chord in processed_data['chords']:
                if isinstance(chord['root'], str) or (chord['root'] < 1 or chord['root'] > 7):
                    logging.warning(f"Skipping {theorytab_id} due to malformed {chord['root'] = }")
                    has_malformed_root = True
                    break

            has_unwanted_tempo = False
            for tempo in processed_data['tempos']:
                if tempo['bpm'] is None:
                    logging.warning(f"Skipping {theorytab_id} due to None BPM")
                    has_unwanted_tempo = True
                    break

                if tempo['bpm'] < args.min_bpm or tempo['bpm'] > args.max_bpm:
                    logging.warning(f"Skipping {theorytab_id} due to out of range {tempo['bpm'] = } with {args.min_bpm = } and {args.max_bpm = }")
                    has_unwanted_tempo = True
                    break

            if has_malformed_root or has_unwanted_tempo:
                continue

            song_url = soup.find('songURL').string
            artist_url = soup.find('artistURL').string
            section_url = soup.find('sectionURL').string

            processed_data['hooktheory'] = {
                'genres': data['genres'],
                'annotators': data['contributors'],
                'song_metrics': data['song_metrics'],
                'artist': soup.find('artist').string,
                'song': soup.find('song').string,
                'section': soup.find('section').string,
                'modified_date': soup.find('dateModified').string,
                'hooktheory_api': f'https://api.hooktheory.com/v1/songs/public/{theorytab_id}',
                'theorytab_url': f'https://www.hooktheory.com/theorytab/view/{artist_url}/{song_url}#{section_url}'
            }

            processed_data['tags'] = retrieve_theorytab_tags(theorytab_id, processed_data)
            processed_dataset[theorytab_id] = processed_data

        except Exception as e:
            print(traceback.format_exc())
            logging.error(f'Exception for {theorytab_id = }')
            exit(1)

    logging.info(f'Size of processed dataset: {len(processed_dataset)} theorytab ids')
    with open(PROCESSED_DB_FILEPATH, 'w') as fp:
        json.dump(processed_dataset, fp, indent=2)


def parse_command_line_arguments():
    parser = ArgumentParser()
    parser.add_argument('--min_bpm', type=int, default=40, help='Minimum BPM value to filter slow songs.')
    parser.add_argument('--max_bpm', type=int, default=300, help='Maximum BPM value to filter fast songs.')

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_command_line_arguments()

    logging.info('Parameters used:')
    logging.info(f'  - Min BPM: {args.min_bpm}')
    logging.info(f'  - Max BPM: {args.max_bpm}')

    with open(ospj(os.path.dirname(PROCESSED_DB_FILEPATH), 'processed_readme.txt'), 'w') as fp:
        fp.write('Parameters used:\n')
        fp.write(f'  - Min BPM: {args.min_bpm}\n')
        fp.write(f'  - Max BPM: {args.max_bpm}\n')

    main(args)
