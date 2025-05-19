import re
import numpy as np

from ..utils import get_note_pc, get_note_name
from ..constants import DEGREE_MAP, MODE_INTERVALS, ACCIDENTAL_MAP, QUALITY_INTERVALS


def get_scale_pcs(tonic_pc, mode):
    return np.cumsum([tonic_pc] + MODE_INTERVALS[mode]) % 12


def parse_rn(rn):
    pattern = r'^([#b]?)([IViv]{1,3})(.*)$'
    match = re.match(pattern, rn)

    if not match:
        raise ValueError(f'No match for {rn = }')
    
    accidental, degree, extension = match.groups()
    return accidental, degree, extension


def get_chord_quality(degree, extension):
    major_map = {'7': 'D7', 'maj7': 'M7', '+': 'a', '+7': 'a7', '+maj7': 'aM7', '': 'M'}
    minor_map = {'o': 'd', 'o7': 'd7', '^o7': 'h7', '7': 'm7', 'maj7': 'mM7', 'omaj7': 'oM7', '': 'm'}

    return major_map[extension] if degree.isupper() else minor_map[extension]


def get_rn_pitch_classes(rn: str, scale: str) -> str:
    """ Example usage:
            get_rn_pitch_classes('I', 'C major') --> '0-4-7'
    """

    tonic, mode = scale.split()
    key_scale = get_scale_pcs(get_note_pc(tonic), mode)

    rn = rn.replace('/o', '^o')  # so that / is only used for secondary chords
    parts = rn.split('/')

    first = parts[0]
    second = parts[1] if len(parts) == 2 else None

    if second:
        accidental, degree, _ = parse_rn(second)
        key_tonic_pc = key_scale[DEGREE_MAP[degree.upper()]] + ACCIDENTAL_MAP[accidental]
        key_scale = get_scale_pcs(key_tonic_pc, 'major')

    accidental, degree, extension = parse_rn(first)

    root_pc = key_scale[DEGREE_MAP[degree.upper()]] + ACCIDENTAL_MAP[accidental]
    intervals = QUALITY_INTERVALS[get_chord_quality(degree, extension)]

    pitch_classes = np.cumsum([root_pc] + intervals) % 12
    return '-'.join(pitch_classes.astype(str))


if __name__ == '__main__':
    rn = 'III+7'
    scale = 'E harmonicMinor'

    pitch_classes = get_rn_pitch_classes(rn, scale)
    print(f"{rn} -> {pitch_classes} {[get_note_name(int(pc)) for pc in pitch_classes.split('-')]}")
