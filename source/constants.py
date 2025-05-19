import json

STEP_SIZE = 32
WINDOW_SIZE = 256

SAMPLING_RATE = 44100
VAMP_FEATURE_STEP = 2048 / SAMPLING_RATE

LABEL_PADDING_VALUE = -1
FEATURE_PADDING_VALUE = 0

CHROMATIC_SCALE = 'C C# D D# E F F# G G# A A# B'.split()
COMPLEXITIES = ['Beginner', 'Intermediate', 'Advanced I', 'Advanced II']

ALL_TASKS = [
    'local_key', 'secondary_degree', 'primary_degree',
    'quality', 'inversion', 'root_pitch_class', 'bass_pitch_class',
    'tonicized_pitch_class', 'simple_rn'
]

REDUCED_TASKS = [
    'local_key', 'simple_rn', 'inversion',
    'root_pitch_class', 'bass_pitch_class', 'tonicized_pitch_class'
]

AUDIOS_FILEPATH = '/storage/datasets/thiago.poppe/TheoryTabDB/audios.h5'
LABELS_FILEPATH = '/storage/datasets/thiago.poppe/TheoryTabDB/segments/labels.h5'
VAMP_FEATURES_FILEPATH = '/storage/datasets/thiago.poppe/TheoryTabDB/segments/vamp_features.h5'
THEORYTAB_DATASET_FILEPATH = '/storage/datasets/thiago.poppe/TheoryTabDB/theorytab_dataset.json'
STRATIFICATION_INFO_FILEPATH = '/storage/datasets/thiago.poppe/TheoryTabDB/stratification_info.json'

MODE_INTERVALS = {
    'major': [2, 2, 1, 2, 2, 2],
    'minor': [2, 1, 2, 2, 1, 2],
    'dorian': [2, 1, 2, 2, 2, 1],
    'phrygian': [1, 2, 2, 2, 1, 2],
    'lydian': [2, 2, 2, 1, 2, 2],
    'mixolydian': [2, 2, 1, 2, 2, 1],
    'locrian': [1, 2, 2, 1, 2, 2],
    'harmonicMinor': [2, 1, 2, 2, 1, 3],
    'phrygianDominant': [1, 3, 1, 2, 1, 2]
}

QUALITY_INTERVALS = {
    'D7': [4, 3, 3], 'M': [4, 3], 'M7': [4, 3, 4], 'a': [4, 4],
    'a7': [4, 4, 2], 'aM7': [4, 4, 3], 'd': [3, 3], 'd7': [3, 3, 3],
    'h7': [3, 3, 4], 'm': [3, 4], 'm7': [3, 4, 3], 'mM7': [3, 4, 4],
    'oM7': [3, 3, 5]
}

ACCIDENTAL_MAP = {'bb': -2, 'b': -1, '': 0, '#': 1, '##': 2}
DEGREE_MAP = {'I': 0, 'II': 1, 'III': 2, 'IV': 3, 'V': 4, 'VI': 5, 'VII': 6}

with open('/storage/datasets/thiago.poppe/TheoryTabDB/tasks_metadata/task_sizes.json', 'r') as fp:
    TASK_SIZES = json.load(fp)

with open('/storage/datasets/thiago.poppe/TheoryTabDB/tasks_metadata/task_domains.json', 'r') as fp:
    TASK_DOMAINS = json.load(fp)