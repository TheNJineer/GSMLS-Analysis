import shelve
import os

run_log = {
    'RES': {
        'Q1': 'DOWNLOADED',
        'Q2': 'DOWNLOADED',
        'Q3': 'DOWNLOADED',
        'Q4': 'DOWNLOADED'
    },
    'MUL': {
        'Q1': 'DOWNLOADED',
        'Q2': 'DOWNLOADED',
        'Q3': 'DOWNLOADED',
        'Q4': 'DOWNLOADED'
    },
    'LND': {
        'Q1': 'DOWNLOADED',
        'Q2': 'DOWNLOADED',
        'Q3': 'IN PROCESS',
        'Q4': 'IN PROCESS'
    }
}
os.chdir('F:\\Python 2.0\\Projects\\Real Life Projects\\Real Estate Analysis\\Saved Data')
with shelve.open('GSMLS Run Dictionary', writeback=True) as saved_data_file:
    saved_data_file['Run Log'] = run_log

