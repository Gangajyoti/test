from code.file_validation import check_files
from code.Location_Validation import data_validation
from pathlib import Path



# files = check_files(for_date=20210528182844)
files = ['data_file_20210528182844.csv', 'data_file_20210528182554.csv']
for file in files:
    path = f'src/data/src_data_daily/{file}'
    data_validation(data_file_path = path ,ref_file_path= 'src/data/Areas_in_blore.xlsx')



