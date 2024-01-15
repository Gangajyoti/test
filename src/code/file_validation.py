import os
from pathlib import Path
from datetime import datetime
import shutil

"""
1 - File Check Module
The intention of this module is to verify that the files received daily are as expected, prior to
processing them. It will read the files daily, from the given source location and check for the
below requirements. Any file not meeting these requirements should not be processed and be
placed in a separate directory.

1. Is this a new file? We do not want to reprocess already processed files. The module
should also be able to process more than one unique file per day.
2. Is the file empty? Only process non-empty files.
3. Is the file extension .csv?
"""


path_root = Path(__file__).parents[2]

def check_files(data_dir="\src\data\src_data_daily",
                for_date = datetime.now().strftime("%Y%m%d"),
                rm_file_dir="\src\data\src_data_daily_removed"):
    """
    functionality : checks for daily,non-empty csv files and copy the check failed files to other directory (rm_file_dir)
                    and delete the from the source (i.e, data_dir)
    :param data_dir: reference source data directory path.
    :param for_date: runs for current date.{for custom date run - pass date_time value like 20210528182844}
    :param rm_file_dir: reference removed files directory path
    :return: passed file list
    """
    src_dir_path = str(path_root)+data_dir
    file_list = os.listdir(src_dir_path)
    discarded_list = list()

    for file in file_list:
        file_date = file.split(".")[0].split("_")[-1]

        if (not file.startswith("data_file_")) or (not file.endswith(".csv")) or (os.stat(f"{src_dir_path}\\{file}").st_size == 0) or (str(for_date)[:8] != file_date[:8]  ):
            discarded_list.append(file)


    passed_files = list(set(file_list)-set(discarded_list))

    rm_dir_path = os.path.join(str(path_root)+rm_file_dir)

    if not os.path.isdir(rm_dir_path):
        print(f"""Directory need to be created: \n creating dir {rm_file_dir}""")
        os.mkdir(rm_dir_path)

    if discarded_list:
        print(f"Discarded files (file_count ={len(discarded_list)}) are being copied to the {rm_dir_path}")
        for file in discarded_list:
            shutil.copy2(f"{src_dir_path}\\{file}",rm_dir_path)
            os.remove(os.path.join(f"{src_dir_path}\\{file}"))

    return passed_files






check_files(for_date=20210528182844)




