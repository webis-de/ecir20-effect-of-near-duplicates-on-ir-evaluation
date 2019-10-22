def all_files_in_hdfs_dir(hdfs_path):
    import os
    import re
    import subprocess

    cmd = 'hdfs dfs -ls ' + hdfs_path
    cmd_output = subprocess.check_output(cmd, shell=True)
    ret = []
    for line in cmd_output.decode('utf-8').strip().split('\n'):
        fields = re.split('\s+', line)
        if len(fields) == 8:
            ret += [fields[-1]]

    return ret

