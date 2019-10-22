def raise_if_success_file_is_missing(directory, sc):
    try:
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
        if fs.exists(sc._jvm.org.apache.hadoop.fs.Path(directory + '/_SUCCESS')):
            return None
    except:
        pass
    
    raise ValueError('The \'_SUCCESS\' file seems missing in ' + directory)
