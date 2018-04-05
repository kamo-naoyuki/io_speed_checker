import csv
import datetime
import os
import pandas
from pathlib import Path
import subprocess
import tempfile
import time
from typing import Sequence


def write_measure(dirname: str='.', megabytes: int=1, nloop: int=1):
    fd, fname = tempfile.mkstemp(dir=dirname)
    string = b'\0' * megabytes * 1024 ** 2

    t = time.perf_counter()
    for i in range(nloop):
        with open(fname, 'wb') as f:
            f.write(string)
    os.fdopen(fd).close()
    os.unlink(fname)
    return megabytes / (time.perf_counter() - t) / nloop


def read_measure(dirname: str='.', megabytes: int=1, nloop: int=1):
    fd, fname = tempfile.mkstemp(dir=dirname)
    string = b'\0' * megabytes * 1024 ** 2
    with open(fname, 'wb') as f:
        f.write(string)

    t = time.perf_counter()
    for i in range(nloop):
        with open(fname, 'rb') as f:
            f.read()
    os.fdopen(fd).close()
    os.unlink(fname)
    return megabytes / (time.perf_counter() - t) / nloop


def io_measure(dirname: str='.', megabytes: int=1, nloop: int=1):
    r = read_measure(dirname=dirname, megabytes=megabytes, nloop=nloop)
    w = write_measure(dirname=dirname, megabytes=megabytes, nloop=nloop)
    return r, w


def get_info(dirlist: Sequence[str]=('.',),
             megabytes: int=1024, nloop: int=1):
    ret = {di: io_measure(dirname=di, megabytes=megabytes, nloop=nloop)
           for di in dirlist}
    return ret


def write_csv(outdir: str='.',
              dirlist: Sequence[str]=('.',), megabytes: int=1,
              nloop: int=1,
              interval: float=0.,
              max_nlines: int=1000,
              max_logs: int=1000):
    while True:
        n = datetime.datetime.now().strftime('%Y-%m-%d.%H:%M:%S')
        with (Path(outdir) / f'data_{n}.csv').open('w') as f:
            writer = csv.writer(f, lineterminator='\n')
            writer.writerow(sum([[f'{Path(d).absolute()} read[MB/s]',
                                  f'{Path(d).absolute()} write[MB/s]']
                                 for d in dirlist], ['']))

            for i in range(max_nlines):
                t = time.perf_counter()

                ret = get_info(dirlist=dirlist,
                               megabytes=megabytes, nloop=nloop)
                writer.writerow(
                    sum([[str(ret[d][0]), str(ret[d][1])] for d in dirlist],
                         [str(datetime.datetime.now())]))

                delta = interval - (time.perf_counter() - t)
                if delta > 0.:
                    time.sleep(delta)

            with os.scandir(outdir) as it:
                entrys = [entry for entry in it
                          if entry.name.startswith('data_')
                          and entry.is_file()]

                if len(entrys) > max_logs:
                    entrys = sorted(entrys, lambda e: e.stat().st_mtime)
                    entrys = entrys[len(datafiles) - max_logs]
                    for e in entrys:
                        os.unlink(e.name)


def pan():
    df = pandas.DataFrame(
        data={'a': [1,2,3],
              'b': [1,2,3]},
        index=pandas.to_datetime(['20180402', '20180403', '20180404']))
    df = pandas.DataFrame(
        data={'c': [1,2,3],
              'd': [1,2,3]},
        index=pandas.to_datetime(['20180402', '20180403', '20180404']))
    df = df + df
    print(df)
write_csv()
