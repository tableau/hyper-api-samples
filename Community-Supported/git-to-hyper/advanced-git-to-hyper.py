"""
This file serves as an example on how to insert data into a hyper database while using multiple
processes to extract data at the same time.

As the Hyper API does not allow multiple connections to the same database it is not possible to
insert the data directly from all processes. To overcome this limitation the following example uses
multiple "extraction processes" mining the data and a single "Injection Process" which inserts it
into the Hyper database. The data is synced between those processes using one Manager.Queue object
from the multiprocessing library per SQL Table.

This way only a single connection to the Hyper database is required while all available resources
can be used to extract the data.
"""

import argparse
import queue
import os
import tempfile
import hashlib
import subprocess
import time

import tableauhyperapi as hapi

from distutils.dir_util import copy_tree
from datetime import datetime
from pathlib import Path
from git import Repo
from git.objects import Commit
from multiprocessing import cpu_count, Manager, Process
from multiprocessing.managers import ValueProxy
from typing import List, Dict
from enum import Enum

__database_file = Path("git.hyper")


class Tables(Enum):
    COMMITS = 0,
    CHANGED_FILES = 1,
    FILE_COMMIT_MAPPING = 2,
    BLAME = 3

def hash_bytestr_iter(bytesiter, hasher):
    for block in bytesiter:
        hasher.update(block)
    return hasher.hexdigest()


def file_as_blockiter(afile, blocksize=65536):
    with afile:
        block = afile.read(blocksize)
        while len(block) > 0:
            yield block
            block = afile.read(blocksize)


def reset_repo_to_commit(repo: Repo, commit: Commit):
    repo.head.reference = commit
    repo.head.reset(index=True, working_tree=True)
    repo.git.clean('-xfd')

def create_table(con: hapi.Connection, table_name: str, columns: list):
    hapi_columns = [hapi.TableDefinition.Column(name=col[0], type=col[1], nullability=hapi.NOT_NULLABLE) for col in columns]
    table = hapi.TableDefinition(table_name=hapi.TableName(table_name), columns=hapi_columns)
    con.catalog.create_table(table_definition=table)
    return table

def set_up_database(con: hapi.Connection) -> Dict[str, hapi.TableDefinition]:
    # create tables
    tables = dict()
    tables[Tables.COMMITS] = create_table(con, 'commits', [
        ('commit_sha', hapi.SqlType.text()),
        ('authored_date', hapi.SqlType.timestamp()),
        ('committed_date', hapi.SqlType.timestamp()),
        ('author_mail', hapi.SqlType.text()),
        ('committer_mail', hapi.SqlType.text()),
        ('insertions', hapi.SqlType.int()),
        ('deletions', hapi.SqlType.int()),
        ('number_of_changed_lines', hapi.SqlType.int()),
        ('number_of_changed_files', hapi.SqlType.int()),
        ('changed_files', hapi.SqlType.text()),
        ('message', hapi.SqlType.text())])
    tables[Tables.CHANGED_FILES] = create_table(con, 'changed_files', [
        ('commit_sha', hapi.SqlType.text()),
        ('file_name', hapi.SqlType.text())])
    tables[Tables.FILE_COMMIT_MAPPING] = create_table(con, 'file_commit_mapping', [
        ('commit_sha', hapi.SqlType.text()),
        ('file_hash', hapi.SqlType.text()),
        ('file_name', hapi.SqlType.text())])
    tables[Tables.BLAME] = create_table(con, 'blame', [
        ('file_hash', hapi.SqlType.text()),
        ('author_mail', hapi.SqlType.text()),
        ('number_of_chars', hapi.SqlType.int())])

    # connect tables using FOREIGN KEY
    con.execute_command(f'ALTER TABLE {tables[Tables.COMMITS].table_name} ADD ASSUMED PRIMARY KEY ("commit_sha")')
    con.execute_command(f'ALTER TABLE {tables[Tables.BLAME].table_name} ADD ASSUMED PRIMARY KEY ("file_hash")')
    con.execute_command(f'''ALTER TABLE {tables[Tables.CHANGED_FILES].table_name} ADD ASSUMED FOREIGN KEY
                ("commit_sha") REFERENCES  {tables[Tables.COMMITS].table_name}''')
    con.execute_command(f'''ALTER TABLE {tables[Tables.FILE_COMMIT_MAPPING].table_name} ADD ASSUMED FOREIGN KEY
                ("commit_sha") REFERENCES  {tables[Tables.COMMITS].table_name}''')
    con.execute_command(f'''ALTER TABLE {tables[Tables.FILE_COMMIT_MAPPING].table_name} ADD ASSUMED FOREIGN KEY
                ("file_hash") REFERENCES  {tables[Tables.BLAME].table_name}''')
    return tables

def insert_commit_data(injection_queue: queue.Queue, commit: Commit):
    commit_message = commit.message.replace('\'', '\'\'')
    injection_queue.put([commit.hexsha,
                         datetime.utcfromtimestamp(commit.authored_date),
                         datetime.utcfromtimestamp(commit.committed_date),
                         commit.author.email,
                         commit.committer.email,
                         commit.stats.total['insertions'],
                         commit.stats.total['deletions'],
                         commit.stats.total['lines'],
                         commit.stats.total['files'],
                         ', '.join(list(commit.stats.files.keys())),
                         commit_message])

def insert_changed_files_data(changed_files: list, injection_queue: queue.Queue, commit_sha: str):
    if changed_files:
        for file in changed_files:
            injection_queue.put([commit_sha, file])

def run_git_blame(inject_queue: queue.Queue, relative_file_path: Path, file_hash: str, repo_dir: str, verbose: bool):
    start_time = time.time()
    git_blame_output = subprocess.run(f"git blame -w -C -M --line-porcelain {relative_file_path}", shell=True, capture_output=True, cwd=repo_dir).stdout.decode('utf-8').split('\n')
    end_time = time.time()
    authors_per_line = [line.split('<')[1].rstrip('>') for line in git_blame_output if 'author-mail' in line]
    for author in set(authors_per_line):
        inject_queue.put([file_hash, author, authors_per_line.count(author)])
    if verbose and end_time - start_time > 5:
        print(f"  running git blame for {relative_file_path} took quite long ({pretty_timedelta_string_in_seconds(start_time, end_time)})")

def insert_git_blame_data(inject_queues: List[queue.Queue], commit: Commit, commit_idx: int, blame_only_for_head: bool, changed_files: list, file_size_limit: int, verbose: bool, repo_dir: str, repo: Repo):
    """
    Run git blame for all changed files (in case of HEAD for all files)
    """
    if commit_idx == 0 or not blame_only_for_head:  # commit_idx = 0 is the HEAD commit
        all_files = [f for f in list(Path(repo_dir).rglob("*")) if '/.git/' not in os.fsdecode(f) and f.is_file()]
        for file_counter, file in enumerate(all_files):
            file_hash = hash_bytestr_iter(file_as_blockiter(open(file, 'rb')), hashlib.sha256())
            relative_file_path = str(os.fsdecode(file)).replace(f"{repo.working_dir}/", '')
            inject_queues[Tables.FILE_COMMIT_MAPPING].put([commit.hexsha, file_hash, relative_file_path])
            if relative_file_path in changed_files or commit_idx == 0:  # the first commit needs to run for all files, as we don't have any data from previous runs we can reuse
                if file_size_limit and file_size_limit > os.path.getsize(file):
                    if verbose:
                        print(f"{relative_file_path} ({file_counter}/{len(all_files)})")
                    run_git_blame(inject_queues[Tables.BLAME], relative_file_path, file_hash, repo_dir, verbose)
                else:
                    print(f"  {relative_file_path}: skipped because of user defined file size limit ({os.path.getsize(file)} > {file_size_limit})")


def pretty_timedelta_string_in_seconds(start, end):
    return "{0:0.3f}".format((end - start)) + ' seconds'


def extraction_func(extraction_backlog: queue.Queue, inject_queues: List[queue.Queue], ram_disk_dir: Path, path_to_repo: Path, file_size_limit: int, blame_only_for_head: bool, verbose: bool, branch: str):
    """
    This function will be called in several parallel processes at the same time to jointly extract
    the meta data from git. It will terminate once there are no more commits to analyze. All results
    are written into injection_queues which are processed by the single injection process.
    """
    # set up repo on ramdisk
    with tempfile.TemporaryDirectory(dir=ram_disk_dir) as temp_dir:
        copy_tree(path_to_repo, temp_dir)
        repo = Repo(temp_dir)

        # Pick a commit_idx from the backlog and analyze it until backlog is empty
        while extraction_backlog.qsize() > 0:
            # try getting an item from the backlog, terminate in case it's empty
            try:
                commit_idx = extraction_backlog.get()
            except queue.Empty:
                break

            # get commit
            for idx, commit in enumerate(repo.iter_commits(branch)):
                if idx == commit_idx:
                    break

            # analyze commit
            print(f"Analyzing commit {commit_idx+1}/{len(list(repo.iter_commits(branch)))} ({commit.hexsha})")
            reset_repo_to_commit(repo, commit)
            insert_commit_data(inject_queues[Tables.COMMITS], commit)
            changed_files = list(commit.stats.files.keys())
            insert_changed_files_data(changed_files, inject_queues[Tables.CHANGED_FILES], commit.hexsha)
            insert_git_blame_data(inject_queues, commit, commit_idx, blame_only_for_head, changed_files, file_size_limit, verbose, temp_dir, repo)

def injection_func(inject_queues: List[queue.Queue], extraction_done: ValueProxy):
    """
    Injecting all extracted data into Hyper database in a single process (The public Hyper API does
    not support multiple parallel connections from different processes).

    This function needs to be terminated from the outside once all extraction processes have
    terminated and all injection_queues are empty
    """
    with hapi.HyperProcess(telemetry=hapi.Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with hapi.Connection(endpoint=hyper.endpoint,
                             database=__database_file,
                             create_mode=hapi.CreateMode.CREATE_AND_REPLACE) as con:
            tables = set_up_database(con)
            while True:
                for table_name, iq in inject_queues.items():
                    if iq.qsize() > 0:
                        data = iq.get()
                        with hapi.Inserter(con, tables[table_name]) as inserter:
                            inserter.add_row(data)
                            inserter.execute()

                # check if extraction is done and all data has been saved to Hyper file. If so, terminate injection by returning to main
                injection_queues_all_empty = sum([inject_queue.qsize() for inject_queue in inject_queues.values()]) == 0
                if injection_queues_all_empty and extraction_done.get():
                    return


def main(path_to_repo: Path, branch: str, number_of_workers: int, ram_disk_dir: Path, file_size_limit: int, blame_only_for_head: bool, verbose: bool):
    """
    Extracting meta data of git repository into HYPER file.
    """
    with Manager() as manager:
        # set up extraction backlog
        extraction_backlog = manager.Queue()
        for commit_idx, commit in enumerate(Repo(path_to_repo).iter_commits(branch)):
            extraction_backlog.put(commit_idx)

        # set up injection queues
        inject_queues = dict()
        inject_queues[Tables.COMMITS] = manager.Queue()
        inject_queues[Tables.CHANGED_FILES] = manager.Queue()
        inject_queues[Tables.FILE_COMMIT_MAPPING] = manager.Queue()
        inject_queues[Tables.BLAME] = manager.Queue()

        # start multiple extraction processes
        extract_procs = list()
        number_of_workers = int(number_of_workers) if number_of_workers else max([1, int(cpu_count() / 3)])  # pretty good self tuning value
        for _ in range(number_of_workers):
            new_worker = Process(target=extraction_func, args=(extraction_backlog, inject_queues, ram_disk_dir, path_to_repo, file_size_limit, blame_only_for_head, verbose, branch))
            extract_procs.append(new_worker)
            new_worker.start()
        extraction_done = manager.Value(bool, False)

        # start single injection process
        injection_proc = Process(target=injection_func, args=(inject_queues, extraction_done))
        injection_proc.start()

        # wait until all extraction processes are done
        for ep in extract_procs:
            ep.join()
        extraction_done.set(True)

        # wait for all remaining data being written to the hyper file
        print('Waiting for remaining data to be written to the hyper file...', end='', flush=True)
        injection_proc.join()
        print('done')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("path_to_repo", help='Path to the repository, e.g. ~/src/repo')
    parser.add_argument("--branch", default='main', help='Branch to follow in the repository. Default: main')
    parser.add_argument("--ram_disk_dir", default=None, help='Path to ram disk on the host machine. Used to improve the execution time by speeding up I/O heavy git operations. Using "/dev/shm" should work for most Linux OS, if you are using a different OS you might need to create the ram disk manually first. It needs to have at least the size of the repository.')
    parser.add_argument("--number_of_workers", default=None, help='How many parallel processes shall be used for the data extraction')
    parser.add_argument("--file_size_limit", default=10 * 1024 * 1024, help='Files bigger than this limit are not analyzed. The unit is byte. Can be turned off by setting it to None. Default: 10 MB')
    parser.add_argument("--blame_only_for_head", action='store_true', help='Run git blame only for the HEAD commit to speed up the data collection')
    parser.add_argument("--verbose", action='store_true', help='Increase verbosity, e.g. print filenames of git blame targets')
    args = parser.parse_args()
    main(args.path_to_repo, args.branch, args.number_of_workers, args.ram_disk_dir, args.file_size_limit, args.blame_only_for_head, args.verbose)
