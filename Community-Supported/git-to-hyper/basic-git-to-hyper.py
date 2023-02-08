import argparse


import tableauhyperapi as hapi


from pathlib import Path
from datetime import datetime
from git import Repo
from git.objects import Commit


__database_file = Path("git.hyper")


def create_table(con: hapi.Connection):
    columns = [
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
        ('message', hapi.SqlType.text()),
    ]
    hapi_columns = [hapi.TableDefinition.Column(name=col[0], type=col[1], nullability=hapi.NOT_NULLABLE) for col in columns]
    table = hapi.TableDefinition(table_name='commits', columns=hapi_columns)
    con.catalog.create_table(table_definition=table)
    return table


def insert_commit_data(commit: Commit, table: hapi.TableDefinition, con: hapi.Connection):
    with hapi.Inserter(con, table) as inserter:
        inserter.add_row([commit.hexsha,
                          datetime.utcfromtimestamp(commit.authored_date),
                          datetime.utcfromtimestamp(commit.committed_date),
                          commit.author.email,
                          commit.committer.email,
                          commit.stats.total['insertions'],
                          commit.stats.total['deletions'],
                          commit.stats.total['lines'],
                          commit.stats.total['files'],
                          ', '.join(list(commit.stats.files.keys())),
                          commit.message])
        inserter.execute()


def main(path_to_repo: Path, branch: str):
    """
    Extracting meta data of git repository into HYPER file.
    """
    # set up Hyper Database
    with hapi.HyperProcess(telemetry=hapi.Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with hapi.Connection(endpoint=hyper.endpoint, database=__database_file, create_mode=hapi.CreateMode.CREATE_AND_REPLACE) as con:
            table = create_table(con)
            repo = Repo(path_to_repo)

            # Iterate over all commits
            for commit_idx, commit in enumerate(repo.iter_commits(branch)):
                print(f"Analyzing commit {commit_idx+1}/{len(list(repo.iter_commits(branch)))} ({commit.hexsha})")
                insert_commit_data(commit, table, con)

            # print statistics
            number_of_commits = con.execute_scalar_query(f"SELECT COUNT(*) FROM {table.table_name}")
            print(f"Analyzed {number_of_commits} commits")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("path_to_repo", help='Path to the repository, e.g. ~/src/repo')
    parser.add_argument("--branch", default='main', help='Branch to follow in the repository. Default: main')
    args = parser.parse_args()
    main(args.path_to_repo, args.branch)
