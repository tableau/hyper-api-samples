import click


import tableauhyperapi as hapi


from pathlib import Path
from git import Repo
from git.objects import Commit


__database_file = Path("git.hyper")


def create_table(con: hapi.Connection, table_name: str, columns: list):
    hapi_columns = []
    for col in columns:
        hapi_columns.append(hapi.TableDefinition.Column(name=col[0], type=col[1], nullability=hapi.NOT_NULLABLE))
    table = hapi.TableDefinition(table_name=hapi.TableName(table_name), columns=hapi_columns)
    con.catalog.create_table(table_definition=table)
    return table


def create_tables(con: hapi.Connection):
    tables = dict()
    tables['commits'] = create_table(con, 'commits', [
        ('commit_sha', hapi.SqlType.text()),
        ('authored_date', hapi.SqlType.int()),
        ('committed_date', hapi.SqlType.int()),
        ('author_mail', hapi.SqlType.text()),
        ('committer_mail', hapi.SqlType.text()),
        ('insertions', hapi.SqlType.int()),
        ('deletions', hapi.SqlType.int()),
        ('number_of_changed_lines', hapi.SqlType.int()),
        ('number_of_changed_files', hapi.SqlType.int()),
        ('changed_files', hapi.SqlType.text()),
        ('message', hapi.SqlType.text())])
    tables['changed_files'] = create_table(con, 'changed_files', [
        ('commit_sha', hapi.SqlType.text()),
        ('file_name', hapi.SqlType.text())])
    return tables


def insert_commit_data(commit: Commit, table: hapi.TableDefinition, con: hapi.Connection):
    with hapi.Inserter(con, table) as inserter:
        inserter.add_row([commit.hexsha,
                          commit.authored_date,
                          commit.committed_date,
                          commit.author.email,
                          commit.committer.email,
                          commit.stats.total['insertions'],
                          commit.stats.total['deletions'],
                          commit.stats.total['lines'],
                          commit.stats.total['files'],
                          ', '.join([key for key in commit.stats.files.keys()]),
                          commit.message])
        inserter.execute()


def insert_changed_files_data(commit: Commit, table: hapi.TableDefinition, con: hapi.Connection):
    changed_files = [key for key in commit.stats.files.keys()]
    if changed_files:
        data_to_insert = []
        for file_name in changed_files:
            data_to_insert.append([commit.hexsha, file_name])
        with hapi.Inserter(con, table) as inserter:
            inserter.add_rows(rows=data_to_insert)
            inserter.execute()


@click.command()
@click.option('--path_to_repo', required=True, help='Path to the repository, e.g. ~/src/repo')
@click.option('--branch', default='main', help='Branch to follow in the repository. Default: main')
def main(path_to_repo: Path, branch: str):
    """
    Extracting meta data of git repository into HYPER file.
    """
    # set up Hyper Database
    with hapi.HyperProcess(telemetry=hapi.Telemetry.SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with hapi.Connection(endpoint=hyper.endpoint, database=__database_file, create_mode=hapi.CreateMode.CREATE_AND_REPLACE) as con:
            tables = create_tables(con)
            repo = Repo(path_to_repo)

            # Iterate over all commits
            for commit_idx, commit in enumerate(repo.iter_commits(branch)):
                print(f"Analyzing commit {commit_idx+1}/{len(list(repo.iter_commits(branch)))} ({commit.hexsha})")
                insert_commit_data(commit, tables['commits'], con)
                insert_changed_files_data(commit, tables['changed_files'], con)

            # print statistics
            number_of_changed_files = con.execute_scalar_query(query=f"SELECT COUNT(*) FROM {tables['changed_files'].table_name}")
            number_of_commits = con.execute_scalar_query(query=f"SELECT COUNT(*) FROM {tables['commits'].table_name}")
            print(f"Analyzed {number_of_changed_files} changed files for {number_of_commits} commits")


if __name__ == '__main__':
    main()
