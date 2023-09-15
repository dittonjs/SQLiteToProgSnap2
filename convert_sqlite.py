import csv
import sqlite3
from prog_snap_2 import ProgSnap2


def read_all(connection: sqlite3.Connection, table_name: str):
    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM '{table_name}'")
    return cursor.fetchall()


def read_project_files_to_dict(connection: sqlite3.Connection):
    project_files = read_all(connection, "ProjectFiles")
    file_map = {}
    for file in project_files:
        file_map[file[0]] = file

    return file_map


def convert_sqlite(db_filepath: str, out_filepath: str = "progsnap2.csv"):
    with (sqlite3.connect(db_filepath) as connection):
        try:
            with open(out_filepath, 'w', newline="") as outfile:
                files = read_project_files_to_dict(connection)

                # get events from database
                edits = read_all(connection, "Edits")
                actions = read_all(connection, "UserActions")
                executions = read_all(connection, "ProgramExecutions")

                # convert events
                prog_snap_2s: list[ProgSnap2] = [
                    *[ProgSnap2.from_edit(edit) for edit in edits],
                    *[ProgSnap2.from_action(action, files) for action in actions],
                    *[ProgSnap2.from_execution(execution, files) for execution in executions]
                ]

                prog_snap_2s.sort(key=lambda x: x.client_timestamp)

                writer = csv.writer(outfile, dialect=csv.unix_dialect)

                writer.writerow([
                    '',
                    'EventID',
                    'SubjectID',
                    'AssignmentID',
                    'CodeStateSection',
                    'EventType',
                    'SourceLocation',
                    'EditType',
                    "InsertText",
                    "DeleteText",
                    "X-Metadata",
                    "ClientTimestamp",
                    "ToolInstances",
                    "CodeStateID"
                ])

                # write other events

                for ps2 in prog_snap_2s:
                    ps2.write_row(writer)

        except FileExistsError:
            print(f"Outfile {out_filepath} already exists. Move it or deleted before running the script.")






