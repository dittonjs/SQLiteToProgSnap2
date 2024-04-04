import csv
import sqlite3
from prog_snap_2 import ProgSnap2


def read_all(connection: sqlite3.Connection, table_name: str):
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT * FROM '{table_name}'")
        return cursor.fetchall()
    except sqlite3.OperationalError as e:
        return []


def read_where(connection: sqlite3.Connection, table_name: str, where_clause: str, keys="*"):
    cursor = connection.cursor()
    try:
        cursor.execute(f"SELECT {keys} FROM '{table_name}' WHERE {where_clause}")
        return cursor.fetchall()
    except sqlite3.OperationalError as e:
        return []


def read_project_files_to_dict(connection: sqlite3.Connection):
    project_files = read_all(connection, "ProjectFiles")
    file_map = {}
    for file in project_files:
        file_map[file[0]] = file

    return file_map


def get_project_files_with_outside_edits(connection):
    return read_where(
        connection=connection,
        table_name="UserActions",
        where_clause="name='OUTSIDE_EDIT'",
        keys="DISTINCT projectFile"
    )


def num_disordered_events(connection, project_file_id):
    edits = read_where(
        connection=connection,
        table_name="Edits",
        where_clause=f"projectFile={project_file_id}",
        keys="id, clientTimestamp, sourceLocation, *"
    )
    disordered_events = 0
    if edits:
        last_timestamp = edits[0][1]
        last_source_location = edits[0][2]
        for edit in edits:
            if edit[1] < last_timestamp:
                disordered_events += 1
            elif edit[1] == last_timestamp and edit[2] < last_source_location:
                disordered_events += 1

            last_timestamp = edit[1]
            last_source_location = edit[2]

    return disordered_events


def get_files_with_disordered_events(connection, files):
    report = {}
    for fileId in files:

        count = num_disordered_events(connection, fileId)
        report[files[fileId][1]] = count
    return report


def convert_sqlite(db_filepath: str, writer, assignment_name="assignment_0", student_name="student"):
    # has_issues = False
    with (sqlite3.connect(db_filepath) as connection):
        # get events from database
        files = read_project_files_to_dict(connection)
        #print(files)
        edits = read_all(connection, "Edits")
        actions = read_all(connection, "UserActions")

        # executions = read_all(connection, "ProgramExecutions")

        # convert events
        prog_snap_2s: list[ProgSnap2] = [
            *[ProgSnap2.from_edit(edit, files, assignment_name, student_name) for edit in edits],
            *[ProgSnap2.from_action(action, files, assignment_name, student_name) for action in actions],
            # *[ProgSnap2.from_execution(execution, files) for execution in executions]
        ]

        prog_snap_2s.sort(key=lambda x: (x.client_timestamp, x.source_location))

        for file in files.items():
            #print(file)
            code_state_section = file[1][1]
            if len(code_state_section) > 0 and code_state_section[0] == '/':
                code_state_section = code_state_section[1:]
            writer.writerow([
                '',
                file[1][0],
                student_name,
                assignment_name,
                code_state_section,
                "X-FileInit",
                "0",
                '',
                file[1][2],
                "",
                "",
                "0",
                "",
                "",
                ""
            ])

        # write other events
        for ps2 in prog_snap_2s:
            ps2.write_row(writer)


    return False # this can be used to track issues but is not currently being used.





