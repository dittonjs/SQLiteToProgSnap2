import csv
import tempfile

from convert_sqlite import convert_sqlite
from zipfile import ZipFile
import sys
import os
import pathlib


#-----------------------------------------------------------
# Usage:
# convert_all assignment_name submissions_zip output_dir
#-----------------------------------------------------------


def find_and_export_syw_file(
        assn_name,
        folder,
        outdir,
        submission_file,
        problem_files,
        writer
):
    if os.path.exists(os.path.join(folder, "_showyourwork.sqlite")):
#        print(f"found file at {folder}")
#        print(assn_name)
        subject_id = submission_file.split("_")[0]
        convert_sqlite(os.path.join(folder, "_showyourwork.sqlite"), writer, assn_name, subject_id)
        return True

    for item in os.listdir(folder):
        if os.path.isdir(os.path.join(folder, item)):
            found = find_and_export_syw_file(assn_name, os.path.join(folder, item), outdir, submission_file, problem_files, writer)
            if found:
                return True

    return False


if __name__ == "__main__":
    _, assignment_name, submissions_zip, outdir = sys.argv
    print(assignment_name)
    pathlib.Path(outdir).mkdir(parents=True, exist_ok=True)

    with ZipFile(submissions_zip,  'r') as submission_zip_ref:
        with tempfile.TemporaryDirectory() as tempdir:
            submission_zip_ref.extractall(tempdir)
            #print("EXTRACTED SUBMISSIONS")
            problem_files = []
            with open(os.path.join(outdir, "export.csv"), 'w', newline="") as outfile:
                writer = csv.writer(outfile)

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
                    "CodeStateID",
                    "X-UserActionID",
                ])
                for submission_file in os.listdir(tempdir):

                    try:
                        print(f"EXTRACTING {submission_file}")
                        #print("============================================")
                        with ZipFile(os.path.join(tempdir, submission_file), 'r') as student_zip:
                            student_zip_path = os.path.join(tempdir, submission_file.replace(".zip", ""))
                            student_zip.extractall(student_zip_path)


                            find_and_export_syw_file(
                                assignment_name,
                                student_zip_path,
                                outdir,
                                submission_file.replace(".zip", ""),
                                problem_files,
                                writer
                            )
                    except IsADirectoryError:
                        print(f"Skipping folder {submission_file} because it is not a zip folder")
                    except:
                        print(f"Failed to extract {submission_file}")





