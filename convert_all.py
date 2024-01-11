import tempfile

from convert_sqlite import convert_sqlite
from zipfile import ZipFile
import sys
import os
import pathlib


def find_and_export_syw_file(assn_name, folder, outdir, csv_file_name, problem_files):
    if os.path.exists(os.path.join(folder, "_showyourwork.sqlite")):
        print(f"found file at {folder}")
        print(assn_name)
        has_issues = convert_sqlite(os.path.join(folder, "_showyourwork.sqlite"), os.path.join(outdir, csv_file_name + ".csv"), assn_name, csv_file_name.split("_")[0])
        if has_issues:
            problem_files.append(csv_file_name)
        return True

    for item in os.listdir(folder):
        if os.path.isdir(os.path.join(folder, item)):
            found = find_and_export_syw_file(assn_name, os.path.join(folder, item), outdir, csv_file_name, problem_files)
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
            print("EXTRACTED SUBMISSIONS")
            problem_files = []
            for submission_file in os.listdir(tempdir):
                print(f"EXTRACTING {submission_file}")
                print("============================================")
                with ZipFile(os.path.join(tempdir, submission_file), 'r') as student_zip:
                    student_zip_path = os.path.join(tempdir, submission_file.replace(".zip", ""))
                    student_zip.extractall(student_zip_path)

                    find_and_export_syw_file(
                        assignment_name,
                        student_zip_path,
                        outdir,
                        submission_file.replace(".zip", ""),
                        problem_files
                    )

            print(len(problem_files))





