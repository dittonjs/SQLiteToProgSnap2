from typing import Optional
import csv

class ProgSnap2:
    def __init__(
            self,
            raw_event: tuple,
            event_id: int,
            subject_id: str,
            assignment_id: str,
            code_state_section: str,
            event_type: str,
            source_location: Optional[int],
            edit_type: str,
            insert_text: str,
            delete_text: str,
            metadata: str,
            client_timestamp: int,
            tool_instances: str,
            code_state_id: str,
    ):
        self.raw_event = raw_event
        self.event_id = event_id
        self.subject_id = subject_id
        self.assignment_id = assignment_id
        self.code_state_section = code_state_section
        self.event_type = event_type
        self.source_location = source_location
        self.edit_type = edit_type
        self.insert_text = insert_text
        self.delete_text = delete_text
        self.metadata = metadata
        self.client_timestamp = client_timestamp
        self.tool_instances = tool_instances
        self.code_state_id = code_state_id

    def write_row(self, writer):
        writer.writerow((
            "",
            str(self.event_id),
            self.subject_id,
            self.assignment_id,
            self.code_state_section,
            self.event_type,
            str(self.source_location) if self.source_location else '',
            self.edit_type,
            self.insert_text,
            self.delete_text,
            self.metadata,
            self.client_timestamp,
            self.code_state_id,
        ))

    @staticmethod
    def from_edit(edit):
        return ProgSnap2(
            raw_event=edit,
            event_id=edit[0],
            subject_id="student",
            assignment_id="",
            code_state_section=edit[5],
            event_type="File.Edit",
            source_location=edit[3],
            edit_type=edit[7],
            insert_text=edit[1],
            delete_text=edit[2],
            metadata=f"reverted: {edit[8]}",
            client_timestamp=edit[4],
            tool_instances=edit[6],
            code_state_id=""
        )

    @staticmethod
    def from_action(action, files):
        return ProgSnap2(
            raw_event=action,
            event_id=action[0],
            subject_id="student",
            assignment_id="",
            code_state_section="" if not action[4] else files[action[4]][2],
            event_type=action[2],
            source_location=None,
            edit_type="",
            insert_text="",
            delete_text="",
            metadata=action[3],
            client_timestamp=action[1],
            tool_instances="",
            code_state_id=""
        )

    # ,EventID,SubjectID,AssignmentID,CodeStateSection,EventType,SourceLocation,EditType,InsertText,DeleteText,X-Metadata,ClientTimestamp,ToolInstances,CodeStateID
    # 574262,574262,Student23,Assign11,task1.py,Run.Program,,,,,Start,1637356595913,PC;PP 1.1.10,
    # 574263,574263,Student23,Assign11,task1.py,Run.Program,,,,,1,1637356622571,PC;PP 1.1.10,
    @staticmethod
    def from_execution(execution, files):
        return ProgSnap2Execution(
            raw_event=execution,
            event_id=execution[0],
            subject_id="student",
            assignment_id="",
            code_state_section=files[execution[7]][2],
            event_type="Run.Program",
            source_location=None,
            edit_type="",
            insert_text="",
            delete_text="",
            metadata="Start",
            client_timestamp=execution[1],
            tool_instances="",
            code_state_id=""
        )


# Execution events are one row in the DB but 2 rows in the CSV file - one for the
# beginning of the execution, one for the end.
class ProgSnap2Execution(ProgSnap2):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        execution = self.raw_event
        self.finish_event = ProgSnap2(
            raw_event=self.raw_event,
            event_id=self.event_id,
            subject_id="student",
            assignment_id="",
            code_state_section=self.code_state_section,
            event_type=str(execution[3]),
            source_location=None,
            edit_type="",
            insert_text="",
            delete_text="",
            metadata="Start",
            client_timestamp=execution[2],
            tool_instances="",
            code_state_id=""
        )

    def write_row(self, writer):
        super().write_row(writer)
        self.finish_event.write_row(writer)


