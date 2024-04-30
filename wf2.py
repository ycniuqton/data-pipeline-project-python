# @@@SNIPSTART data-pipeline-your-workflow-python
from datetime import timedelta
from typing import Any, List

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import phase_1, phase_2, phase_3

from abc import ABC, abstractmethod
from activities import Phase1Handler, Phase2Handler, Phase3Handler


# Define the abstract command class
class Command(ABC):
    def __init__(self, activity, params=None):
        self.activity = activity
        self.params = params if params else {}

    @abstractmethod
    async def execute(self, event_stage):
        pass


# Define the WorkflowStep command
class WorkflowStep(Command):
    async def execute(self, event_stage):
        if event_stage:
            event_stage['new_params'] = self.params

        event_stage = await workflow.execute_activity(
            self.activity,
            event_stage,
            start_to_close_timeout=timedelta(seconds=15),
        )
        return event_stage


@workflow.defn
class CustomWorkflow:
    @workflow.run
    async def run_workflow(self):
        event_stage = None

        # Define activities and their parameters
        activities_and_params = [
            (phase_1, {'amount': 1}),
            (phase_2, {'amount': 2}),
            (phase_3, {'amount': 3}),
            (phase_2, {'amount': 4}),
            (phase_2, {'amount': 5}),
            (phase_3, {'amount': 32})
        ]

        # Instantiate command objects for each step of the workflow
        workflow_commands = [WorkflowStep(activity, params) for activity, params in activities_and_params]

        # Run the workflow
        for command in workflow_commands:
            event_stage = await command.execute(event_stage)

        return event_stage



# @@@SNIPEND
