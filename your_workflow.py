# @@@SNIPSTART data-pipeline-your-workflow-python
from datetime import timedelta
from typing import Any, List

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from activities import TemporalCommunityPost, post_ids, top_posts, phase_1, phase_2, phase_3

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
            self.activity.handle_phase,  # Using the phase handler's handle_phase method
            event_stage,
            start_to_close_timeout=timedelta(seconds=15),
        )
        return event_stage


@workflow.defn
class TemporalCommunityWorkflow:
    @workflow.run
    async def run(self) -> List[TemporalCommunityPost]:
        news_ids = await workflow.execute_activity(
            post_ids,
            start_to_close_timeout=timedelta(seconds=15)

        )

        return await workflow.execute_activity(
            top_posts,
            news_ids,
            start_to_close_timeout=timedelta(seconds=15),
        )


@workflow.defn
class CustomWorkflow:
    # @workflow.run
    async def run(self):
        event_stage = await workflow.execute_activity(
            phase_1,
            start_to_close_timeout=timedelta(seconds=15)
        )
        event_stage['new_params'] = {'amount': 81}

        event_stage = await workflow.execute_activity(
            phase_2,
            event_stage,
            start_to_close_timeout=timedelta(seconds=15),
        )
        event_stage['new_params'] = {'amount': 41}

        event_stage = await workflow.execute_activity(
            phase_2,  # Second call to phase_2
            event_stage,
            start_to_close_timeout=timedelta(seconds=15),
        )
        event_stage['new_params'] = {'amount': 22}

        event_stage = await workflow.execute_activity(
            phase_3,
            event_stage,
            start_to_close_timeout=timedelta(seconds=15),
        )

        return event_stage

    @workflow.run
    async def run_workflow(self):
        event_stage = None

        # Instantiate phase handlers
        phase1_handler = Phase1Handler()
        phase2_handler = Phase2Handler()
        phase3_handler = Phase3Handler()

        # Define the list of activities and parameters
        activities_and_params = [
            (phase1_handler, {}),
            (phase2_handler, {'amount': 41}),
            (phase2_handler, {'amount': 41}),
            (phase2_handler, {'amount': 313}),
            (phase2_handler, {'amount': 313}),
            (phase3_handler, {'amount': 81})
        ]

        # Instantiate the list of commands
        workflow_commands = [WorkflowStep(activity, params) for activity, params in activities_and_params]

        # Run the workflow
        for command in workflow_commands:
            event_stage = await command.execute(event_stage)

        return event_stage



# @@@SNIPEND
