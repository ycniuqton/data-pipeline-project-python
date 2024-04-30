# @@@SNIPSTART data-pipeline-run-workflow-python
import asyncio

import pandas as pd
from temporalio.client import Client

from activities import TASK_QUEUE_NAME
# from your_workflow import CustomWorkflow
from wf2 import CustomWorkflow
import uuid


def generate_uuid():
    return uuid.uuid4()


async def main():
    client = await Client.connect("10.3.95.62:7233")
    random_id = generate_uuid().__str__()
    print(random_id)
    stories = await client.execute_workflow(
        CustomWorkflow.run_workflow,
        id=random_id,
        task_queue=TASK_QUEUE_NAME,
    )
    [print(i) for i in stories.get('event_chain')]
    return stories

if __name__ == "__main__":
    asyncio.run(main())
# @@@SNIPEND
