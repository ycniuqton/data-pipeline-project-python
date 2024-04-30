# @@@SNIPSTART data-pipeline-run-worker-python
import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from activities import TASK_QUEUE_NAME, post_ids, top_posts, phase_1, phase_2, phase_3
# from your_workflow import TemporalCommunityWorkflow, CustomWorkflow
from wf2 import CustomWorkflow


async def main():
    client = await Client.connect("10.3.95.62:7233")
    # worker = Worker(
    #     client,
    #     task_queue=TASK_QUEUE_NAME,
    #     workflows=[TemporalCommunityWorkflow],
    #     activities=[top_posts, post_ids],
    # )
    # await worker.run()

    worker = Worker(
        client,
        task_queue=TASK_QUEUE_NAME,
        workflows=[CustomWorkflow],
        activities=[phase_3, phase_2, phase_1],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
# @@@SNIPEND
