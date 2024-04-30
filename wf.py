# @@@SNIPSTART data-pipeline-run-worker-python
import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

# @@@SNIPSTART data-pipeline-your-workflow-python
from datetime import timedelta
from typing import Any, List

from temporalio import workflow

# with workflow.unsafe.imports_passed_through():
#     from activities import TemporalCommunityPost, post_ids, top_posts

from dataclasses import dataclass
from typing import Any, List

import aiohttp
from temporalio import activity


@dataclass
class TemporalCommunityPost:
    title: str
    url: str
    views: int


@activity.defn
async def f1() -> List[str]:
    async with aiohttp.ClientSession() as session:
        async with session.get("https://community.temporal.io/latest.json") as response:
            if not 200 <= int(response.status) < 300:
                raise RuntimeError(f"Status: {response.status}")
            post_ids = await response.json()

    return [str(topic["id"]) for topic in post_ids["topic_list"]["topics"]]


@activity.defn
async def f2(post_ids: List[str]) -> List[TemporalCommunityPost]:
    results: List[TemporalCommunityPost] = []
    async with aiohttp.ClientSession() as session:
        for item_id in post_ids:
            async with session.get(
                f"https://community.temporal.io/t/{item_id}.json"
            ) as response:
                if response.status < 200 or response.status >= 300:
                    raise RuntimeError(f"Status: {response.status}")
                item = await response.json()
                slug = item["slug"]
                url = f"https://community.temporal.io/t/{slug}/{item_id}"
                community_post = TemporalCommunityPost(
                    title=item["title"], url=url, views=item["views"]
                )
                results.append(community_post)
    results.sort(key=lambda x: x.views, reverse=True)
    top_ten = results[:10]
    return top_ten


@workflow.defn
class TemporalCommunityWorkflow:
    @workflow.run
    async def run(self) -> List[TemporalCommunityPost]:
        news_ids = await workflow.execute_activity(
            f1,
            start_to_close_timeout=timedelta(seconds=15)

        )

        print(news_ids)
        return await workflow.execute_activity(
            f2,
            news_ids,
            start_to_close_timeout=timedelta(seconds=15),
        )


TASK_QUEUE_NAME = "custom-task-name"


async def main():
    client = await Client.connect("10.3.95.62:7233")
    worker = Worker(
        client,
        task_queue=TASK_QUEUE_NAME,
        workflows=[TemporalCommunityWorkflow],
        activities=[f2, f1],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())