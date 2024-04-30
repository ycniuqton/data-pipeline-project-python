# @@@SNIPSTART data-pipeline-delete-schedule-python
import asyncio

from temporalio.client import Client


async def main():
    client = await Client.connect("10.3.95.62:7233")
    handle = client.get_schedule_handle(
        "top-stories-every-10-hours",
    )

    await handle.delete()


if __name__ == "__main__":
    asyncio.run(main())

# @@@SNIPEND
