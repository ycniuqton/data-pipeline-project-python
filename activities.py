# @@@SNIPSTART data-pipeline-activity-python
from dataclasses import dataclass
from typing import Any, List

import aiohttp
from temporalio import activity
from temporalio import workflow


TASK_QUEUE_NAME = "123"


@dataclass
class TemporalCommunityPost:
    title: str
    url: str
    views: int


@dataclass
class TemporalEvent:
    last_event: dict
    event_chain: List[dict]
    new_params: dict


@activity.defn
async def post_ids() -> List[str]:
    async with aiohttp.ClientSession() as session:
        async with session.get("https://community.temporal.io/latest.json") as response:
            if not 200 <= int(response.status) < 300:
                raise RuntimeError(f"Status: {response.status}")
            post_ids = await response.json()

    return [str(topic["id"]) for topic in post_ids["topic_list"]["topics"]]


@activity.defn
async def top_posts(post_ids: List[str]) -> List[TemporalCommunityPost]:
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


@activity.defn
async def phase_1(temporal_event: TemporalEvent):
    this_event = {"function": "init", "amount": 1000, "value": 1000}
    result = TemporalEvent(last_event=this_event,
                           event_chain=[this_event],
                           new_params={'amount': 10})
    return result


@activity.defn
async def phase_2(temporal_event: TemporalEvent):
    last_event = temporal_event.last_event
    event_chain = temporal_event.event_chain
    param = temporal_event.new_params

    this_event = {"function": "add", "amount": param.get('amount', 0), "value": last_event.get('value')}
    current_value = last_event.get('value')

    if this_event.get('function') == 'add':
        current_value += this_event.get('amount')
        this_event['value'] = current_value

    event_chain.append(this_event)

    result = TemporalEvent(last_event=this_event,
                           event_chain=event_chain,
                           new_params={'amount': 10})
    return result


@activity.defn
async def phase_3(temporal_event: TemporalEvent):
    last_event = temporal_event.last_event
    event_chain = temporal_event.event_chain
    param = temporal_event.new_params

    this_event = {"function": "sub", "amount": param.get('amount', 0), "value": last_event.get('value')}
    current_value = last_event.get('value')

    if this_event.get('function') == 'sub':
        current_value -= this_event.get('amount')
        this_event['value'] = current_value

    event_chain.append(this_event)

    result = TemporalEvent(last_event=this_event,
                           event_chain=event_chain,
                           new_params={})
    return result


class TemporalEventHandler:
    @staticmethod
    def extract_params(new_params):
        return new_params

    @staticmethod
    @activity.defn
    async def handle_phase(temporal_event):
        pass

    @staticmethod
    def pack_result(last_event, event_chain, new_params):
        return TemporalEvent(last_event=last_event, event_chain=event_chain, new_params=new_params)


class Phase1Handler(TemporalEventHandler):
    @staticmethod
    @activity.defn
    async def handle_phase(temporal_event):
        new_params = Phase1Handler.extract_params(temporal_event.new_params)
        this_event = {"function": "init", "amount": new_params.get('amount', 0), "value": 1000}
        return Phase1Handler.pack_result(last_event=this_event, event_chain=[this_event], new_params={'amount': 10})


class Phase2Handler(TemporalEventHandler):
    @staticmethod
    @activity.defn
    async def handle_phase(temporal_event):
        last_event = temporal_event.last_event
        event_chain = temporal_event.event_chain
        param = Phase2Handler.extract_params(temporal_event.new_params)

        this_event = {"function": "add", "amount": param.get('amount', 0), "value": last_event.get('value')}
        current_value = last_event.get('value')

        if this_event.get('function') == 'add':
            current_value += this_event.get('amount')
            this_event['value'] = current_value

        event_chain.append(this_event)

        return Phase2Handler.pack_result(last_event=this_event, event_chain=event_chain, new_params={'amount': 10})


class Phase3Handler(TemporalEventHandler):
    @staticmethod
    @activity.defn
    async def handle_phase(temporal_event):
        last_event = temporal_event.last_event
        event_chain = temporal_event.event_chain
        param = Phase3Handler.extract_params(temporal_event.new_params)

        this_event = {"function": "sub", "amount": param.get('amount', 0), "value": last_event.get('value')}
        current_value = last_event.get('value')

        if this_event.get('function') == 'sub':
            current_value -= this_event.get('amount')
            this_event['value'] = current_value

        event_chain.append(this_event)

        return Phase3Handler.pack_result(last_event=this_event, event_chain=event_chain, new_params={})
