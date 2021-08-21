
from datetime import datetime
from defrag import LOGGER
from defrag.modules.db.redis import RedisPool
from defrag.modules.helpers.requests import Req
from pottery import RedisSet, RedisDict, RedisDeque
from pydantic import BaseModel
from typing import Any, Dict, List, Optional, Union
import asyncio


class Notification(BaseModel):
    body: str
    push: bool
    pull: bool


class EmailNotification(Notification):
    email_address: str
    email_object: str


class MatrixNotification(Notification):
    pass


class TelegramNotification(Notification):
    user_id: Optional[int]
    chat_id: Optional[int]
    bot_endpoint: Optional[str]


class Dispatchable(BaseModel):
    origin: str
    notification: Notification
    retries: int = 0
    schedules: List[float] = []
    foreign_key: Optional[Union[str, int]] = None


class HashedDispatchable(Dispatchable):

    id: Optional[int] = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.id = hash(str(self.foreign_key)+str(self.notification)) if self.foreign_key else hash(
            str(self.notification))
        self.schedules = sorted(self.schedules, reverse=True)


class Dispatcher:
    """
    - Clients can subscribe as pushees, pollers, or both (to implement).
    - Pushees should be ready to handle HTTP requests. Pollers are not required to be so.
    - Single queue.
    - Whenever a item on the queue has its handler time out, it is cached to the `scheduled` set.

    TODO:
    - Add functionality for subscribing clients.
    - Add confirmation for all new subscriptions, with the confirmation not being a response but a
    call back testing the pushee's endpoint.
    """

    process_q: asyncio.Queue
    scheduled = RedisDict({}, redis=RedisPool().connection,
                          key="scheduled_items")
    unscheduled_items_ids = RedisSet(
        [], redis=RedisPool().connection, key="unscheduled_items")
    subscribed_pushees = RedisSet(
        [], redis=RedisPool().connection, key="subscribed_pushees")
    subscribed_pollers = RedisSet(
        [], redis=RedisPool().connection, key="subscribed_pollers")
    due_for_polling_notifications = RedisDeque(
        [], redis=RedisPool().connection, key="due_for_polling_notifications")

    @classmethod
    async def run(cls) -> None:
        """ Initializes the queue and launch the two consumers. """
        cls.process_q = asyncio.Queue()
        asyncio.gather(cls.start_polling_process(), cls.start_ticking_clock())

    @classmethod
    async def put(cls, dispatchable: Union[Dispatchable, Dict[str, Any]]) -> None:
        """ Ensures that the input is a unique dispatchable and puts it into the queue. """
        item = HashedDispatchable(dispatchable).dict() if isinstance(
            dispatchable, Dispatchable) else dispatchable
        if not "id" in item:
            raise Exception("Cannot process items without id!")
        await cls.process_q.put(item)

    @classmethod
    async def start_polling_process(cls) -> None:
        """
        Dispatches the item just in case it is not a scheduled item. Otherwise 
        adds to the scheduled items if not there already
        """
        LOGGER.info("Started to poll the process queue.")
        while True:
            item = await cls.process_q.get()
            if not item["schedules"]:
                await cls.dispatch(item)
                cls.process_q.task_done()
            elif not item["id"] in cls.scheduled:
                cls.scheduled[item["id"]] = item

    @classmethod
    async def start_ticking_clock(cls, interval: int = 60) -> None:
        """ 
        Every minute, monitor the scheduled set for due items.
        Dispatch those found there. 
        """
        LOGGER.info("Started to monitor scheduled items")
        while True:
            await asyncio.sleep(interval)
            now_timestamp = datetime.now().timestamp()
            due = [i for i in cls.scheduled.values() if i["schedules"]
                   [-1] <= now_timestamp]
            await asyncio.gather(*[cls.dispatch(i) for i in due])

    @classmethod
    def unschedule(cls, item_id: str) -> None:
        """ Unschedule (marks for cancellation) a scheduled items. """
        found = [k for k in cls.scheduled.keys() if k == item_id]
        if found:
            cls.unscheduled_items_ids.add(item_id)

    @classmethod
    async def dispatch(cls, item: Dict[str, Any]) -> None:
        """
        The dispatcher looks up the 'unscheduled' set, to see if the item
        being processed is found there. 
        If found, the item is removed from the set and discarded. 
        If not found, the item is has its notification payload either added to a queue available for external applications to poll, or tried for push/sending.
        If the push/sending fails, the item is sent to the queue again unless it has been retried 3 times already (discarded if so). 
        If the sending succeeds, the item is rescheduled if it has remaining scheduled times. Otherwise it is removed from the the scheduled items.
        """
        if item["id"] in cls.unscheduled_items_ids:
            cls.unscheduled_items_ids.remove(item["id"])
            cls.scheduled.__delitem__(item["id"])
            return
        if item["notification"]["poll"]:
            cls.due_for_polling_notifications.appendleft(item["notification"])
        if item["notification"]["push"]:
            response = await cls.send(item["notification"])
            if response.status != 200:
                if item["retries"] < 3:
                    LOGGER.warning(f"item sending timed out. Retrying soon.")
                    item["retries"] += 1
                    await cls.put(item)
                    return
                else:
                    LOGGER.warning(
                        f"Dropping notification {item['notification']} after 3 unsuccessful retries: {item}")
        if item["schedules"]:
            item["schedules"].pop()
        if not item["schedules"]:
            cls.scheduled.__delitem__(item["id"])
        else:
            cls.scheduled[item["id"]] = item

    @staticmethod
    async def send(item: Dict[str, Any], testing: bool = True) -> Any:
        if not testing:
            if data := item["requests_options"]["data"]:
                async with Req(item["requests_options"]["url"], json=data) as response:
                    return response
        LOGGER.info(f"item sent! {str(item)} at {str(datetime.now())}")
        return {"status": 200}
