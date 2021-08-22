import asyncio
from datetime import datetime, timedelta, timezone
from defrag.modules.helpers.data_manipulation import find_first
from defrag.modules.helpers.sync_utils import as_async
from pottery.deque import RedisDeque
from defrag.modules.db.redis import RedisPool
from defrag import app
from defrag.modules.helpers import Query, QueryResponse
from defrag.modules.helpers.dispatcher import Dispatcher, Dispatchable, Notification, TelegramNotification
from typing import List, Optional, Tuple, Union
from dateutil import rrule
from pydantic.main import BaseModel
from defrag.modules.helpers.requests import Req

__MOD_NAME__ = "organizer"

fedocal_endpoint = ""
poll_interval = timedelta(days=15)
date_format = "%Y-%m-%d"
time_format = "%H:%M:%S"


# ----
# DATA
# ----


class FedocalMeeting(BaseModel):
    meeting_id: int
    meeting_name: str
    meeting_manager: str
    meeting_date: str
    meeting_date_end: str
    meeting_time_start: str
    meeting_time_stop: str
    meeting_timezone: str
    meeting_information: str
    meeting_location: str
    calendar_name: str


class CustomMeeting(BaseModel):

    class Rrule(BaseModel):

        class Freq(BaseModel):
            pass

        class Monthly(Freq):
            pass

        class Daily(Freq):
            pass

        class Hourly(Freq):
            pass

        class Minutely(Freq):
            pass

        freq: Freq
        dtstart = None  # target UTC datetime
        interval = 1
        wkst = None
        count = None
        until = None
        bysetpos = None
        bymonth = None
        bymonthday = None
        byyearday = None
        byeaster = None
        byweekno = None
        byweekday = None
        byhour = None
        byminute = None
        bysecond = None
        cache = False

    id: int
    title: str
    manager: str
    creator: str
    # as datetime.strftime with "%Y-%m-%d%H:%M:%S" from datetime.now(timezone.utc)
    created: str
    start: str  # as datetime.strftime with "%Y-%m-%d%H:%M:%S" from target UTC datetime
    end: str  # as datetime.strftime with "%Y-%m-%d%H:%M:%S" from target UTC datetime
    description: str
    location: str
    tags: List[str]
    restricted: List[str]
    rrule: Optional[Rrule]

# ---------
# UTILITIES
# ---------


def disassemble_to_date_time(d: datetime) -> Tuple[str, str]:
    date = d.strftime(date_format)
    time = d.strftime(time_format)
    return date, time


def assemble_to_datetime(d: str, t: str) -> datetime:
    return datetime.strptime(d+t, format=date_format+time_format)


def meeting_from_fedocal(m: FedocalMeeting) -> CustomMeeting:
    start = datetime.strftime(assemble_to_datetime(
        m.meeting_date, m.meeting_time_start), fmt=date_format+time_format)
    end = datetime.strftime(assemble_to_datetime(
        m.meeting_date_end, m.meeting_time_stop), fmt=date_format+time_format)
    return CustomMeeting(
        id=m.meeting_id,
        title=m.meeting_name,
        manager=m.meeting_manager,
        creator="defrag API",
        created=datetime.strftime(datetime.now(
            timezone.utc), fmt=date_format+time_format),
        start=start,
        end=end,
        description=m.meeting_information,
        location=m.meeting_information,
        tags=[],
        restricted=[],
        rrule=None
    )


def meeting_to_fedocal(m: CustomMeeting) -> FedocalMeeting:
    date, time_start = disassemble_to_date_time(
        datetime.strptime(m.start, format=date_format+time_format))
    date_end, time_stop = disassemble_to_date_time(
        datetime.strptime(m.end, format=date_format+time_format))
    return FedocalMeeting(
        meeting_id=m.id,
        meeting_name=m.title,
        meeting_manager=m.manager,
        meeting_date=date,
        meeting_date_end=date_end,
        meeting_time_start=time_start,
        meeting_time_stop=time_stop,
        meeting_time_zone="utc",
        meeting_information=m.description,
        meeting_location=m.location,
        calendar_name="defrag API"
    )


# ---------
# REMINDERS
# ---------


class UserDeltas(BaseModel):
    weeks: Optional[int]
    days: Optional[int]
    hours: Optional[int]
    minutes: Optional[int]

    def apply(self, tgt: datetime) -> List[float]:
        deltas = []
        if self.weeks:
            deltas.append((tgt - timedelta(weeks=self.weeks)).timestamp())
        if self.days:
            deltas.append((tgt - timedelta(days=self.days)).timestamp())
        if self.hours:
            deltas.append((tgt - timedelta(hours=self.hours)).timestamp())
        if self.minutes:
            deltas.append((tgt - timedelta(hours=self.minutes)).timestamp())
        return deltas


class PostReminder(UserDeltas):
    tgt: datetime
    notification: Notification
    deltas: UserDeltas


async def set_reminder(tgt: datetime, notification: Notification, user_deltas: UserDeltas) -> None:
    await Dispatcher.put(Dispatchable(origin="reminders", schedules=user_deltas.apply(tgt), notification=notification))

# --------
# CALENDAR
# --------


class Calendar:

    container = RedisDeque([], redis=RedisPool().connection,
                           key="community_calendar")

    @classmethod
    async def add(cls, m: CustomMeeting, n: Notification, d: UserDeltas) -> None:
        future_occurrences = []
        if m.rrule:
            future_occurrences = rrule(**m.rrule.dict())
        else:
            future_occurrences.append(datetime.strptime(
                m.start, format=date_format+time_format).timestamp())
        disp = Dispatchable(
            origin="Calendar.add",
            notification=n,
            foreign_key=m.id,
            schedules=[d.apply(o) for o in future_occurrences]
        )
        m_time = datetime.strptime(
            m.start, format=date_format+time_format).timestamp()

        def inserting(item):
            def successor(target, origin_time_timestamp):
                return datetime.strptime(target.start, format=date_format+time_format).timestamp() > origin_time_timestamp
            index = find_first(cls.container, successor, m_time)
            cls.container.insert(index, item)
        await asyncio.gather(Dispatcher.put(disp), as_async(inserting)(m.dict()))

    @classmethod
    async def remove(cls, meeting_id: int) -> None:
        def removing(index: int):
            # not sure about this, apparently it does not accept a plain `.delitem__(index)`
            cls.container.__delitem__(**{"index": index})
        index = find_first(cls.container, lambda target,
                           origin: target.id == origin, origin=meeting_id)
        await asyncio.gather(as_async(Dispatcher.unschedule)(meeting_id), as_async(removing)(index))

    @classmethod
    async def add_all_new_meetings(cls, n: Notification, d: UserDeltas, meetings: Optional[List[CustomMeeting]]) -> None:
        to_add = meetings or []
        if not to_add:
            fedocal_meetings = await cls.poll_fedocal()
            to_add = [meeting_from_fedocal(m) for m in fedocal_meetings if m.meeting_id not in [
                m["meeting_id"] for m in cls.container]]
        await asyncio.gather(*[cls.add(m, n, d) for m in to_add])

    @staticmethod
    async def poll_fedocal(interval=None) -> List[FedocalMeeting]:
        now = datetime.now()
        start, _ = disassemble_to_date_time(datetime.now())
        end, _ = disassemble_to_date_time(now + (interval or poll_interval))
        async with Req(fedocal_endpoint, params={"start": start, "end": end}) as response:
            res = await response.json()
            return [FedocalMeeting(**entry) for entry in res["meetings"]]

    @staticmethod
    async def set_reminders_from_fedocal(meetings: List[FedocalMeeting], user_deltas: UserDeltas) -> None:
        to_schedule = [Dispatchable(foreign_key=m.meeting_id, origin="openSUSE_fedocal", schedules=user_deltas.apply(
            assemble_to_datetime(m.meeting_date, m.meeting_time_start)), notification=TelegramNotification(body=m.meeting_information)) for m in meetings]
        await asyncio.gather(*[Dispatcher.put(m) for m in to_schedule])


# --------
# HANDLERS
# --------


@app.post(f"/{__MOD_NAME__}/add_reminder/")
async def post_reminder(reminder: PostReminder) -> QueryResponse:
    await set_reminder(reminder.tgt, reminder.notification, reminder.deltas)
    return QueryResponse(query=Query(service=__MOD_NAME__), message="Reminder(s) set!")


@app.post(f"/{__MOD_NAME__}/add_fedocal_meetings/")
async def post_fedocal_meetings(meetings: List[FedocalMeeting], user_deltas: UserDeltas, notification: Notification):
    query = Query(service=__MOD_NAME__)
    await Calendar.add_all_new_meetings(meetings=[meeting_from_fedocal(m) for m in meetings], n=notification, d=user_deltas)
    return QueryResponse(query=query, message="Meeting(s) added and reminder(s) set!")


@app.post(f"/{__MOD_NAME__}/add_meetings/")
async def post_meetings(meetings: List[CustomMeeting], user_deltas: UserDeltas, notification: Notification):
    query = Query(service=__MOD_NAME__)
    await Calendar.add_all_new_meetings(meetings=meetings, n=notification, d=user_deltas)
    return QueryResponse(query=query, message="Meeting(s) added and reminder(s) set!")


@app.post(f"/{__MOD_NAME__}/cancel_meeting/")
async def post_cancel_meeting(meeting_id: int):
    query = Query(service=__MOD_NAME__)
    await Calendar.remove(meeting_id)
    return QueryResponse(query=query, message="Meeting and reminder(s) cancelled.")
