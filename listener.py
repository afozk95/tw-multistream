from typing import Any, Callable, Dict, List, Literal, NewType, Optional, Tuple, TypeVar, Union
from dataclasses import dataclass
import datetime as dt
import logging
from queue import Queue
from threading import Thread
import pymongo
import requests
import tweepy


logger = logging.getLogger(__name__)
fh = logging.FileHandler("multistream.log", mode="w")
fh.setLevel(logging.NOTSET)
ch = logging.StreamHandler()
ch.setLevel(logging.NOTSET)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
fh.setFormatter(formatter)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)


LocationBoundingBox = NewType("LocationBoundingBox", Tuple[float, float, float, float])
T = TypeVar("T")


@dataclass
class Credential:
    consumer_key: str
    consumer_secret: str
    access_token: str
    access_token_secret: str
    bearer_token: Optional[str] = None

    def to_dict(self) -> Dict[str, str]:
        data = {
            "consumer_key": self.consumer_key,
            "consumer_secret": self.consumer_secret,
            "access_token": self.access_token,
            "access_token_secret": self.access_token_secret,
        }
        return data


class MongoStream(tweepy.Stream):
    def __init__(
        self,
        cred: Credential,
        stream_name: str,
        db_name: str = "twitter",
        status_collection_name: Optional[str] = "statuses",
        status_deletion_collection_name: Optional[str] = "status_deletions",
        scrub_geo_collection_name: Optional[str] = "status_deletions",
        status_withheld_collection_name: Optional[str] = "status_withhelds",
        user_withheld_collection_name: Optional[str] = "user_withhelds",
        is_queue: bool = False,
    ):
        super().__init__(**cred.to_dict())
        self.stream_name: str = stream_name
        self.status_collection_name: Optional[str] = status_collection_name
        self.status_deletion_collection_name: Optional[str] = status_deletion_collection_name
        self.scrub_geo_collection_name: Optional[str] = scrub_geo_collection_name
        self.status_withheld_collection_name: Optional[str] = status_withheld_collection_name
        self.user_withheld_collection_name: Optional[str] = user_withheld_collection_name
        self.db_name: str = db_name
        self.mongo_client: pymongo.MongoClient = pymongo.MongoClient()
        self.db: pymongo.database.Database = self.mongo_client[self.db_name]
        self.is_queue: bool = is_queue
        self.queue: Optional[Queue] = Queue() if is_queue else None
        self.init()
 
    def init(self) -> None:
        self.init_mongo_db()
        self.start_threads(4, self._thread_func)

    def init_mongo_db(self) -> None:
        if self.status_collection_name:
            self.db[self.status_collection_name].create_index([("id_str", pymongo.ASCENDING)], unique=True)

    def _thread_func(self, queue: Queue) -> None:
        if self.is_queue:
            while True:
                status = queue.get()
                self.insert_status_to_db(status)
                queue.task_done()

    def start_threads(self, num: int, func: Callable) -> None:
        if self.is_queue:
            for _ in range(num):
                t = Thread(target=func, args=(self.queue,))
                t.daemon = True
                t.start()
                logger.warning(f"(stream {self.stream_name}) worker thread ({t.name}) starts")

    def insert_status_to_db(self, status: tweepy.models.Status) -> None:
        try:
            data = {
                **status._json,
                "collected_at": dt.datetime.utcnow(),
            }
            self.db[self.status_collection_name].insert_one(data)
        # when multiple streams get the same status (status can satisfy conditions for multiple streams at once),
        # all except one tries to insert a status that already exists in the database, which causes an error
        # as `id_str` field must be unique.
        # simplest solution is catching the exception and ignoring it
        # one can also use the following (upsert = update if exists, otherwise insert):
        # self.db[self.status_collection_name].update({"id_str": status._json["id_str"]}, status._json}, upsert=True)
        except pymongo.errors.DuplicateKeyError:
            pass

    def on_closed(self, response: requests.Response) -> None:
        logger.warning(f"({self.stream_name}) stream connection closed by twitter, {response}")

    def on_connect(self) -> None:
        logger.warning(f"({self.stream_name}) stream started")

    def on_connection_error(self) -> None:
        logger.warning(f"({self.stream_name}) stream connection has errored or timed out")

    def on_disconnect(self) -> None:
        logger.warning(f"({self.stream_name}) stream disconnected")

    def on_exception(self, exception: Exception) -> None:
        logger.warning(f"({self.stream_name}) stream encountered an exception, {exception}")

    def on_keep_alive(self) -> None:
        logger.warning(f"({self.stream_name}) stream received keep-alive signal")

    def on_request_error(self, status_code: int) -> None:
        logger.warning(f"({self.stream_name}) stream encountered HTTP error, {status_code}")

    def on_status(self, status: tweepy.models.Status) -> None:
        if self.status_collection_name:
            if self.is_queue:
                self.queue.put(status)
            else:
                self.insert_status_to_db(status)

    def on_delete(self, status_id: int, user_id: int) -> None:
        logger.warning(f"({self.stream_name}) status deletion")
        if self.status_deletion_collection_name:
            deletion = {
                "collected_at": dt.datetime.utcnow(),
                "user_id": user_id,
                "status_is": status_id,
            }
            self.db[self.status_deletion_collection_name].insert_one(deletion)
 
    def on_disconnect_message(self, message: Dict[str, Any]) -> None:
        logger.warning(f"on_disconnect_message {type(message)}")
        logger.warning(f"({self.stream_name}) stream received disconnect message, {message}")

    def on_limit(self, track: int) -> None:
        logger.warning(f"({self.stream_name}) stream received limit notice, {track}")

    def on_scrub_geo(self, notice: Dict[str, Any]) -> None:
        logger.warning(f"on_scrub_geo {type(notice)}")
        if self.scrub_geo_collection_name:
            self.db[self.scrub_geo_collection_name].insert_one(notice)

    def on_status_withheld(self, notice: Dict[str, Any]) -> None:
        logger.warning(f"on_status_withheld {type(notice)}")
        if self.status_withheld_collection_name:
            self.db[self.status_withheld_collection_name].insert_one(notice)

    def on_user_withheld(self, notice: Dict[str, Any]) -> None:
        logger.warning(f"on_user_withheld {type(notice)}")
        if self.user_withheld_collection_name:
            self.db[self.user_withheld_collection_name].insert_one(notice)

    def on_warning(self, warning: Dict[str, Any]) -> None:
        logger.warning(f"on_warning {type(warning)}")
        logger.warning(f"({self.stream_name}) stream received stall warning, {warning}")


class MultiListener:
    MAX_TRACK_PER_STREAM = 400
    MAX_FOLLOW_PER_STREAM = 5000
    MAX_LOCATIONS_PER_STREAM = 25

    def __init__(self, creds: List[Credential]) -> None:
        self.creds: List[Credential] = creds
        self.stream_threads: List[Thread] = []

    def infer_parameter_name(
        self,
        parameter_name: Optional[str] = None,
        follow: Optional[List[str]] = None,
        track: Optional[List[str]] = None,
        locations: Optional[List[LocationBoundingBox]] = None,
    ) -> Literal["follow", "track", "locations"]:
        if parameter_name is None:
            if follow and len(follow) > self.MAX_FOLLOW_PER_STREAM:
                parameter_name = "follow"
            elif track and len(track) > self.MAX_TRACK_PER_STREAM:
                parameter_name = "track"
            elif locations and len(locations) > self.MAX_LOCATIONS_PER_STREAM:
                parameter_name = "locations"
            else:
                logger.warning("no parameter with length greater than max allowed supplied")
                parameter_name = "follow"
        elif parameter_name == "follow":
            if follow is None:
                logger.warning(f"parameter_name {parameter_name} has value {follow}")
            else:
                if len(follow) <= self.MAX_FOLLOW_PER_STREAM:
                    logger.warning(f"parameter_name {parameter_name} has length no greater than maximum allowed {self.MAX_FOLLOW_PER_STREAM}")
                else:
                    return parameter_name
        elif parameter_name == "track":
            if track is None:
                logger.warning(f"parameter_name {parameter_name} has value {track}")
            else:
                if len(track) <= self.MAX_TRACK_PER_STREAM:
                    logger.warning(f"parameter_name {parameter_name} has length no greater than maximum allowed {self.MAX_TRACK_PER_STREAM}")
                else:
                    return parameter_name
        elif parameter_name == "locations":
            if locations is None:
                logger.warning(f"parameter_name {parameter_name} has value {locations}")
            else:
                if len(locations) <= self.MAX_LOCATIONS_PER_STREAM:
                    logger.warning(f"parameter_name {parameter_name} has length no greater than maximum allowed {self.locations}")
                else:
                    return parameter_name
        else:
            raise ValueError(f"unknown parameter_name {parameter_name}")

        return parameter_name

    def create_stream(
        self,
        cred: Credential,
        stream_name: str,
        follow: Optional[List[str]],
        track: Optional[List[str]],
        locations: Optional[List[LocationBoundingBox]],
        languages: Optional[List[str]],
        stall_warnings: bool = False,
        filter_level: Optional[str] = None,
        is_queue_in_child_listeners: bool = False,
    ) -> None:
        stream = MongoStream(cred=cred, stream_name=stream_name, is_queue=is_queue_in_child_listeners)
        stream_thread = stream.filter(
            follow=follow,
            track=track,
            locations=locations,
            languages=languages,
            stall_warnings=stall_warnings,
            filter_level=filter_level,
            threaded=True,
        )
        self.stream_threads.append(stream_thread)

    def filter(
        self,
        parameter_name: Optional[Literal["follow", "track", "locations"]] = None,
        follow: Optional[Union[str, List[str]]] = None,
        track: Optional[Union[str, List[str]]] = None,
        locations: Optional[Union[LocationBoundingBox, List[LocationBoundingBox]]] = None,
        languages: Optional[Union[str, List[str]]] = None,
        stall_warnings: bool = False,
        filter_level: Optional[str] = None,
        is_queue_in_child_listeners: bool = False,
    ) -> None:
        if isinstance(follow, str):
            follow = [follow]
        if isinstance(track, str):
            track = [track]
        if isinstance(locations, type(LocationBoundingBox)):
            locations = [locations]
        if isinstance(languages, str):
            languages = [languages]

        if parameter_name is None:
            parameter_name = self.infer_parameter_name(parameter_name, follow, track, locations)

        parameters = self.handle_filter_parameters(parameter_name, follow, track, locations, languages)
        follow, track, locations, languages = tuple(parameters.values())
        if parameter_name == "follow":
            for i, (cred, f) in enumerate(zip(self.creds, follow), start=1):
                stream_name = f"stream_{i}"
                self.create_stream(
                    cred=cred,
                    stream_name=stream_name,
                    follow=f,
                    track=track,
                    locations=locations,
                    languages=languages,
                    stall_warnings=stall_warnings,
                    filter_level=filter_level,
                    is_queue_in_child_listeners=is_queue_in_child_listeners,
                )
        elif parameter_name == "track":
            for i, (cred, t) in enumerate(zip(self.creds, track), start=1):
                stream_name = f"stream_{i}"
                self.create_stream(
                    cred=cred,
                    stream_name=stream_name,
                    follow=follow,
                    track=t,
                    locations=locations,
                    languages=languages,
                    stall_warnings=stall_warnings,
                    filter_level=filter_level,
                    is_queue_in_child_listeners=is_queue_in_child_listeners,
                )
        elif parameter_name == "locations":
            for i, (cred, l) in enumerate(zip(self.creds, locations), start=1):
                stream_name = f"stream_{i}"
                self.create_stream(
                    cred=cred,
                    stream_name=stream_name,
                    follow=follow,
                    track=track,
                    locations=l,
                    languages=languages,
                    stall_warnings=stall_warnings,
                    filter_level=filter_level,
                    is_queue_in_child_listeners=is_queue_in_child_listeners,
                )
        else:
            raise ValueError(f"unknown parameter_name {parameter_name}")

    def handle_filter_parameters(
        self,
        parameter_name: Literal["follow", "track", "locations"],
        follow: Optional[List[str]] = None,
        track: Optional[List[str]] = None,
        locations: Optional[List[LocationBoundingBox]] = None,
        languages: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        if parameter_name == "follow":
            if follow is not None:
                follow = self.split_filter_parameters(follow, parameter_name)
                track = None
                locations = self.limit_filter_parameters(locations, parameter_name="locations") if locations else None
            else:
                raise TypeError(f"cannot multilisten with parameter_name {parameter_name}, since parameter value is {follow}")
        elif parameter_name == "track":
            if track is not None:
                follow = None
                track = self.split_filter_parameters(track, parameter_name)
                locations = self.limit_filter_parameters(locations, parameter_name="locations") if locations else None
            else:
                raise TypeError(f"cannot multilisten with parameter_name {parameter_name}, since parameter value is {track}")
        elif parameter_name == "locations":
            if locations is not None:
                follow = self.limit_filter_parameters(follow, parameter_name="follow") if follow else None
                track = self.limit_filter_parameters(track, parameter_name="track") if track else None
                locations = self.split_filter_parameters(locations, parameter_name)
            else:
                raise TypeError(f"cannot multilisten with parameter_name {parameter_name}, since parameter value is {locations}")
        else:
            raise ValueError(f"unknown parameter_name {parameter_name}")

        return {
            "follow": follow,
            "track": track,
            "locations": locations,
            "languages": languages,
        }

    def split_filter_parameters(self, values: List[T], parameter_name: Literal["follow", "track", "locations"]) -> List[List[T]]:
        def helper(values: List[T], max_num_of_groups: int, max_group_len: int) -> List[List[T]]:
            max_total_len = max_num_of_groups * max_group_len
            values = values[:max_total_len]
            groups = [values[n:n+max_group_len] for n in range(0, len(values), max_group_len)]
            return groups

        max_number_of_streams = len(self.creds)
        if parameter_name == "follow":
            return helper(values, max_number_of_streams, self.MAX_FOLLOW_PER_STREAM)
        elif parameter_name == "track":
            return helper(values, max_number_of_streams, self.MAX_TRACK_PER_STREAM)
        elif parameter_name == "locations":
            return helper(values, max_number_of_streams, self.MAX_LOCATIONS_PER_STREAM)
        else:
            raise ValueError(f"unknown parameter_name {parameter_name}")

    def limit_filter_parameters(self, values: List[T], parameter_name: Literal["follow", "track", "locations"]) -> List[T]:
        if parameter_name == "follow":
            return values[:self.MAX_FOLLOW_PER_STREAM]
        if parameter_name == "track":
            return values[:self.MAX_TRACK_PER_STREAM]
        if parameter_name == "locations":
            return values[:self.MAX_LOCATIONS_PER_STREAM]
        else:
            raise ValueError(f"unknown parameter_name {parameter_name}")
