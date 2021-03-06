from abc import ABC
from datetime import datetime, timedelta, timezone
from os import PathLike
from pathlib import Path, PosixPath
from typing import (Any, ClassVar, FrozenSet, Iterable,
                    Iterator, Match, MutableSequence, Optional,
                    Pattern, Sequence, TypeVar, Union)
from warnings import warn

import json
import logging
import re


logger = logging.getLogger(__name__)

EMPTY_TUPLE: tuple = tuple()
EMPTY_SET: frozenset = frozenset()

class UniversalSet(frozenset):
    def __contains__(self, item: Optional[Any]) -> bool:
        return True
UNIVERSAL_SET: UniversalSet = UniversalSet()


UTC = timezone.utc
HOUR = timedelta(hours=1)
MINUTE = timedelta(minutes=1)

DATETIME_MATCH_GROUPS: Sequence[str] = [
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
]

def match_to_datetime(match: Match) -> datetime:
    D = match.groupdict()
    dt_kwargs = {grp: D.get(grp) for grp in DATETIME_MATCH_GROUPS if D.get(grp)}
    if "year" in dt_kwargs:
        if dt_kwargs["year"] == 2:
            dt_kwargs["year"] = datetime.strptime(dt_kwargs["year"], "%y").year
        else:
            dt_kwargs["year"] = int(dt_kwargs["year"], base=10)
    for k, v in dt_kwargs.items():
        if isinstance(v, str):
            dt_kwargs[k] = int(v, base=10)
    dt = datetime(**dt_kwargs, tzinfo=UTC)

    ofs = D.get("offset_hours")
    if ofs:
        dt -= timedelta(hours=ofs)
    return dt


class LogFile(Path, ABC):
    filename: ClassVar[Pattern]
    path: Path
    start: datetime

    def __init__(self, path: Path, start: datetime) -> None:
        logger.debug("LogFile(path=%r, start=%r)", path, start)
        self.path = path
        self.start = start

    @classmethod
    def validate_path(cls, path: Path) -> Union[bool, Match]:
        return (path.exists()
                and not path.is_dir()
                and cls.filename.fullmatch(path.name))

class InitScriptLogFile(LogFile, ABC):
    script_name: str

    def __init__(self, path: Path, start: datetime, script_name: str) -> None:
        logger.debug("InitScriptLogFile(path=%r, start=%r, script_name=%r)", path, start, script_name)
        self.path = path
        self.start = start
        self.script_name = script_name

E = TypeVar("E")

def compress(it: Iterable[E]) -> Iterable[E]:
    return filter(None, it)

def try_first(it: Iterable[E]) -> Optional[E]:
    try:
        return next(iter(it))
    except:
        return None

def try_coalesce(it: Iterable[E]) -> Optional[E]:
    return try_first(compress(it))

def iter_parsed(it: Iterable[Path], types: Sequence[type]) -> Iterator[LogFile]:
    for path in it:
        parsed = try_coalesce(cls.try_parse(path) for cls in types)
        if parsed:
            logger.debug("Parsed path %s as %s", path, type(parsed))
            yield parsed
        else:
            logger.debug("Failed to parse path %s as any of %r", path, [cls.__name__ for cls in types])

driver_log_file_types: MutableSequence[type] = []
executor_log_file_types: MutableSequence[type] = []
eventlog_file_types: MutableSequence[type] = []
init_script_file_types: MutableSequence[type] = []

def driver_log_file_type(cls: type) -> type:
    driver_log_file_types.append(cls)
    return cls

def executor_log_file_type(cls: type) -> type:
    executor_log_file_types.append(cls)
    return cls

def eventlog_log_file_type(cls: type) -> type:
    eventlog_file_types.append(cls)
    return cls

def init_script_log_file_type(cls: type) -> type:
    init_script_file_types.append(cls)
    return cls


@driver_log_file_type
class CompleteLog4j(LogFile):
    filename = re.compile(r"""
        log4j-
        (?P<year>\d{4})-
        (?P<month>\d{2})-
        (?P<day>\d{2})-
        (?P<hour>\d{2})
        \.log
        \.gz
    """, re.VERBOSE)

    @classmethod
    def try_parse(cls, path: PathLike) -> Optional["CompleteLog4j"]:
        path = Path(path)
        match = cls.validate_path(path)
        if match:
            start = match_to_datetime(match)
            return cls(path, start)
        return None

@driver_log_file_type
@executor_log_file_type
class CompleteSparkStderr(LogFile):
    filename = re.compile(r"""
        stderr--
        (?P<year>\d{4})-
        (?P<month>\d{2})-
        (?P<day>\d{2})--
        (?P<hour>\d{2})-
        (?P<minute>\d{2})
    """, re.VERBOSE)

    @classmethod
    def try_parse(cls, path: PathLike) -> Optional["CompleteSparkStderr"]:
        path = Path(path)
        match = cls.validate_path(path)
        if match:
            end = match_to_datetime(match)
            start = end - HOUR
            return cls(path, start)
        return None

@driver_log_file_type
@executor_log_file_type
class CompleteSparkStdout(LogFile):
    filename = re.compile(r"""
        stdout--
        (?P<year>\d{4})-
        (?P<month>\d{2})-
        (?P<day>\d{2})--
        (?P<hour>\d{2})-
        (?P<minute>\d{2})
    """, re.VERBOSE)

    @classmethod
    def try_parse(cls, path: PathLike) -> Optional["CompleteSparkStdout"]:
        path = Path(path)
        match = cls.validate_path(path)
        if match:
            end = match_to_datetime(match)
            start = end - HOUR
            return cls(path, start)

@eventlog_log_file_type
class CompleteEventlog(LogFile):
    filename = re.compile(r"""
        eventlog
        -
        (?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})
        --
        (?P<hour>\d{2})-(?P<minute>\d{2})
        \.gz
    """, re.VERBOSE)

    @classmethod
    def try_parse(cls, path: PathLike) -> Optional["CompleteEventlog"]:
        path = Path(path)
        match = cls.validate_path(path)
        if match:
            end = match_to_datetime(match)
            start = end - HOUR
            return cls(path, start)
        return None

@init_script_log_file_type
class InitScriptStderr(InitScriptLogFile):
    filename = re.compile(r"""
        (?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})
        _
        (?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})
        _
        (?P<offset_hours>\d{2})
        _
        (?P<script_name>.*)
        \.stderr\.log
    """, re.VERBOSE)

    @classmethod
    def try_parse(cls, path: PathLike) -> Optional["InitScriptStderr"]:
        path = Path(path)
        match = cls.validate_path(path)
        if match:
            end = match_to_datetime(match)
            start = end - HOUR
            return cls(path, start, match["script_name"])
        return None

@init_script_log_file_type
class InitScriptStdout(InitScriptLogFile):
    filename = re.compile(r"""
        (?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})
        _
        (?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2})
        _
        (?P<offset_hours>\d{2})
        _
        (?P<script_name>.*)
        \.stdout\.log
    """, re.VERBOSE)

    @classmethod
    def try_parse(cls, path: PathLike) -> Optional["InitScriptStdout"]:
        path = Path(path)
        match = cls.validate_path(path)
        if match:
            end = match_to_datetime(match)
            start = end - HOUR
            return cls(path, start, match["script_name"])
        return None


ENTRY_TIMESTAMP: Pattern = re.compile(r"""
    ^
    (?P<year>\d{2,4})[/-](?P<month>\d{2})[/-](?P<day>\d{2})
    [\sT]
    (?P<hour>\d{2})[:-](?P<minute>\d{2})[:-](?P<second>\d{2})[Zz]?
    \b
""", re.VERBOSE)

def find_first_entry_timestamp(partial_log_file: PathLike) -> Optional[datetime]:
    with open(partial_log_file, "rt") as f:
        for line in f:
            match = ENTRY_TIMESTAMP.match(line)
            if match:
                return match_to_datetime(match)
    return None

@driver_log_file_type
class PartialLog4j(LogFile):
    filename = re.compile(re.escape(r"log4j-active.log"))

    @classmethod
    def try_parse(cls, path: PathLike) -> Optional["PartialLog4j"]:
        path = Path(path)
        match = cls.validate_path(path)
        if match:
            start = find_first_entry_timestamp(path)
            if start:
                start.minute = 0
                start.second = 0
                return cls(path, start)
        return None

@driver_log_file_type
@executor_log_file_type
class PartialSparkStderr(LogFile):
    filename = re.compile(re.escape(r"stderr"))

    @classmethod
    def try_parse(cls, path: PathLike) -> Optional["PartialSparkStderr"]:
        path = Path(path)
        match = cls.validate_path(path)
        if match:
            start = find_first_entry_timestamp(path)
            if start:
                start.minute = 0
                start.second = 0
                return cls(path, start)
        return None

@driver_log_file_type
@executor_log_file_type
class PartialSparkStdout(LogFile):
    filename = re.compile(re.escape(r"stdout"))

    @classmethod
    def try_parse(cls, path: PathLike) -> Optional["PartialSparkStdout"]:
        path = Path(path)
        match = cls.validate_path(path)
        if match:
            start = find_first_entry_timestamp(path)
            if start:
                start.minute = 0
                start.second = 0
                return cls(path, start)
        return None

@eventlog_log_file_type
class PartialEventlog(LogFile):
    filename = re.compile(re.escape(r"eventlog"))

    @classmethod
    def try_parse(cls, path: PathLike) -> Optional["PartialEventlog"]:
        path = Path(path)
        match = cls.validate_path(path)
        if match:
            with open(path, "rt") as f:
                ts = json.loads(f.readline())["timestamp"] / 1000  # milliseconds to seconds
            start = datetime.fromtimestamp(ts, UTC)
            start.minute = 0
            start.second = 0
            return cls(path, start)
        return None


logger.info("driver_log_file_types: %s", ",".join(cls.__name__ for cls in driver_log_file_types))
logger.info("executor_log_file_types: %s", ",".join(cls.__name__ for cls in executor_log_file_types))
logger.info("eventlog_file_types: %s", ",".join(cls.__name__ for cls in eventlog_file_types))
logger.info("init_script_file_types: %s", ",".join(cls.__name__ for cls in init_script_file_types))


class ClusterLogDeliveryDir(PosixPath):
    def __iter__(self) -> Iterator["ClusterDir"]:
        items = self.iterdir()
        dirs = filter(Path.is_dir, items)
        cluster_dirs = map(self.ClusterDir, dirs)
        return cluster_dirs

    class ClusterDir(PosixPath):
        @property
        def id(self) -> str:
            """Databricks cluster id."""
            return self.name

        @property
        def driver(self) -> Optional["DriverDir"]:
            path = self / "driver"
            return self.DriverDir(path) if path.exists() else None

        @property
        def eventlog(self) -> Optional["EventLogsDir"]:
            path = self / "eventlog"
            return self.EventLogsDir(path) if path.exists() else None

        @property
        def executor(self) -> Optional["ExecutorsDir"]:
            path = self / "executor"
            return self.ExecutorsDir(path) if path.exists() else None

        @property
        def init_scripts(self) -> Optional["InitScriptsDir"]:
            path = self / "init_scripts"
            return self.InitScriptsDir(path) if path.exists() else None

        class DriverDir(PosixPath):
            def __iter__(self) -> Iterator[LogFile]:
                items = self.iterdir()
                files = (p for p in items if not p.is_dir())
                log_files = iter_parsed(files, driver_log_file_types)
                return log_files

        class EventLogsDir(PosixPath):
            def __iter__(self) -> Iterator["EventLogSparkContextDir"]:
                items = self.iterdir()
                dirs = filter(Path.is_dir, items)
                eventlog_spark_context_dirs = map(self.EventLogSparkContextDir, dirs)
                return eventlog_spark_context_dirs

            class EventLogSparkContextDir(PosixPath):
                    @property
                    def id(self) -> str:
                        """Spark context id."""
                        return self.name

                    def __iter__(self) -> Iterator["EventLogSparkSessionDir"]:
                        items = self.iterdir()
                        dirs = filter(Path.is_dir, items)
                        eventlog_spark_session_dirs = map(self.EventLogSparkSessionDir, dirs)
                        return eventlog_spark_session_dirs

                    class EventLogSparkSessionDir(PosixPath):
                        @property
                        def id(self) -> str:
                            """Spark session id."""
                            return self.name

                        def __iter__(self) -> Iterator[LogFile]:
                            items = self.iterdir()
                            files = (p for p in items if not p.is_dir())
                            log_files = iter_parsed(files, eventlog_file_types)
                            return log_files

        class ExecutorsDir(PosixPath):
            def __iter__(self) -> Iterator["ExecutorSparkAppDir"]:
                items = self.iterdir()
                dirs = filter(Path.is_dir, items)
                executor_spark_app_dirs = map(self.ExecutorSparkAppDir, dirs)
                return executor_spark_app_dirs

            class ExecutorSparkAppDir(PosixPath):
                @property
                def id(self) -> str:
                    """Spark application id."""
                    return self.name

                def __iter__(self) -> Iterator["ExecutorDir"]:
                    items = self.iterdir()
                    dirs = filter(Path.is_dir, items)
                    executor_dirs = map(self.ExecutorDir, dirs)
                    return executor_dirs

                class ExecutorDir(PosixPath):
                    @property
                    def id(self) -> str:
                        """Spark executor id."""
                        return self.name

                    def __iter__(self) -> Iterator[LogFile]:
                        items = self.iterdir()
                        files = (p for p in items if not p.is_dir())
                        log_files = iter_parsed(files, executor_log_file_types)
                        return log_files

        class InitScriptsDir(PosixPath):
            def __iter__(self) -> Iterator["SparkContextDir"]:
                items = self.iterdir()
                dirs = filter(Path.is_dir, items)
                spark_context_dirs = map(self.SparkContextDir, dirs)
                return spark_context_dirs

            class SparkContextDir(PosixPath):
                @property
                def id(self) -> str:
                    """Spark context id."""
                    return self.name

                def __iter__(self) -> Iterator[LogFile]:
                    items = self.iterdir()
                    files = (p for p in items if not p.is_dir())
                    log_files = iter_parsed(files, init_script_file_types)
                    return log_files


def convert_to_fuse_path(path: PathLike) -> Path:
    spath = str(path)
    if spath.startswith("dbfs:"):
        spath = spath[5:]
    if not spath.startswith("/"):
        spath = "/" + spath
    if not spath.startswith("/dbfs"):
        spath = "/dbfs" + spath
    return Path(spath)

def convert_to_frozenset_of_str(strings: Optional[Union[str, Iterable[str]]]) -> FrozenSet[str]:
    if strings is None:
        return UNIVERSAL_SET
    if isinstance(strings, str):
        strings = strings,
    return frozenset(strings)

class DateTimeRange:
    start: Optional[datetime]
    end: Optional[datetime]

    def __init__(self, start: Optional[datetime]=None, end: Optional[datetime]=None) -> None:
        self.start = start
        self.end = end

    def __contains__(self, dt: datetime) -> bool:
        if self.start and self.end:
            return self.start <= dt < self.end
        elif self.start:
            return self.start <= dt
        elif self.end:
            return dt < self.end
        else:
            return True

def find_log_files(cluster_log_delivery_path: PathLike,
                   *,
                   driver_logs: bool=True,
                   event_logs: bool=True,
                   executor_logs: bool=True,
                   init_script_logs: bool=True,
                   after: datetime=None,
                   before: datetime=None,
                   cluster_id: Optional[Union[str, Iterable[str]]]=None,
                   spark_context_id: Optional[Union[str, Iterable[str]]]=None,
                   spark_session_id: Optional[Union[str, Iterable[str]]]=None,
                   spark_app_id: Optional[Union[str, Iterable[str]]]=None,
                   spark_executor_id: Optional[Union[str, Iterable[str]]]=None,
) -> Iterator[LogFile]:
    path = convert_to_fuse_path(cluster_log_delivery_path)
    if not path.exists():
        raise FileNotFoundError(path)
    if not path.is_dir():
        raise NotADirectoryError(path)
    cluster_logs = ClusterLogDeliveryDir(path)

    after__before = DateTimeRange(after, before)

    cluster_ids = convert_to_frozenset_of_str(cluster_id)
    spark_context_ids = convert_to_frozenset_of_str(spark_context_id)
    spark_session_ids = convert_to_frozenset_of_str(spark_session_id)
    spark_app_ids = convert_to_frozenset_of_str(spark_app_id)
    spark_executor_ids = convert_to_frozenset_of_str(spark_executor_id)

    logger.debug("Traversing %s", cluster_logs)
    for cluster in (c for c in cluster_logs if c.id in cluster_ids):
        logger.debug("Traversing %s", cluster)
        if driver_logs and cluster.driver:
            logger.debug("Traversing %s", cluster.driver)
            for log in (l for l in cluster.driver if l.start in after__before):
                logger.debug("Yielding %s", log)
                yield log
        if event_logs and cluster.eventlog:
            logger.debug("Traversing %s", cluster.eventlog)
            for spark_context in (sc for sc in cluster.eventlog if sc.id in spark_context_ids):
                logger.debug("Traversing %s", spark_context)
                for spark_session in (ss for ss in spark_context if ss.id in spark_session_ids):
                    logger.debug("Traversing %s", spark_session)
                    for log in (l for l in spark_session if l.start in after__before):
                        logger.debug("Yielding %s", log)
                        yield log
        if executor_logs and cluster.executor:
            logger.debug("Traversing %s", cluster.executor)
            for spark_app in (sa for sa in cluster.executor if sa.id in spark_app_ids):
                logger.debug("Traversing %s", spark_app)
                for spark_executor in (e for e in spark_app if e.id in spark_executor_ids):
                    logger.debug("Traversing %s", spark_executor)
                    for log in (l for l in spark_executor if l.start in after__before):
                        logger.debug("Yielding %s", log)
                        yield log
        if init_script_logs and cluster.init_scripts:
            logger.debug("Traversing %s", cluster.init_scripts)
            for spark_context in (sc for sc in cluster.init_scripts if sc.id in spark_context_ids):
                logger.debug("Traversing %s", spark_context)
                for log in (l for l in spark_context if l.start in after__before):
                    logger.debug("Yielding %s", log)
                    yield log

_dir = [find_log_files.__name__]


try:
    from IPython.display import FileLink
    from shutil import copyfile
    from tempfile import TemporaryDirectory
    from zipfile import ZipFile

    def export_log_files(cluster_log_delivery_path: PathLike,
                         *,
                         driver_logs: bool=True,
                         event_logs: bool=True,
                         executor_logs: bool=True,
                         init_script_logs: bool=True,
                         after: datetime=None,
                         before: datetime=None,
                         cluster_id: Optional[Union[str, Iterable[str]]]=None,
                         spark_context_id: Optional[Union[str, Iterable[str]]]=None,
                         spark_session_id: Optional[Union[str, Iterable[str]]]=None,
                         spark_app_id: Optional[Union[str, Iterable[str]]]=None,
                         spark_executor_id: Optional[Union[str, Iterable[str]]]=None,
    ) -> FileLink:  # has attr `_repr_html_` that is implicitly displayable in Databricks notebooks
        find_kwargs = locals()
        logdirp = cluster_log_delivery_path = convert_to_fuse_path(cluster_log_delivery_path)
        with TemporaryDirectory() as tmpdir:  # local to the driver node by necessity
            tmpdirp = Path(tmpdir)
            arcdirp = tmpdirp / logdirp.name
            arcdirp.mkdir()
            arcfilep = arcdirp.with_suffix(".zip")
            with ZipFile(arcfilep, "w") as zf:
                for oldlogp in find_log_files(**find_kwargs):
                    arcname = oldlogp.relative_to(logdirp)
                    newlogp = arcdirp / arcname
                    copyfile(str(oldlogp), str(newlogp))
                    zf.write(newlogp, arcname)
            dbfs_arcfilep = Path("/dbfs/tmp/") / f"{arcfilep.stem}_{datetime.now(UTC):%Y%m%dT%H%M%SZ}{arcfilep.suffix}"
            copyfile(str(arcfilep), str(dbfs_arcfilep))
        return FileLink(dbfs_arcfilep)

    _dir.append(export_log_files.__name__)
except ModuleNotFoundError as err:
    logger.warning("%s", err)
    warn(str(err), ImportWarning)
    warn(f"Could not define export_log_files: {err}")


def __dir__() -> Sequence[str]:
    return _dir
