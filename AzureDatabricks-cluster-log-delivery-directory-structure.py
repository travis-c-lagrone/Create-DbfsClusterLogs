from abc import ABC
from datetime import datetime, timedelta, timezone
from os import PathLike
from pathlib import Path
from re import compile, escape, Match, Pattern, VERBOSE
from typing import ClassVar, Final, Generator, Iterable, Literal, MutableSequence, Optional, TypeVar, Union


UTC: Final = timezone.utc
HOUR: Final = timedelta(hours=1)


driver_log_file_types: MutableSequence[type] = []
executor_log_file_types: MutableSequence[type] = []
eventlog_file_types: MutableSequence[type] = []
init_script_file_types: MutableSequence[type] = []

def driver(cls: type) -> type:
    driver_log_file_types.append(cls)
    return cls

def executor(cls: type) -> type:
    executor_log_file_types.append(cls)
    return cls

def eventlog(cls: type) -> type:
    eventlog_file_types.append(cls)
    return cls

def init_script(cls: type) -> type:
    init_script_file_types.append(cls)
    return cls


class LogFile(ABC):
    filename: ClassVar[Pattern]
    path: Path
    start: datetime

    @classmethod
    def validate_path(cls, path: Path) -> Union[Literal[False], Match]:
        return (path.exists()
                and not path.is_dir()
                and cls.filename.fullmatch(path.name))

@driver
class CompleteLog4j(LogFile):
    filename: Pattern = compile(r"""
        log4j-
        (?<year>\d{4})-
        (?<month>\d{2})-
        (?<day>\d{2})-
        (?<hour>\d{2})
        \.log
        \.gz
    """, VERBOSE)
    start: datetime

    def __init__(self, path: Path, start: datetime) -> None:
        self.path = path
        self.start = start

    @classmethod
    def try_parse(cls, path: PathLike) -> Optional["CompleteLog4j"]:
        path = Path(path)
        if (match := cls.validate_path(path)):
            dt_kwargs = [int(match[grp]) for grp in ("year", "month", "day", "hour")]
            start = datetime(**dt_kwargs, tzinfo=UTC)
            return cls(path, start)
        return None

@driver
class PartialLog4j(LogFile):  # TODO
    filename: ClassVar[Pattern] = compile(escape(r"log4j-active.log"))
    start: datetime

    def __init__(self, path: Path, start: datetime) -> None:
        self.path = path
        self.start = start

    @classmethod
    def try_parse(cls, path: PathLike) -> Optional["PartialLog4j"]:
        path = Path(path)
        if (match := cls.validate_path(path)):
            raise NotImplementedError("Datetime extraction from log entries")
            return cls(path)
        return None

@driver
@executor
class CompleteSparkStderr(LogFile):
    filename: Pattern = compile(r"""
        stderr--
        (?<year>\d{4})-
        (?<month>\d{2})-
        (?<day>\d{2})--
        (?<hour>\d{2})-
        (?<minute>\d{2})
    """, VERBOSE)
    start: datetime

    def __init__(self, path: Path, start: datetime) -> None:
        self.path = path
        self.start = start

    @classmethod
    def try_parse(cls, path: PathLike) -> Optional["CompleteSparkStderr"]:
        path = Path(path)
        if (match := cls.validate_path(path)):
            dt_kwargs = [int(match[grp]) for grp in ("year", "month", "day", "hour", "minute")]
            end = datetime(**dt_kwargs, tzinfo=UTC)
            start = end - HOUR
            return cls(path, start)
        return None

@driver
@executor
class PartialSparkStderr(LogFile):  # TODO
    filename: ClassVar[Pattern] = compile(escape(r"stderr"))
    start: datetime

    def __init__(self, path: Path, start: datetime) -> None:
        self.path = path
        self.start = datetime

    @classmethod
    def try_parse(cls, path: PathLike) -> Optional["PartialSparkStderr"]:
        path = Path(path)
        if (match := cls.validate_path(path)):
            raise NotImplementedError("Datetime extraction from log entries")
            return cls(path)
        return None

@driver
@executor
class CompleteSparkStdout(LogFile):
    filename: ClassVar[Pattern] = compile(r"""
        stdout--
        (?<year>\d{4})-
        (?<month>\d{2})-
        (?<day>\d{2})--
        (?<hour>\d{2})-
        (?<minute>\d{2})
    """, VERBOSE)
    start: datetime

    def __init__(self, path: Path, start: datetime) -> None:
        self.path = path
        self.start = start

    @classmethod
    def try_parse(cls, path: PathLike) -> Optional["CompleteSparkStdout"]:
        path = Path(path)
        if (match := cls.validate_path(path)):
            dt_kwargs = [int(match[grp]) for grp in ("year", "month", "day", "hour", "minute")]
            end = datetime(**dt_kwargs, tzinfo=UTC)
            start = end - HOUR
            return cls(path, start)

@driver
@executor
class PartialSparkStdout(LogFile):  # TODO
    filename: ClassVar[Pattern] = compile(escape(r"stdout"))
    start: datetime

    def __init__(self, path: Path, start: datetime) -> None:
        self.path = path
        self.start = start

    @classmethod
    def try_parse(cls, path: PathLike) -> Optional["PartialSparkStdout"]:
        path = Path(path)
        if (match := cls.validate_path(path)):
            raise NotImplementedError("Datetime extraction from log entries")
            return cls(path)
        return None


E = TypeVar("E")

def compress(it: Iterable[E]) -> Iterable[E]:
    return filter(None, it)

def first(it: Iterable[E]) -> E:
    return next(iter(it))

def coalesce(it: Iterable[E]) -> E:
    return first(compress(it))


def parse_driver(path: Path) -> Optional[LogFile]:
    return coalesce(cls.try_parse(path) for cls in driver_log_file_types)

def iter_driver(driver_dir: Path) -> Generator[LogFile]:
    files = (fp for fp in driver_dir.iterdir() if not fp.is_dir())
    yield from compress(map(parse_driver, files))
