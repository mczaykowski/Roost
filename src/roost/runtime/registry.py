from __future__ import annotations

from dataclasses import dataclass
from importlib import metadata
from typing import Any, Callable, Dict, Iterable, Mapping, MutableMapping

from roost.runtime.engine import Engine

EngineFactory = Callable[..., Engine]


@dataclass(frozen=True)
class EngineInfo:
    engine_id: str
    source: str


class UnknownEngineError(KeyError):
    def __init__(self, engine_id: str, *, available: Iterable[str]):
        self.engine_id = engine_id
        self.available = tuple(sorted(set(available)))
        super().__init__(f"Unknown engine '{engine_id}'. Available: {', '.join(self.available) or '(none)'}")


class EngineLoadError(RuntimeError):
    def __init__(self, engine_id: str, *, source: str, cause: BaseException):
        self.engine_id = engine_id
        self.source = source
        self.cause = cause
        super().__init__(f"Failed to load engine '{engine_id}' from {source}: {cause}")


class EngineRegistry:
    """
    Lazy engine registry.

    Engines are discovered via Python entry points (group: 'roost.engines') and loaded on-demand.
    Each entry point must resolve to a callable factory: `(**kwargs) -> Engine`.
    """

    def __init__(self, *, loaders: Mapping[str, Callable[[], EngineFactory]], sources: Mapping[str, str]):
        self._loaders: Dict[str, Callable[[], EngineFactory]] = dict(loaders)
        self._sources: Dict[str, str] = dict(sources)
        self._factory_cache: MutableMapping[str, EngineFactory] = {}

    @classmethod
    def from_entry_points(cls, *, group: str = "roost.engines") -> "EngineRegistry":
        loaders: Dict[str, Callable[[], EngineFactory]] = {}
        sources: Dict[str, str] = {}

        eps = metadata.entry_points()
        selected = eps.select(group=group) if hasattr(eps, "select") else eps.get(group, [])
        for ep in selected:
            engine_id = str(ep.name)
            source = f"entrypoint:{group}:{ep.name} -> {ep.value}"

            def _loader(*, _ep=ep, _engine_id=engine_id, _source=source) -> EngineFactory:
                try:
                    obj = _ep.load()
                except Exception as e:  # pragma: no cover - exercised by integration
                    raise EngineLoadError(_engine_id, source=_source, cause=e) from e
                if not callable(obj):
                    raise TypeError(f"Engine factory for '{_engine_id}' is not callable: {obj!r}")
                return obj  # type: ignore[return-value]

            loaders[engine_id] = _loader
            sources[engine_id] = source

        return cls(loaders=loaders, sources=sources)

    @classmethod
    def from_factories(cls, factories: Mapping[str, EngineFactory], *, source: str = "inline") -> "EngineRegistry":
        loaders = {k: (lambda *, _f=f: _f) for k, f in factories.items()}
        sources = {k: source for k in factories.keys()}
        return cls(loaders=loaders, sources=sources)

    def engine_ids(self) -> list[str]:
        return sorted(self._loaders.keys())

    def info(self) -> list[EngineInfo]:
        return [EngineInfo(engine_id=eid, source=self._sources.get(eid, "unknown")) for eid in self.engine_ids()]

    def load_factory(self, engine_id: str) -> EngineFactory:
        if engine_id in self._factory_cache:
            return self._factory_cache[engine_id]
        loader = self._loaders.get(engine_id)
        if not loader:
            raise UnknownEngineError(engine_id, available=self._loaders.keys())
        factory = loader()
        self._factory_cache[engine_id] = factory
        return factory

    def create(self, engine_id: str, **kwargs: Any) -> Engine:
        factory = self.load_factory(engine_id)
        engine = factory(**kwargs)
        if not getattr(engine, "engine_id", None):
            raise TypeError(f"Engine instance from '{engine_id}' is missing 'engine_id'")
        return engine

