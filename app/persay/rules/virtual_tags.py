# persay/rules/virtual_tags.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Optional
import threading

from .types import (
    TagQuality,
    TagSource,
    VirtualTagKind,
)


PublishFunc = Callable[[str, Any, Dict[str, Any]], None]
# сигнатура: publish_func(name, value, meta)


@dataclass
class VirtualTag:
    """
    Виртуальный тег — живёт в памяти движка.
    Может быть внутренним (только для логики) или публикуемым (в MQTT).
    """
    name: str
    value: Any = None
    ts: datetime = field(default_factory=datetime.utcnow)
    quality: TagQuality = TagQuality.GOOD
    source: TagSource = TagSource.VIRTUAL
    kind: VirtualTagKind = VirtualTagKind.INTERNAL
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_preview(self) -> str:
        return f"{self.name}={self.value} ({self.quality.value})"

    def update(
        self,
        value: Any,
        quality: Optional[TagQuality] = None,
        ts: Optional[datetime] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.value = value
        if quality is not None:
            self.quality = quality
        self.ts = ts or datetime.utcnow()
        if meta:
            # не перезатираем, а дополняем
            self.meta.update(meta)


class VirtualTagRegistry:
    """
    Простой in-memory реестр виртуальных тегов.

    Используем для:
    - хранения внутренних служебных значений правил;
    - хранения публикуемых "состояний правил" (rule.XYZ.state);
    - "буферных" значений, которые потом уйдут в MQTT.

    Если передать publish_func, то для тегов kind=PUBLISHABLE
    при обновлении будет вызван publish_func(name, value, meta).
    """
    def __init__(self, publish_func: Optional[PublishFunc] = None):
        self._tags: Dict[str, VirtualTag] = {}
        self._lock = threading.Lock()
        self._publish_func = publish_func

    # --- базовые операции -------------------------------------------------

    def create(
        self,
        name: str,
        *,
        kind: VirtualTagKind = VirtualTagKind.INTERNAL,
        initial_value: Any = None,
        quality: TagQuality = TagQuality.GOOD,
        meta: Optional[Dict[str, Any]] = None,
    ) -> VirtualTag:
        """
        Создать тег, если его нет.
        Если есть — вернём существующий.
        """
        with self._lock:
            if name in self._tags:
                return self._tags[name]

            tag = VirtualTag(
                name=name,
                value=initial_value,
                quality=quality,
                kind=kind,
                meta=meta or {},
            )
            self._tags[name] = tag
            return tag

    def get(self, name: str) -> Optional[VirtualTag]:
        with self._lock:
            return self._tags.get(name)

    def all(self) -> Dict[str, VirtualTag]:
        """
        Вернём копию словаря, чтобы снаружи не трогали оригинал.
        """
        with self._lock:
            return dict(self._tags)

    # --- обновление -------------------------------------------------------

    def update(
        self,
        name: str,
        value: Any,
        *,
        quality: Optional[TagQuality] = None,
        ts: Optional[datetime] = None,
        meta: Optional[Dict[str, Any]] = None,
        create_if_missing: bool = True,
    ) -> VirtualTag:
        """
        Обновить (или создать) тег и, если надо, опубликовать.
        """
        with self._lock:
            if name not in self._tags:
                if not create_if_missing:
                    raise KeyError(f"Virtual tag '{name}' not found")
                # создаём как INTERNAL по умолчанию
                self._tags[name] = VirtualTag(name=name)

            tag = self._tags[name]
            tag.update(value=value, quality=quality, ts=ts, meta=meta)

            # сохраним, а после выхода из lock — опубликуем
            publish_needed = (
                self._publish_func is not None
                and tag.kind == VirtualTagKind.PUBLISHABLE
            )
            tag_copy = None
            if publish_needed:
                # делаем копию, чтобы снаружи не держать линк на внутренний объект
                tag_copy = VirtualTag(
                    name=tag.name,
                    value=tag.value,
                    ts=tag.ts,
                    quality=tag.quality,
                    source=tag.source,
                    kind=tag.kind,
                    meta=dict(tag.meta),
                )

        # вне lock — публикуем
        if publish_needed and tag_copy is not None:
            self._publish_func(
                tag_copy.name,
                tag_copy.value,
                {
                    "ts": tag_copy.ts.isoformat(),
                    "quality": tag_copy.quality.value,
                    **tag_copy.meta,
                },
            )

        return tag

    # --- служебное --------------------------------------------------------

    def set_publish_func(self, publish_func: PublishFunc) -> None:
        """
        Позволяет задать / переопределить публикатор (например, когда
        MQTT-бус создаётся позже).
        """
        with self._lock:
            self._publish_func = publish_func

    def exists(self, name: str) -> bool:
        with self._lock:
            return name in self._tags

    def delete(self, name: str) -> None:
        with self._lock:
            if name in self._tags:
                del self._tags[name]
