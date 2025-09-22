# app/core/validate_andromeda.py
from __future__ import annotations
from typing import Dict, Any

def _req(d: dict, k: str, typ, path: str):
    if k not in d: raise ValueError(f"{path}.{k}: обязателен")
    v = d[k]
    if typ is bool and not isinstance(v, bool):
        raise ValueError(f"{path}.{k}: должен быть bool")
    if typ is int:
        try: int(v)
        except: raise ValueError(f"{path}.{k}: должен быть int")
    if typ is float:
        try: float(v)
        except: raise ValueError(f"{path}.{k}: должен быть float")
    if typ is str and not isinstance(v, str):
        raise ValueError(f"{path}.{k}: должен быть str")
    return v

def _as_int(v, path, mn=None, mx=None) -> int:
    try: iv = int(v)
    except: raise ValueError(f"{path}: ожидается int, получено {v!r}")
    if mn is not None and iv < mn: raise ValueError(f"{path}: должно быть ≥ {mn}")
    if mx is not None and iv > mx: raise ValueError(f"{path}: должно быть ≤ {mx}")
    return iv

def _as_float(v, path, mn=None) -> float:
    try: fv = float(v)
    except: raise ValueError(f"{path}: ожидается float, получено {v!r}")
    if mn is not None and fv < mn: raise ValueError(f"{path}: должно быть ≥ {mn}")
    return fv

def validate_andromeda_cfg(cfg: Dict[str, Any]) -> None:
    if not isinstance(cfg, dict):
        raise ValueError("корневой YAML должен быть объектом")
    agent = cfg.get("agent")
    if not isinstance(agent, dict):
        raise ValueError("agent: обязателен и должен быть объектом")

    # logger
    log = agent.get("logger", {})
    if not isinstance(log, dict): raise ValueError("agent.logger: объект")
    _as_int(_req(log, "level_log", int, "agent.logger"), "agent.logger.level_log", 1, 4)
    _as_int(_req(log, "max_size_log", int, "agent.logger"), "agent.logger.max_size_log", 1)
    _as_int(_req(log, "max_backups_log", int, "agent.logger"), "agent.logger.max_backups_log", 1)
    _as_int(_req(log, "max_age_log", int, "agent.logger"), "agent.logger.max_age_log", 1)
    if not isinstance(log.get("write_in_logfile", False), bool):
        raise ValueError("agent.logger.write_in_logfile: bool")
    if not isinstance(log.get("path_log", ""), str):
        raise ValueError("agent.logger.path_log: str")

    # buffer
    buf = agent.get("buffer", {})
    if not isinstance(buf, dict): raise ValueError("agent.buffer: объект")
    if not isinstance(buf.get("enabled", True), bool): raise ValueError("agent.buffer.enabled: bool")
    _as_int(_req(buf, "total_topic", int, "agent.buffer"), "agent.buffer.total_topic", 1000, 1_000_000)
    _as_int(_req(buf, "batch_size", int, "agent.buffer"), "agent.buffer.batch_size", 10, 2000)
    _as_int(_req(buf, "time_send_batch", int, "agent.buffer"), "agent.buffer.time_send_batch", 10, 5000)

    # mqtt
    mq = agent.get("mqtt", {})
    if not isinstance(mq, dict): raise ValueError("agent.mqtt: объект")
    cid = _req(mq, "controllerid", str, "agent.mqtt").strip()
    if not cid: raise ValueError("agent.mqtt.controllerid: не пустой")
    _as_int(_req(mq, "global_pub_time", int, "agent.mqtt"), "agent.mqtt.global_pub_time", 1)
    for k in ("publish_topic_pattern","subscribe_topic_pattern"):
        pat = _req(mq, k, str, "agent.mqtt")
        if "{controllerId}" not in pat and "{controllerid}" not in pat:
            raise ValueError(f"agent.mqtt.{k}: шаблон должен содержать {{controllerId}}")
    _as_int(_req(mq, "aperture_percent", int, "agent.mqtt"), "agent.mqtt.aperture_percent", 0, 100)
    _as_float(_req(mq, "aperture_threshold", float, "agent.mqtt"), "agent.mqtt.aperture_threshold", 0.0)

    # global_broker
    gb = agent.get("global_broker", {})
    if not isinstance(gb, dict): raise ValueError("agent.global_broker: объект")
    host = _req(gb, "host", str, "agent.global_broker")
    if not any(host.startswith(p) for p in ("ssl://", "tcp://", "unix://")):
        raise ValueError("agent.global_broker.host: ожидается префикс ssl:// или tcp:// или unix://")
    if not _req(gb, "client_id", str, "agent.global_broker").strip():
        raise ValueError("agent.global_broker.client_id: не пустой")
    _as_int(_req(gb, "keepalive", int, "agent.global_broker"), "agent.global_broker.keepalive", 1)
    for b in ("clean_session","autoreconnect"):
        if not isinstance(gb.get(b, True), bool):
            raise ValueError(f"agent.global_broker.{b}: bool")
    _as_int(_req(gb, "retry_interval", int, "agent.global_broker"), "agent.global_broker.retry_interval", 1)
    _as_int(_req(gb, "max_retries", int, "agent.global_broker"), "agent.global_broker.max_retries", 0)
    tls = gb.get("tls", {})
    if tls and not isinstance(tls, dict):
        raise ValueError("agent.global_broker.tls: объект")
    if tls and "ca_cert" in tls and not isinstance(tls["ca_cert"], str):
        raise ValueError("agent.global_broker.tls.ca_cert: str")

    # local_broker
    lb = agent.get("local_broker", {})
    if not isinstance(lb, dict): raise ValueError("agent.local_broker: объект")
    _req(lb, "host", str, "agent.local_broker")
    _req(lb, "client_id", str, "agent.local_broker")
    _as_int(_req(lb, "keepalive", int, "agent.local_broker"), "agent.local_broker.keepalive", 1)
    for b in ("clean_session","autoreconnect"):
        if not isinstance(lb.get(b, True), bool):
            raise ValueError(f"agent.local_broker.{b}: bool")
    _as_int(_req(lb, "retry_interval", int, "agent.local_broker"), "agent.local_broker.retry_interval", 1)
    _as_int(_req(lb, "max_retries", int, "agent.local_broker"), "agent.local_broker.max_retries", 0)

    wc = lb.get("wildcard", {})
    if wc:
        if not isinstance(wc, dict): raise ValueError("agent.local_broker.wildcard: объект")
        if not isinstance(wc.get("enabled", True), bool): raise ValueError("agent.local_broker.wildcard.enabled: bool")
        pats = wc.get("patterns", [])
        if not isinstance(pats, list) or not all(isinstance(s, str) and s for s in pats):
            raise ValueError("agent.local_broker.wildcard.patterns: массив непустых строк")

    tf = lb.get("topic_filters", {})
    if tf:
        if not isinstance(tf, dict): raise ValueError("agent.local_broker.topic_filters: объект")
        if not isinstance(tf.get("enabled", True), bool): raise ValueError("agent.local_broker.topic_filters.enabled: bool")
        ew = tf.get("ends_with", [])
        if ew and (not isinstance(ew, list) or not all(isinstance(s, str) and s for s in ew)):
            raise ValueError("agent.local_broker.topic_filters.ends_with: массив непустых строк")
