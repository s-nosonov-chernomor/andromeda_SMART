from __future__ import annotations
from typing import Any, Dict, List


def _require_dict(obj: Any, path: str) -> Dict[str, Any]:
    if not isinstance(obj, dict):
        raise ValueError(f"{path}: должен быть объект")
    return obj


def _require_list(obj: Any, path: str) -> List[Any]:
    if not isinstance(obj, list):
        raise ValueError(f"{path}: должен быть массив")
    return obj


def _require_bool(v: Any, path: str) -> bool:
    if isinstance(v, bool):
        return v
    raise ValueError(f"{path}: должен быть bool")


def _require_int_ge0(v: Any, path: str) -> int:
    try:
        iv = int(v)
    except Exception:
        raise ValueError(f"{path}: должен быть целым числом")
    if iv < 0:
        raise ValueError(f"{path}: должен быть ≥ 0")
    return iv


def _require_str(v: Any, path: str, allow_empty: bool = True) -> str:
    if not isinstance(v, str):
        raise ValueError(f"{path}: должен быть строкой")
    if not allow_empty and not v.strip():
        raise ValueError(f"{path}: не должна быть пустой")
    return v


def _optional_num(v: Any, path: str) -> float | int | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    # допускаем строку-число
    try:
        fv = float(v)
        # если без дробной части — вернём int
        return int(fv) if fv.is_integer() else fv
    except Exception:
        raise ValueError(f"{path}: должно быть числом или пусто")


def _key_format_ok(key: str) -> bool:
    # ожидаем формат "<line>|<unit_id>|<param_name>"
    # line и name — любые непустые, unit_id — целое
    parts = key.split("|")
    if len(parts) != 3:
        return False
    line, uid, name = parts
    if not line or not name:
        return False
    try:
        int(uid)
    except Exception:
        return False
    return True


def validate_alerts_cfg(cfg: Dict[str, Any]) -> None:
    """
    Поднимает ValueError при первой найденной проблеме.
    Допущения:
      - Поля bot_token/chat_id/endpoint_url могут быть пустыми (UI уже предупреждает).
      - selected/exception.key должны ссылаться на существующие params.key в рамках потока.
    """
    root = _require_dict(cfg, "корень")
    flows = root.get("flows", [])
    flows = _require_list(flows, "flows")

    for fi, flow in enumerate(flows):
        fpath = f"flows[{fi}]"
        flow = _require_dict(flow, fpath)

        # name
        if "name" in flow:
            _require_str(flow["name"], f"{fpath}.name", allow_empty=False)
        else:
            raise ValueError(f"{fpath}.name: обязателен")

        # type
        t = flow.get("type", "telegram")
        t = _require_str(t, f"{fpath}.type", allow_empty=False).lower()
        if t not in ("telegram", "ronet"):
            raise ValueError(f"{fpath}.type: допустимые значения: telegram | ronet")

        # enabled
        if "enabled" in flow:
            _require_bool(flow["enabled"], f"{fpath}.enabled")

        # options
        opts = flow.get("options", {})
        opts = _require_dict(opts, f"{fpath}.options")
        if t == "telegram":
            # поддержка legacy: flows[i].telegram.{bot_token,chat_id}
            tele = opts.get("telegram") or flow.get("telegram") or {}
            tele = _require_dict(tele, f"{fpath}.options.telegram")
            if "bot_token" in tele:
                _require_str(tele["bot_token"], f"{fpath}.options.telegram.bot_token", allow_empty=True)
            if "chat_id" in tele:
                cid = tele["chat_id"]
                # допускаем число; внутри движка всё равно приводим к строке
                if not isinstance(cid, (str, int)):
                    raise ValueError(f"{fpath}.options.telegram.chat_id: должен быть строкой или числом")

        elif t == "ronet":
            rn = opts.get("ronet", {})
            rn = _require_dict(rn, f"{fpath}.options.ronet")
            if "endpoint_url" in rn:
                _require_str(rn["endpoint_url"], f"{fpath}.options.ronet.endpoint_url", allow_empty=True)

        # params
        params = _require_list(flow.get("params", []), f"{fpath}.params")
        keys_in_flow: set[str] = set()
        for pi, p in enumerate(params):
            ppath = f"{fpath}.params[{pi}]"
            p = _require_dict(p, ppath)

            key = p.get("key")
            if key is None:
                raise ValueError(f"{ppath}.key: обязателен")
            key = _require_str(key, f"{ppath}.key", allow_empty=False)
            if not _key_format_ok(key):
                raise ValueError(f"{ppath}.key: ожидается формат 'line|unit_id|param'")
            if key in keys_in_flow:
                raise ValueError(f"{ppath}.key: дубликат ключа в пределах потока")
            keys_in_flow.add(key)

            if "alias" in p:
                _require_str(p["alias"], f"{ppath}.alias", allow_empty=True)
            if "path" in p:
                _require_str(p["path"], f"{ppath}.path", allow_empty=True)

            _optional_num(p.get("nominal"), f"{ppath}.nominal")
            _optional_num(p.get("tolerance"), f"{ppath}.tolerance")

            if "ok_text" in p:
                _require_str(p["ok_text"], f"{ppath}.ok_text", allow_empty=True)
            if "alarm_text" in p:
                _require_str(p["alarm_text"], f"{ppath}.alarm_text", allow_empty=True)

        # section validator (events/intervals)
        def _validate_section(sec_name: str) -> None:
            spath = f"{fpath}.{sec_name}"
            sec = flow.get(sec_name, {})
            sec = _require_dict(sec, spath)

            if "group_window_s" in sec:
                _require_int_ge0(sec["group_window_s"], f"{spath}.group_window_s")

            sel = _require_list(sec.get("selected", []), f"{spath}.selected")
            for si, skey in enumerate(sel):
                skey = _require_str(skey, f"{spath}.selected[{si}]", allow_empty=False)
                if not _key_format_ok(skey):
                    raise ValueError(f"{spath}.selected[{si}]: ключ должен быть в формате 'line|unit_id|param'")
                if skey not in keys_in_flow:
                    raise ValueError(f"{spath}.selected[{si}]: ключ не найден среди params.key данного потока")

            exc = _require_list(sec.get("exceptions", []), f"{spath}.exceptions")
            for ei, ex in enumerate(exc):
                epath = f"{spath}.exceptions[{ei}]"
                ex = _require_dict(ex, epath)
                k = ex.get("key")
                if k is None:
                    raise ValueError(f"{epath}.key: обязателен")
                k = _require_str(k, f"{epath}.key", allow_empty=False)
                if not _key_format_ok(k):
                    raise ValueError(f"{epath}.key: ключ должен быть в формате 'line|unit_id|param'")
                if k not in keys_in_flow:
                    raise ValueError(f"{epath}.key: ключ не найден среди params.key данного потока")

                if "value" not in ex:
                    raise ValueError(f"{epath}.value: обязателен")
                v = ex["value"]
                # допускаем str|int|float|bool
                if not isinstance(v, (str, int, float, bool)):
                    raise ValueError(f"{epath}.value: должен быть строкой/числом/bool")

        _validate_section("events")
        _validate_section("intervals")


__all__ = ["validate_alerts_cfg"]
