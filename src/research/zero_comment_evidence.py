"""Fail-closed proof extraction for explicit zero reply counts in get_posts."""

from __future__ import annotations

from typing import Any

from aiotieba.api._protobuf.Post_pb2 import Post as PostProto
from aiotieba.api.get_posts.protobuf.PbPageResIdl_pb2 import PbPageResIdl
from google.protobuf.message import DecodeError

_DATA_DESCRIPTOR = PbPageResIdl.DESCRIPTOR.fields_by_name["data"].message_type
_FORUM_DESCRIPTOR = _DATA_DESCRIPTOR.fields_by_name["forum"].message_type
_PAGE_DESCRIPTOR = _DATA_DESCRIPTOR.fields_by_name["page"].message_type
_POST_DESCRIPTOR = _DATA_DESCRIPTOR.fields_by_name["post_list"].message_type
_THREAD_DESCRIPTOR = _DATA_DESCRIPTOR.fields_by_name["thread"].message_type

_DATA_FIELD = PbPageResIdl.DESCRIPTOR.fields_by_name["data"].number
_FORUM_FIELD = _DATA_DESCRIPTOR.fields_by_name["forum"].number
_PAGE_FIELD = _DATA_DESCRIPTOR.fields_by_name["page"].number
_POST_FIELD = _DATA_DESCRIPTOR.fields_by_name["post_list"].number
_THREAD_FIELD = _DATA_DESCRIPTOR.fields_by_name["thread"].number
_FORUM_ID_FIELD = _FORUM_DESCRIPTOR.fields_by_name["id"].number
_PAGE_NUMBER_FIELD = _PAGE_DESCRIPTOR.fields_by_name["current_page"].number
_POST_ID_FIELD = _POST_DESCRIPTOR.fields_by_name["id"].number
_POST_AUTHOR_FIELD = _POST_DESCRIPTOR.fields_by_name["author_id"].number
_POST_COUNT_FIELD = _POST_DESCRIPTOR.fields_by_name["sub_post_number"].number
_POST_TID_FIELD = _POST_DESCRIPTOR.fields_by_name["tid"].number
_POST_INLINE_FIELD = _POST_DESCRIPTOR.fields_by_name["sub_post_list"].number
_THREAD_ID_FIELD = _THREAD_DESCRIPTOR.fields_by_name["id"].number
_THREAD_FID_FIELD = _THREAD_DESCRIPTOR.fields_by_name["fid"].number
_COUNT_FIELD_PATH = "data.post_list.sub_post_number"


class _UnknownWireError(ValueError):
    """The body cannot safely prove an explicit zero."""


def _read_varint(data: bytes, offset: int) -> tuple[int, int]:
    value = 0
    for index in range(10):
        if offset >= len(data):
            raise _UnknownWireError("truncated varint")
        byte = data[offset]
        offset += 1
        if index == 9 and byte > 1:
            raise _UnknownWireError("varint exceeds 64 bits")
        value |= (byte & 0x7F) << (index * 7)
        if byte < 0x80:
            return value, offset
    raise _UnknownWireError("unterminated varint")


def _wire_fields(data: bytes) -> list[tuple[int, int, int | bytes | None]]:
    fields: list[tuple[int, int, int | bytes | None]] = []
    offset = 0
    while offset < len(data):
        tag, offset = _read_varint(data, offset)
        field_number = tag >> 3
        wire_type = tag & 7
        if field_number < 1 or field_number >= 1 << 29:
            raise _UnknownWireError("invalid field number")
        if wire_type == 0:
            value, offset = _read_varint(data, offset)
        elif wire_type == 1:
            if len(data) - offset < 8:
                raise _UnknownWireError("truncated fixed64")
            value = None
            offset += 8
        elif wire_type == 2:
            length, offset = _read_varint(data, offset)
            if length > len(data) - offset:
                raise _UnknownWireError("truncated length-delimited field")
            end = offset + length
            value = data[offset:end]
            offset = end
        elif wire_type == 5:
            if len(data) - offset < 4:
                raise _UnknownWireError("truncated fixed32")
            value = None
            offset += 4
        else:
            raise _UnknownWireError("unsupported wire type")
        fields.append((field_number, wire_type, value))
    return fields


def _field_values(
    fields: list[tuple[int, int, int | bytes | None]], field_number: int, wire_type: int
) -> list[int | bytes | None]:
    matches = [field for field in fields if field[0] == field_number]
    if any(field[1] != wire_type for field in matches):
        raise _UnknownWireError("unexpected wire type")
    return [field[2] for field in matches]


def _one_varint(fields: list[tuple[int, int, int | bytes | None]], field_number: int) -> int:
    values = _field_values(fields, field_number, 0)
    if len(values) != 1 or not isinstance(values[0], int):
        raise _UnknownWireError("missing or duplicate varint")
    return values[0]


def _one_message(fields: list[tuple[int, int, int | bytes | None]], field_number: int) -> bytes:
    values = _field_values(fields, field_number, 2)
    if len(values) != 1 or not isinstance(values[0], bytes):
        raise _UnknownWireError("missing or duplicate message")
    return values[0]


def _positive_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise _UnknownWireError("identity is not a positive integer")
    return value


def _identity(value: object, name: str) -> object:
    return getattr(value, name, None)


def extract_zero_comment_evidence(
    response_body: bytes | None,
    *,
    requested_tid: int,
    source_page: int,
    raw_page: object,
) -> dict[int, dict[str, object]]:
    """Return proof only for posts whose raw wire count explicitly encodes zero."""
    if not isinstance(response_body, bytes) or not response_body:
        return {}

    try:
        tid = _positive_int(requested_tid)
        page_number = _positive_int(source_page)

        response = PbPageResIdl()
        response.ParseFromString(response_body)
        if response.error.errorno:
            return {}

        response_fields = _wire_fields(response_body)
        data_wire = _one_message(response_fields, _DATA_FIELD)
        data_fields = _wire_fields(data_wire)
        forum_wire = _one_message(data_fields, _FORUM_FIELD)
        page_wire = _one_message(data_fields, _PAGE_FIELD)
        thread_wire = _one_message(data_fields, _THREAD_FIELD)
        post_wires = _field_values(data_fields, _POST_FIELD, 2)
        if any(not isinstance(post, bytes) for post in post_wires):
            return {}

        forum_id = _one_varint(_wire_fields(forum_wire), _FORUM_ID_FIELD)
        proto_page = _one_varint(_wire_fields(page_wire), _PAGE_NUMBER_FIELD)
        thread_fields = _wire_fields(thread_wire)
        thread_id = _one_varint(thread_fields, _THREAD_ID_FIELD)
        thread_fids = _field_values(thread_fields, _THREAD_FID_FIELD, 0)
        if len(thread_fids) > 1 or (thread_fids and not isinstance(thread_fids[0], int)):
            return {}
        thread_fid = thread_fids[0] if thread_fids else 0

        if (
            forum_id <= 0
            or thread_id != tid
            or proto_page != page_number
            or response.data.forum.id != forum_id
            or response.data.thread.id != thread_id
            or response.data.page.current_page != proto_page
        ):
            return {}

        raw_forum = _positive_int(_identity(_identity(raw_page, "forum"), "fid"))
        raw_thread = _identity(raw_page, "thread")
        raw_tid = _positive_int(_identity(raw_thread, "tid"))
        raw_thread_fid = _identity(raw_thread, "fid")
        raw_page_number = _positive_int(_identity(_identity(raw_page, "page"), "current_page"))
        if (
            raw_forum != forum_id
            or raw_tid != tid
            or raw_page_number != page_number
            or (thread_fid and thread_fid != forum_id)
            or (raw_thread_fid and raw_thread_fid != forum_id)
        ):
            return {}

        raw_posts = _identity(raw_page, "objs")
        if not isinstance(raw_posts, (list, tuple)):
            return {}

        raw_by_pid: dict[int, list[Any]] = {}
        for raw_post in raw_posts:
            pid_value = _identity(raw_post, "pid")
            if isinstance(pid_value, int) and not isinstance(pid_value, bool) and pid_value > 0:
                raw_by_pid.setdefault(pid_value, []).append(raw_post)

        wire_posts: dict[int, list[tuple[bytes, list[tuple[int, int, int | bytes | None]]]]] = {}
        for post_wire in post_wires:
            if not isinstance(post_wire, bytes):
                return {}
            post_fields = _wire_fields(post_wire)
            post_id = _one_varint(post_fields, _POST_ID_FIELD)
            if post_id > 0:
                wire_posts.setdefault(post_id, []).append((post_wire, post_fields))

        evidence: dict[int, dict[str, object]] = {}
        for pid, matches in raw_by_pid.items():
            if pid <= 0 or len(matches) != 1:
                continue
            wire_matches = wire_posts.get(pid, [])
            if len(wire_matches) != 1:
                continue

            raw_post = matches[0]
            post_wire, post_fields = wire_matches[0]
            author_id = _one_varint(post_fields, _POST_AUTHOR_FIELD)
            try:
                raw_post_pid = _positive_int(_identity(raw_post, "pid"))
                raw_post_tid = _positive_int(_identity(raw_post, "tid"))
                raw_post_fid = _positive_int(_identity(raw_post, "fid"))
                raw_author_id = _positive_int(_identity(raw_post, "author_id"))
                raw_user_id = _positive_int(_identity(_identity(raw_post, "user"), "user_id"))
            except _UnknownWireError:
                continue
            count_values = _field_values(post_fields, _POST_COUNT_FIELD, 0)
            post_tids = _field_values(post_fields, _POST_TID_FIELD, 0)
            inline_messages = _field_values(post_fields, _POST_INLINE_FIELD, 2)
            if (
                author_id <= 0
                or raw_post_pid != pid
                or raw_post_tid != tid
                or raw_post_fid != forum_id
                or author_id != raw_author_id
                or author_id != raw_user_id
                or len(count_values) != 1
                or count_values[0] != 0
                or len(post_tids) > 1
                or (post_tids and (not isinstance(post_tids[0], int) or post_tids[0] not in (0, tid)))
                or any(_wire_fields(message) for message in inline_messages if isinstance(message, bytes))
                or len(inline_messages) != sum(isinstance(message, bytes) for message in inline_messages)
                or _identity(raw_post, "reply_num") != 0
                or len(_identity(raw_post, "comments") or ()) != 0
            ):
                continue

            post_proto = PostProto()
            post_proto.ParseFromString(post_wire)
            if post_proto.sub_post_number != 0 or post_proto.sub_post_list.sub_post_list:
                continue

            evidence[pid] = {
                "tid": tid,
                "pid": pid,
                "fid": forum_id,
                "author_user_id": author_id,
                "reported_count": 0,
                "count_field_present": True,
                "source_api": "aiotieba.get_posts",
                "evidence_kind": "protobuf_wire_explicit_zero",
                "source_page": page_number,
                "count_field_path": _COUNT_FIELD_PATH,
                "count_wire_field_number": _POST_COUNT_FIELD,
            }
        return evidence
    except (DecodeError, _UnknownWireError, AttributeError, TypeError, OverflowError):
        return {}
