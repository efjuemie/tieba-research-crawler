from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

from aiotieba.api._protobuf.Post_pb2 import Post as PostProto
from aiotieba.api.get_posts.protobuf.PbPageResIdl_pb2 import PbPageResIdl
from aiotieba.core.net import NetCore
from yarl import URL

from src.research import client as research_client_module
from src.research.client import ResearchClient, ResearchPage, _GetPostsResponseCapture
from src.research.zero_comment_evidence import extract_zero_comment_evidence

_TID = 101
_FID = 202
_PID = 303
_UID = 404
_PAGE = 2
_COUNT_FIELD = PostProto.DESCRIPTOR.fields_by_name["sub_post_number"].number


def _varint(value: int) -> bytes:
    encoded = bytearray()
    while value > 0x7F:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _message_field(field_number: int, value: bytes) -> bytes:
    return _varint((field_number << 3) | 2) + _varint(len(value)) + value


def _fixture(
    *,
    count_values: tuple[int, ...] | None = (0,),
    inline_comment: bool = False,
    duplicate_raw_post: bool = False,
    duplicate_wire_post: bool = False,
) -> tuple[bytes, object]:
    response = PbPageResIdl()
    response.data.forum.id = _FID
    response.data.page.current_page = _PAGE
    response.data.thread.id = _TID
    response.data.thread.fid = _FID

    proto_post = PostProto(id=_PID, author_id=_UID, tid=_TID)
    if inline_comment:
        proto_post.sub_post_list.sub_post_list.add().id = 909
    post_wire = proto_post.SerializeToString()
    if count_values is not None:
        post_wire += b"".join(_varint((_COUNT_FIELD << 3) | 0) + _varint(value) for value in count_values)

    wire_posts = _message_field(6, post_wire) * (2 if duplicate_wire_post else 1)
    body = _message_field(2, response.data.SerializeToString() + wire_posts)
    raw_post = SimpleNamespace(
        pid=_PID,
        tid=_TID,
        fid=_FID,
        author_id=_UID,
        user=SimpleNamespace(user_id=_UID),
        reply_num=0,
        comments=[],
    )
    raw_page = SimpleNamespace(
        thread=SimpleNamespace(tid=_TID, fid=_FID),
        forum=SimpleNamespace(fid=_FID),
        page=SimpleNamespace(current_page=_PAGE),
        objs=[raw_post, SimpleNamespace(**raw_post.__dict__)] if duplicate_raw_post else [raw_post],
    )
    return body, raw_page


def test_research_page_keeps_four_argument_construction_and_defaults_evidence_empty() -> None:
    page = ResearchPage(object(), object(), datetime.now(UTC), "aiotieba.get_posts")

    assert page.zero_comment_evidence == {}


def test_explicit_wire_zero_produces_exact_identity_evidence() -> None:
    body, raw_page = _fixture()

    evidence = extract_zero_comment_evidence(
        body,
        requested_tid=_TID,
        source_page=_PAGE,
        raw_page=raw_page,
    )

    assert evidence == {
        _PID: {
            "tid": _TID,
            "pid": _PID,
            "fid": _FID,
            "author_user_id": _UID,
            "reported_count": 0,
            "count_field_present": True,
            "source_api": "aiotieba.get_posts",
            "evidence_kind": "protobuf_wire_explicit_zero",
            "source_page": _PAGE,
            "count_field_path": "data.post_list.sub_post_number",
            "count_wire_field_number": _COUNT_FIELD,
        }
    }


def test_omitted_count_field_is_not_treated_as_zero() -> None:
    body, raw_page = _fixture(count_values=None)

    assert extract_zero_comment_evidence(
        body,
        requested_tid=_TID,
        source_page=_PAGE,
        raw_page=raw_page,
    ) == {}


def test_positive_or_duplicate_count_field_is_not_evidence() -> None:
    positive_body, raw_page = _fixture(count_values=(2,))
    duplicate_zero_body, _ = _fixture(count_values=(0, 0))

    assert extract_zero_comment_evidence(
        positive_body, requested_tid=_TID, source_page=_PAGE, raw_page=raw_page
    ) == {}
    assert extract_zero_comment_evidence(
        duplicate_zero_body, requested_tid=_TID, source_page=_PAGE, raw_page=raw_page
    ) == {}


def test_inline_comment_and_identity_mismatch_reject_evidence() -> None:
    body_with_comment, raw_page = _fixture(inline_comment=True)
    body, _ = _fixture()

    assert extract_zero_comment_evidence(
        body_with_comment, requested_tid=_TID, source_page=_PAGE, raw_page=raw_page
    ) == {}
    assert extract_zero_comment_evidence(
        body, requested_tid=_TID + 1, source_page=_PAGE, raw_page=raw_page
    ) == {}
    assert extract_zero_comment_evidence(
        body, requested_tid=_TID, source_page=_PAGE + 1, raw_page=raw_page
    ) == {}


def test_raw_post_tid_fid_and_author_ids_must_match_exactly() -> None:
    body, raw_page = _fixture()
    raw_page.objs[0].tid = _TID + 1
    assert extract_zero_comment_evidence(
        body, requested_tid=_TID, source_page=_PAGE, raw_page=raw_page
    ) == {}

    body, raw_page = _fixture()
    raw_page.objs[0].fid = _FID + 1
    assert extract_zero_comment_evidence(
        body, requested_tid=_TID, source_page=_PAGE, raw_page=raw_page
    ) == {}

    body, raw_page = _fixture()
    raw_page.objs[0].author_id = _UID + 1
    assert extract_zero_comment_evidence(
        body, requested_tid=_TID, source_page=_PAGE, raw_page=raw_page
    ) == {}

    body, raw_page = _fixture()
    raw_page.objs[0].user.user_id = _UID + 1
    assert extract_zero_comment_evidence(
        body, requested_tid=_TID, source_page=_PAGE, raw_page=raw_page
    ) == {}


def test_duplicate_raw_or_wire_pid_rejects_evidence() -> None:
    body, duplicate_raw_page = _fixture(duplicate_raw_post=True)
    assert extract_zero_comment_evidence(
        body, requested_tid=_TID, source_page=_PAGE, raw_page=duplicate_raw_page
    ) == {}

    duplicate_wire_body, raw_page = _fixture(duplicate_wire_post=True)
    assert extract_zero_comment_evidence(
        duplicate_wire_body, requested_tid=_TID, source_page=_PAGE, raw_page=raw_page
    ) == {}


def test_malformed_truncated_wire_returns_unknown() -> None:
    body, raw_page = _fixture()

    assert extract_zero_comment_evidence(
        body[:-1], requested_tid=_TID, source_page=_PAGE, raw_page=raw_page
    ) == {}


def test_instance_capture_is_context_local_and_restores_real_sdk_net_core() -> None:
    async def run() -> None:
        net_core = NetCore(connector=object())

        async def fake_send(request: object, *_args: object, **_kwargs: object) -> bytes:
            test_request = cast("Any", request)
            await asyncio.sleep(test_request.delay)
            return test_request.body

        net_core.send = fake_send
        original_send = net_core.send
        capture = _GetPostsResponseCapture(net_core)

        def request(body: bytes, delay: float = 0.0, path: str = "/c/f/pb/page") -> object:
            return SimpleNamespace(
                method="POST",
                url=URL.build(
                    scheme="https",
                    host="tiebac.baidu.com",
                    path=path,
                    query_string="cmd=302001",
                ),
                body=body,
                delay=delay,
            )

        async def capture_one(body: bytes, delay: float) -> tuple[bytes | None, int]:
            with capture.capture() as holder:
                await net_core.send(request(body, delay))
                return holder.body, holder.matching_response_count

        first, second = await asyncio.gather(capture_one(b"one", 0.01), capture_one(b"two", 0.0))
        assert first == (b"one", 1)
        assert second == (b"two", 1)

        with capture.capture() as holder:
            await net_core.send(request(b"ignored", path="/c/f/pb/other"))
            assert holder.body is None
            assert holder.matching_response_count == 0

        with capture.capture() as holder:
            await net_core.send(request(b"first"))
            await net_core.send(request(b"second"))
            assert holder.body is None
            assert holder.matching_response_count == 2

        assert net_core.send is original_send

    asyncio.run(run())


def test_research_client_attaches_proof_from_the_existing_posts_http_response(monkeypatch) -> None:
    async def run() -> None:
        body, raw_page = _fixture()
        net_core = NetCore(connector=object())
        send_count = 0

        async def fake_send(_request: object, *_args: object, **_kwargs: object) -> bytes:
            nonlocal send_count
            send_count += 1
            return body

        net_core.send = fake_send
        client = ResearchClient()
        client._http_core = SimpleNamespace(net_core=net_core)
        request = SimpleNamespace(
            method="POST",
            url=URL.build(
                scheme="https",
                host="tiebac.baidu.com",
                path="/c/f/pb/page",
                query_string="cmd=302001",
            ),
        )

        async def fake_get_posts(*_args: object, **_kwargs: object) -> object:
            await net_core.send(request)
            return raw_page

        client.get_posts = fake_get_posts
        monkeypatch.setattr(research_client_module, "convert_aiotieba_posts", lambda _raw: object())

        result = await client.get_posts_research(_TID, _PAGE)

        assert result.zero_comment_evidence[_PID]["count_field_present"] is True
        assert result.zero_comment_evidence[_PID]["reported_count"] == 0
        assert send_count == 1
        assert net_core.send is fake_send

    asyncio.run(run())
