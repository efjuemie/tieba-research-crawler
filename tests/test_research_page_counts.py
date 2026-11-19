"""Research API page-count preservation regressions."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from aiotieba.api.get_comments._classdef import Page_c
from aiotieba.api.get_posts._classdef import Page_p
from aiotieba.api.get_threads._classdef import Page_t
from tiebameow.models.dto import CommentsDTO, PageInfoDTO, PostsDTO, ThreadsDTO

from src.research import client as research_client_module
from src.research.client import ResearchClient
from src.scraper.worker import TaskHandler


class StubResearchClient(ResearchClient):
    def __init__(self, raw):
        self.raw = raw
        self.calls = []

    async def get_threads(self, fname_or_fid, pn=1, *, rn=30, sort=None, is_good=False):
        self.calls.append(("get_threads", fname_or_fid, pn, rn, sort, is_good))
        return self.raw

    async def get_posts(
        self,
        tid,
        pn=1,
        *,
        rn=30,
        sort=None,
        only_thread_author=False,
        with_comments=False,
        comment_sort_by_agree=True,
        comment_rn=4,
    ):
        self.calls.append((
            "get_posts",
            tid,
            pn,
            rn,
            sort,
            only_thread_author,
            with_comments,
            comment_sort_by_agree,
            comment_rn,
        ))
        return self.raw

    async def get_comments(self, tid, pid, pn=1, *, is_comment=False):
        self.calls.append(("get_comments", tid, pid, pn, is_comment))
        return self.raw


def _make_dto(dto_type, *, page_size=10, current_page=2, total_page=5, has_more=True, has_prev=True):
    return dto_type.from_incomplete_data({
        "page": {
            "page_size": page_size,
            "current_page": current_page,
            "total_page": total_page,
            "has_more": has_more,
            "has_prev": has_prev,
        },
        "forum": {"fid": 7654321, "fname": "合成贴吧"},
    })


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method_name", "converter_name", "dto_type", "raw_page_type", "reported_count", "args", "kwargs", "source"),
    [
        (
            "get_threads_research",
            "convert_aiotieba_threads",
            ThreadsDTO,
            Page_t,
            23,
            ("恐艾",),
            {"pn": 2, "rn": 10},
            "aiotieba.get_threads",
        ),
        (
            "get_threads_research",
            "convert_aiotieba_threads",
            ThreadsDTO,
            Page_t,
            0,
            ("恐艾",),
            {"pn": 2, "rn": 10},
            "aiotieba.get_threads",
        ),
        (
            "get_comments_research",
            "convert_aiotieba_comments",
            CommentsDTO,
            Page_c,
            23,
            (123456789012345678, 987654321098765432),
            {"pn": 2},
            "aiotieba.get_comments",
        ),
        (
            "get_comments_research",
            "convert_aiotieba_comments",
            CommentsDTO,
            Page_c,
            0,
            (123456789012345678, 987654321098765432),
            {"pn": 2},
            "aiotieba.get_comments",
        ),
    ],
)
async def test_research_pages_copy_count_without_changing_page_metadata(
    monkeypatch,
    method_name,
    converter_name,
    dto_type,
    raw_page_type,
    reported_count,
    args,
    kwargs,
    source,
):
    raw_page = raw_page_type(
        page_size=10,
        current_page=2,
        total_page=5,
        total_count=reported_count,
        has_more=True,
        has_prev=True,
    )
    raw = SimpleNamespace(page=raw_page, objs=[], forum=SimpleNamespace(fid=7654321, fname="合成贴吧"))
    dto = _make_dto(dto_type)
    converter_calls = []
    monkeypatch.setattr(
        research_client_module,
        converter_name,
        lambda value: converter_calls.append(value) or dto,
    )
    client = StubResearchClient(raw)
    observed_before = datetime.now(UTC)

    page = await getattr(client, method_name)(*args, **kwargs)

    observed_after = datetime.now(UTC)
    assert page.raw is raw
    assert page.dto is dto
    assert page.dto.page == PageInfoDTO(
        page_size=10,
        current_page=2,
        total_page=5,
        total_count=reported_count,
        has_more=True,
        has_prev=True,
    )
    assert page.dto.page.total_count is not None
    assert page.source_api == source
    assert observed_before <= page.observed_at <= observed_after
    assert page.observed_at.tzinfo is UTC
    assert raw.page.total_count == reported_count
    assert converter_calls == [raw]
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_posts_research_accepts_aiotieba_page_without_total_count(monkeypatch):
    raw_page = Page_p(page_size=30, current_page=3, total_page=8, has_more=True, has_prev=True)
    assert not hasattr(raw_page, "total_count")
    raw = SimpleNamespace(page=raw_page, objs=[], forum=SimpleNamespace(fid=7654321, fname="合成贴吧"))
    dto = _make_dto(PostsDTO, page_size=30, current_page=3, total_page=8)
    converter_calls = []
    monkeypatch.setattr(
        research_client_module,
        "convert_aiotieba_posts",
        lambda value: converter_calls.append(value) or dto,
    )
    client = StubResearchClient(raw)
    observed_before = datetime.now(UTC)

    page = await client.get_posts_research(123456789012345678, pn=3, rn=30)

    observed_after = datetime.now(UTC)
    assert page.raw is raw
    assert page.dto is dto
    assert page.dto.page == PageInfoDTO(
        page_size=30,
        current_page=3,
        total_page=8,
        total_count=0,
        has_more=True,
        has_prev=True,
    )
    assert page.source_api == "aiotieba.get_posts"
    assert page.zero_comment_evidence == {}
    assert observed_before <= page.observed_at <= observed_after
    assert page.observed_at.tzinfo is UTC
    assert converter_calls == [raw]
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_raw_positive_count_keeps_empty_threads_page_unconfirmed(monkeypatch):
    raw = SimpleNamespace(
        page=Page_t(
            page_size=10,
            current_page=1,
            total_page=1,
            total_count=7,
            has_more=False,
            has_prev=False,
        ),
        objs=[],
        forum=SimpleNamespace(fid=7654321, fname="合成贴吧"),
    )
    dto = ThreadsDTO.from_incomplete_data({
        "objs": [],
        "page": {"current_page": 1, "total_page": 1},
        "forum": {"fid": 7654321, "fname": "合成贴吧"},
    })
    monkeypatch.setattr(research_client_module, "convert_aiotieba_threads", lambda _raw: dto)
    client = StubResearchClient(raw)

    page = await client.get_threads_research("恐艾")

    assert page.dto.objs == []
    assert page.dto.page.total_count == 7
    assert not TaskHandler._is_confirmed_empty(page.dto.page, 1)
