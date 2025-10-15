from typing import Iterable, Iterator, List, TypeVar

T = TypeVar("T")


def chunked(items: List[T], chunk_size: int) -> Iterator[List[T]]:
    """리스트를 고정 크기 청크로 분할하는 제너레이터.

    Args:
        items: 분할할 리스트.
        chunk_size: 각 청크의 크기(양수).

    Yields:
        분할된 리스트 조각(최대 chunk_size).
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be positive")
    for start in range(0, len(items), chunk_size):
        yield items[start : start + chunk_size]


def _render_progress_bar(progress_ratio: float, width: int = 40) -> str:
    """내부: 진행률 바 문자열을 생성합니다.

    Args:
        progress_ratio: 0.0~1.0 사이 진행률.
        width: 바의 전체 너비.
    """
    clamped = 0.0 if progress_ratio < 0 else 1.0 if progress_ratio > 1 else progress_ratio
    filled = int(width * clamped)
    bar = "█" * filled + "-" * (width - filled)
    return f"[{bar}] {clamped*100:6.2f}%"


def print_progress(current: int, total: int, prefix: str | None = None) -> None:
    """현재/전체를 기준으로 콘솔에 단일 라인 프로그레스 바를 출력합니다.

    - 같은 라인을 갱신하며, 완료 시 개행합니다.
    - 대상(total) 기준 100%를 표시합니다.

    Args:
        current: 현재 처리된 건수.
        total: 전체 대상 건수(0이면 0%로 처리).
        prefix: 라인 앞에 붙일 태그(테이블명 등).
    """
    ratio = 0.0 if total <= 0 else min(max(current / total, 0.0), 1.0)
    bar = _render_progress_bar(ratio)
    label = f"{prefix} {bar}" if prefix else bar
    end_char = "\n" if current >= total else "\r"
    print(label, end=end_char, flush=True)


