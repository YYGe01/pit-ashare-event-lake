from pitlake.utils import sanitize_for_path, sha256_bytes


def test_sha256_bytes_is_stable() -> None:
    assert sha256_bytes(b"abc") == (
        "sha256:ba7816bf8f01cfea414140de5dae2223"
        "b00361a396177a9cb410ff61f20015ad"
    )


def test_sanitize_for_path() -> None:
    assert sanitize_for_path("cninfo/list page:2026") == "cninfo_list_page_2026"

