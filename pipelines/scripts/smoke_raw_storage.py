from pipelines.storage.local_raw_store import write_raw_text
from pipelines.storage.manifest import write_manifest


def main() -> None:
    raw_result = write_raw_text(
        source="smoke",
        dataset="raw_storage",
        filename="sample.txt",
        content="OneHaven raw storage smoke test\n",
        overwrite=True,
    )

    manifest_result = write_manifest(
        source="smoke",
        dataset="raw_storage",
        raw_file_path=raw_result["raw_file_path"],
        status="success",
        file_format="txt",
        record_count=1,
        checksum_sha256=raw_result["checksum_sha256"],
        file_size_bytes=raw_result["file_size_bytes"],
        metadata={
            "purpose": "Verify Story 3.1 raw file and manifest storage pattern"
        },
    )

    print("Raw file written:")
    print(raw_result["raw_file_path"])
    print("Manifest written:")
    print(manifest_result["manifest_path"])


if __name__ == "__main__":
    main()
